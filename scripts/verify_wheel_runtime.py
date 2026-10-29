"""Check an installed release wheel's native binary, SQL, Parquet and Delta."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
from pathlib import Path
import platform
import re
import struct
import subprocess
import sys
import tempfile
import zipfile


def _check_wheel(
    wheel: Path, expected_tag: str, expected_machine: str
) -> tuple[str, str]:
    if not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError("wheel path does not name a local wheel: {}".format(wheel))
    if "cp311-abi3" not in wheel.name:
        raise ValueError(
            "release wheel must use the cp311-abi3 tag: {}".format(wheel.name)
        )
    if expected_tag not in wheel.name:
        raise ValueError(
            "wheel {} does not contain its expected platform tag {}".format(
                wheel.name, expected_tag
            )
        )

    machine = platform.machine().lower().replace("-", "_")
    actual_machine = {"amd64": "x86_64", "x64": "x86_64", "arm64": "aarch64"}.get(
        machine, machine
    )
    if actual_machine != expected_machine:
        raise ValueError(
            "runtime machine is {}, expected native {}".format(
                actual_machine, expected_machine
            )
        )

    with zipfile.ZipFile(wheel) as archive:
        wheel_metadata = next(
            (name for name in archive.namelist() if name.endswith(".dist-info/WHEEL")),
            None,
        )
        if wheel_metadata is None:
            raise ValueError("wheel is missing dist-info/WHEEL metadata")
        metadata = archive.read(wheel_metadata).decode("utf-8")
        wheel_tags = set(re.findall(r"^Tag: cp311-abi3-(\S+)$", metadata, re.MULTILINE))
        if not set(expected_tag.split(".")).issubset(wheel_tags):
            raise ValueError(
                "wheel metadata does not declare the expected abi3 platform tag"
            )

    with wheel.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    return digest, actual_machine


def _value(row: object, key: str, index: int) -> object:
    try:
        return getattr(row, key)
    except (AttributeError, TypeError):
        return row[index]  # type: ignore[index]


def _missing_shared_libraries(output: str) -> list[str]:
    markers = (
        "=> not found",
        "error loading shared library",
        "cannot open shared object file",
    )
    return [
        line.strip()
        for line in output.splitlines()
        if any(marker in line.lower() for marker in markers)
    ]


def _verify_native_binary(wheel: Path, root: Path, expected_machine: str) -> None:
    with zipfile.ZipFile(wheel) as archive:
        native_name = next(
            (
                name
                for name in archive.namelist()
                if name.startswith("sparkless/_native")
                and (name.endswith(".so") or name.endswith(".pyd"))
            ),
            None,
        )
        if native_name is None:
            raise ValueError("wheel is missing its native extension")
        native_path = Path(archive.extract(native_name, root / "native"))

    contents = native_path.read_bytes()
    if sys.platform == "linux":
        if contents[:4] != b"\x7fELF":
            raise ValueError("Linux wheel extension is not an ELF binary")
        byte_order = "<" if contents[5] == 1 else ">"
        machine_code = struct.unpack(byte_order + "H", contents[18:20])[0]
        expected_code = 62 if expected_machine == "x86_64" else 183
        if machine_code != expected_code:
            raise ValueError(
                "ELF extension architecture does not match its native runner"
            )
        result = subprocess.run(
            ["ldd", str(native_path)], text=True, capture_output=True, check=False
        )
        output = result.stdout + result.stderr
        missing_libraries = _missing_shared_libraries(output)
        if missing_libraries:
            raise ValueError(
                "wheel extension has unresolved runtime dependencies: {}".format(
                    "\n".join(missing_libraries)
                )
            )
    elif sys.platform == "darwin":
        if contents[:4] not in {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf"}:
            raise ValueError("macOS wheel extension is not a 64-bit Mach-O binary")
        byte_order = "<" if contents[:4] == b"\xcf\xfa\xed\xfe" else ">"
        cpu_type = struct.unpack(byte_order + "I", contents[4:8])[0]
        expected_code = 0x01000007 if expected_machine == "x86_64" else 0x0100000C
        if cpu_type != expected_code:
            raise ValueError(
                "Mach-O extension architecture does not match its native runner"
            )
        commands = subprocess.run(
            ["otool", "-l", str(native_path)],
            text=True,
            capture_output=True,
            check=True,
        ).stdout
        match = re.search(r"\bminos\s+(\d+)\.(\d+)", commands)
        if match is None:
            match = re.search(
                r"cmd LC_VERSION_MIN_MACOSX[\s\S]*?\bversion\s+(\d+)\.(\d+)",
                commands,
            )
        if not match:
            raise ValueError(
                "could not read the macOS deployment target from Mach-O metadata"
            )
        actual_target = (int(match.group(1)), int(match.group(2)))
        expected_target = (10, 12) if expected_machine == "x86_64" else (11, 0)
        if actual_target > expected_target:
            raise ValueError(
                "Mach-O deployment target exceeds the wheel's declared platform tag"
            )
    elif sys.platform == "win32":
        if contents[:2] != b"MZ" or len(contents) < 0x40:
            raise ValueError("Windows wheel extension is not a PE binary")
        pe_offset = struct.unpack("<I", contents[0x3C:0x40])[0]
        if contents[pe_offset : pe_offset + 4] != b"PE\0\0":
            raise ValueError("Windows extension has an invalid PE signature")
        machine_code = struct.unpack("<H", contents[pe_offset + 4 : pe_offset + 6])[0]
        expected_code = 0x8664 if expected_machine == "x86_64" else 0xAA64
        if machine_code != expected_code:
            raise ValueError(
                "PE extension architecture does not match its native runner"
            )
    else:
        raise ValueError("unsupported runtime platform: {}".format(sys.platform))


def _exercise_runtime(root: Path) -> None:
    from sparkless.sql import SparkSession

    session = SparkSession.builder.app_name(
        "sparkless-wheel-runtime-witness"
    ).get_or_create()
    try:
        left = session.createDataFrame([(1,), (2,)], ["id"])
        right = session.createDataFrame([(1, 777), (2, 888)], ["id", "id_right"])
        left.createOrReplaceTempView("wheel_left")
        right.createOrReplaceTempView("wheel_right")
        joined = session.sql(
            "SELECT rhs.id FROM wheel_left AS lhs "
            "LEFT JOIN wheel_right AS rhs ON lhs.id = rhs.id WHERE rhs.id = 1"
        ).collect()
        if len(joined) != 1 or _value(joined[0], "id", 0) != 1:
            raise ValueError(
                "qualified SQL projection, join, or filter returned the wrong row"
            )

        nested = session.createDataFrame(
            [(1, {"name": "alpha"}, None), (2, None, "present")],
            "id bigint, payload struct<name:string>, optional string",
        )
        nested.createOrReplaceTempView("wheel_nested")
        projected = session.sql(
            "SELECT id, payload.name, optional FROM wheel_nested WHERE id >= 1 ORDER BY id"
        ).collect()
        if len(projected) != 2:
            raise ValueError("nested/nullable SQL projection did not return two rows")
        if (
            _value(projected[0], "id", 0) != 1
            or _value(projected[0], "optional", 2) is not None
        ):
            raise ValueError("nested/nullable SQL projection changed the first row")
        if (
            _value(projected[1], "id", 0) != 2
            or _value(projected[1], "optional", 2) != "present"
        ):
            raise ValueError("nested/nullable SQL projection changed the second row")

        parquet_path = root / "parquet-roundtrip"
        nested.write.mode("overwrite").parquet(str(parquet_path))
        parquet_rows = session.read.parquet(str(parquet_path)).orderBy("id").collect()
        if len(parquet_rows) != 2 or _value(parquet_rows[0], "optional", 2) is not None:
            raise ValueError("local Parquet round trip did not preserve nullable rows")

        delta_path = root / "delta-roundtrip"
        nested.write.format("delta").mode("overwrite").save(str(delta_path))
        delta_rows = (
            session.read.format("delta").load(str(delta_path)).orderBy("id").collect()
        )
        if len(delta_rows) != 2 or _value(delta_rows[0], "optional", 2) is not None:
            raise ValueError("local Delta round trip did not preserve nullable rows")
    finally:
        session.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--platform-tag", required=True)
    parser.add_argument("--machine", choices=("x86_64", "aarch64"), required=True)
    args = parser.parse_args()

    try:
        digest, _actual_machine = _check_wheel(
            args.wheel, args.platform_tag, args.machine
        )
        if importlib.metadata.version("sparkless") != args.wheel.name.split("-")[1]:
            raise ValueError("installed Sparkless version does not match the wheel")

        with tempfile.TemporaryDirectory(
            prefix="sparkless-wheel-witness-"
        ) as temporary:
            root = Path(temporary)
            _verify_native_binary(args.wheel, root, args.machine)
            _exercise_runtime(root)

        print(f"Wheel runtime checks passed: {args.wheel.name} (sha256: {digest})")
    except (
        OSError,
        ValueError,
        zipfile.BadZipFile,
        ImportError,
        ModuleNotFoundError,
        subprocess.SubprocessError,
    ) as error:
        print("Wheel runtime witness failed: {}".format(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
