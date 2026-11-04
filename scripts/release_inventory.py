"""The single candidate distribution inventory used by release validation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DistributionSpec:
    project: str
    directory: str
    version: str
    wheels: int
    sdists: int
    platforms: frozenset[str]
    required_dependencies: frozenset[str] = frozenset()


NATIVE_PLATFORMS = frozenset(
    {
        "macosx_10_12_x86_64",
        "macosx_11_0_arm64",
        "manylinux2014_x86_64.manylinux_2_17_x86_64",
        "manylinux_2_28_aarch64",
        "musllinux_1_2_aarch64",
        "musllinux_1_2_x86_64",
        "win_amd64",
        "win_arm64",
    }
)
SERVER_PLATFORMS = frozenset({"manylinux_2_28_x86_64", "macosx_14_0_arm64"})


def distribution_specs(
    api_version: str, engine_version: str
) -> tuple[DistributionSpec, ...]:
    """Return the complete, ordered Python release inventory."""
    return (
        DistributionSpec(
            project="sparkless",
            directory="sparkless",
            version=api_version,
            wheels=1,
            sdists=1,
            platforms=frozenset({"any"}),
            required_dependencies=frozenset({f"sparkless-native=={engine_version}"}),
        ),
        DistributionSpec(
            project="sparkless-native",
            directory="native",
            version=engine_version,
            wheels=8,
            sdists=1,
            platforms=NATIVE_PLATFORMS,
        ),
        DistributionSpec(
            project="sparkless-server",
            directory="server-base",
            version=engine_version,
            wheels=2,
            sdists=0,
            platforms=SERVER_PLATFORMS,
            required_dependencies=frozenset({f"sparkless-connect=={engine_version}"}),
        ),
        DistributionSpec(
            project="sparkless-server-duckdb",
            directory="server-duckdb",
            version=engine_version,
            wheels=2,
            sdists=0,
            platforms=SERVER_PLATFORMS,
            required_dependencies=frozenset({f"sparkless-connect=={engine_version}"}),
        ),
        DistributionSpec(
            project="sparkless-connect",
            directory="connect",
            version=engine_version,
            wheels=1,
            sdists=1,
            platforms=frozenset({"any"}),
        ),
        DistributionSpec(
            project="sparkless-testing",
            directory="testing",
            version=engine_version,
            wheels=1,
            sdists=1,
            platforms=frozenset({"any"}),
        ),
    )
