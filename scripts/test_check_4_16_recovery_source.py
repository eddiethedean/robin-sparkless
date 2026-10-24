import unittest

from check_4_16_recovery_source import (
    REQUIRED_JOBS,
    SERVER_ARTIFACTS,
    SERVER_BUILD_JOBS,
    SOURCE_RUN_ID,
    SOURCE_SHA,
    validate_source_run,
)


def fixture():
    run = {
        "id": SOURCE_RUN_ID,
        "head_sha": SOURCE_SHA,
        "head_branch": "v4.16.0",
        "event": "push",
    }
    names = list(REQUIRED_JOBS + SERVER_BUILD_JOBS)
    names.extend("Python tests - platform {}".format(index) for index in range(15))
    jobs = {"jobs": [{"name": name, "conclusion": "success"} for name in names]}
    artifacts = {
        "artifacts": [
            {"name": name, "expired": False} for name in SERVER_ARTIFACTS
        ]
    }
    return run, jobs, artifacts


class Check416RecoverySourceTest(unittest.TestCase):
    def test_accepts_only_the_validated_release_source(self):
        validate_source_run(*fixture())

    def test_rejects_a_different_source_commit(self):
        run, jobs, artifacts = fixture()
        run["head_sha"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "source run commit"):
            validate_source_run(run, jobs, artifacts)

    def test_rejects_expired_server_wheel_artifact(self):
        run, jobs, artifacts = fixture()
        artifacts["artifacts"][0]["expired"] = True
        with self.assertRaisesRegex(ValueError, "artifact is unavailable"):
            validate_source_run(run, jobs, artifacts)


if __name__ == "__main__":
    unittest.main()
