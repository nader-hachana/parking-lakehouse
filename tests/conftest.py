import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# The generator scripts are plain files in generator/, not an installed package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generator"))


def java_works() -> bool:
    if shutil.which("java") is None:
        return False
    return subprocess.run(["java", "-version"], check=False, capture_output=True).returncode == 0


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session, for the tests that run the pipeline's SQL and DataFrame logic."""
    if not java_works():
        pytest.skip("Java is not available, so the Spark tests are skipped")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
