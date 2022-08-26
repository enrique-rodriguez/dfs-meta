import pytest
from metadata.bootstrap import bootstrap


@pytest.fixture
def config(tmp_path):
    return {
        "basedir": str(tmp_path),
        "db": {"dbpath": "data.bin", "read_model_path": "data.json"},
    }


@pytest.fixture
def bus(config):
    return bootstrap(config)
