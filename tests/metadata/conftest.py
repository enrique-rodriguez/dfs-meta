import pytest
from metadata.bootstrap import bootstrap
from dfs_shared.application.uow import UnitOfWork
from dfs_shared.domain.repository import RepositoryManager
from dfs_shared.infrastructure.inmemory_repo import InMemoryRepository
from metadata.filesystem.infrastructure.json_read_model import JsonReadModel


class InMemoryRepositoryManager(RepositoryManager):
    repo_class = InMemoryRepository


class FakeUnitOfWork(UnitOfWork):
    def __init__(self, read_model_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository = InMemoryRepositoryManager(seen=self.seen)
        self.read_model = JsonReadModel(read_model_path)

    def __enter__(self):
        self.committed = False
        return super().__enter__()

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


@pytest.fixture
def config(tmp_path):
    return {
        "basedir": str(tmp_path),
        "db": {"dbpath": "data.bin", "read_model_path": "data.json"},
    }


@pytest.fixture
def bus(uow, config):
    return bootstrap(config=config, uow=uow)


@pytest.fixture
def uow(config):
    read_model_path = config.get("basedir")+config["db"].get("read_model_path")
    return FakeUnitOfWork(seen=set(), read_model_path=read_model_path)
