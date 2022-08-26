import os
import pytest
from metadata.bootstrap import bootstrap
from dfs_shared.application.uow import UnitOfWork
from dfs_shared.domain.repository import Repository
from dfs_shared.domain.repository import RepositoryManager
from dfs_shared.infrastructure.json_db import JsonDatabase


class InMemoryRepository(Repository):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection = set()

    def _get(self, id):
        try:
            return next(o for o in self.collection if o.id == id)
        except StopIteration:
            return None

    def get_by_spec(self, spec):
        for obj in self.collection:
            if spec.criteria(obj):
                return obj
        return None

    def _save(self, obj):
        self.collection.add(obj)

    def _delete(self, obj):
        self.collection = set(filter(lambda o: o != obj, self.collection))

    def _update(self, obj):
        pass

    def rollback(self, obj):
        pass


class InMemoryRepositoryManager(RepositoryManager):
    repo_class = InMemoryRepository


class FakeUnitOfWork(UnitOfWork):
    def __init__(self, read_model_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository = InMemoryRepositoryManager(seen=self.seen)
        self.read_model = JsonDatabase(read_model_path)

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
