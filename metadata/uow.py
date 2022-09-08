import pymongo
from dfs_shared.application import uow
from dfs_shared.domain.repository import RepositoryManager
from metadata.filesystem.infrastructure.pickle_repo import PickleRepository
from metadata.filesystem.infrastructure.json_db import JsonDatabase


class PickleRepositoryManager(RepositoryManager):
    repo_class = PickleRepository

    def __init__(self, repo_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository = self.repo_class(repo_path, seen=self.seen)
    
    def get_repo(self, entity):
        return self.repository
    
    def set_autocommit(self, value):
        self.repository.set_autocommit(value)
    
    def rollback(self):
        self.repository.rollback()
    
    def commit(self):
        self.repository.commit()


class PickleUnitOfWork(uow.UnitOfWork):
    def __init__(self, repo_path, read_model_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbname = kwargs.get("dbname", "metadata_test")
        self.repository = PickleRepositoryManager(repo_path, seen=self.seen)

    def __enter__(self):
        self.client = pymongo.MongoClient()
        self.database = self.client[self.dbname]
        self.repository.set_autocommit(False)
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        self.client.close()
        self.repository.set_autocommit(True)
        return super().__exit__(*args, **kwargs)

    def commit(self):
        self.repository.commit()

    def rollback(self):
        self.repository.rollback()
