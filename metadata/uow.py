from dfs_shared.application import uow
from dfs_shared.infrastructure import json_db
from dfs_shared.infrastructure import pickle_repo
from dfs_shared.domain.repository import RepositoryManager


class PickleRepositoryManager(RepositoryManager):
    repo_class = pickle_repo.PickleRepository

    def __init__(self, repo_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repo_path = repo_path
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
        self.repository = PickleRepositoryManager(repo_path, seen=self.seen)
        self.read_model = json_db.JsonDatabase(read_model_path)

    def __enter__(self):
        self.repository.set_autocommit(False)
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        self.repository.set_autocommit(True)
        return super().__exit__(*args, **kwargs)

    def commit(self):
        self.repository.commit()

    def rollback(self):
        self.repository.rollback()
