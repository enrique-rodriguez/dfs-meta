from dfs_shared.application import uow
from dfs_shared.domain.repository import RepositoryManager
from metadata.filesystem.infrastructure.pickle_repo import PickleRepository
from metadata.filesystem.infrastructure.json_read_model import JsonReadModel


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
        self.read_model = JsonReadModel(read_model_path)
        self.repository = PickleRepositoryManager(repo_path, seen=self.seen)

    def __enter__(self):
        self.repository.set_autocommit(False)
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        self.repository.set_autocommit(True)
        return super().__exit__(*args, **kwargs)

    def commit(self):
        self.repository.commit()
        self.read_model.commit()

    def rollback(self):
        self.repository.rollback()
        self.read_model.rollback()
