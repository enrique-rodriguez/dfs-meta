import os
import pickle
from copy import deepcopy
from collections import defaultdict
from dfs_shared.domain.repository import Repository


class PickleRepository(Repository):
    def __init__(self, path, auto_commit=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.staging = set()
        self.auto_commit = auto_commit
        self.objects = self.get_objects(path)

    def save(self, obj):
        """Persists the object by adding it to the staging area.
        
        If autocommit is set True, the changes will be commited immediately.

        If autocommit is set False, a manual call to 'commit' is needed to persist the data.

        Parameters
        ----------
        obj : Object
            The object to save
        """

        s = super()
        self.staging.add(lambda: s.save(obj))
        self.do_commit()

    def update(self, obj):
        """Updates the object by adding it to the staging area.
        
        If autocommit is set True, the changes will be commited immediately.

        If autocommit is set False, a manual call to 'commit' is needed to update the data.

        Parameters
        ----------
        obj : Object
            The object to update
        """

        s = super()
        self.staging.add(lambda: s.update(obj))
        self.do_commit()

    def delete(self, id):
        """Deletes the object by adding it to the staging area.
        
        If autocommit is set True, the object will be deleted immediately.

        If autocommit is set False, a manual call to 'commit' is needed to delete the object.

        Parameters
        ----------
        id : str
            The id of the object to delete
        """
        
        s = super()
        self.staging.add(lambda: s.delete(id))
        self.do_commit()
    
    def set_autocommit(self, value):
        """Set the state of autocommiting.
        
        Parameters
        ----------
        value : bool
            The new state for autocommitting
        """

        self.auto_commit = value

    def _save(self, obj):
        _type = type(obj)
        obj_cp = deepcopy(obj)
        obj_cp.events.clear()
        self.objects[_type].add(obj_cp)
        
    def _update(self, obj):
        obj_saved = self.get(obj.id)
        for field, value in vars(obj).items():
            setattr(obj_saved, field, value)

    def _delete(self, obj):
        _type = type(obj)
        self.objects[_type] = set(filter(lambda o: o != obj, self.objects[_type]))

    def _get(self, id):
        for _, obj_set in self.objects.items():
            for obj in obj_set:
                if obj.id == id:
                    return obj
        return None
    
    def get_by_spec(self, spec):
        for obj in self.objects[spec.type]:
            if spec.criteria(obj):
                return obj
        return None

    def get_objects(self, path):
        if not os.path.exists(path):
            return defaultdict(set)
        with open(path, "rb") as f:
            return pickle.load(f)

    def do_commit(self):
        if self.auto_commit:
            self.commit()

    def commit(self):
        while len(self.staging) > 0:
            operation = self.staging.pop()
            operation()

        with open(self.path, "wb") as f:
            pickle.dump(self.objects, f)

    def rollback(self):
        self.clear_staging()

    def clear_staging(self):
        self.staging.clear()
