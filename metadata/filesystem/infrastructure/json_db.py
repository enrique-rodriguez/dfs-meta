import os
import json


class JsonDatabase:
    def __init__(self, path, auto_commit=False):
        self.path = path
        self.staging = list()
        self.auto_commit = auto_commit
        self.objects = self.get_objects(path)

    def get_objects(self, path):
        if not os.path.exists(path):
            return dict()
        with open(path, "r") as f:
            return json.load(f)

    def commit(self):
        while len(self.staging) > 0:
            op = self.staging.pop()
            op()
        with open(self.path, "w") as f:
            json.dump(self.objects, f)

    def set(self, key, value):
        def op():
            self.objects[key] = value

        self.staging.append(op)
        if self.auto_commit:
            self.commit()

    def get(self, key, default=None):
        return self.objects.get(key, default)

    def rollback(self):
        self.staging.clear()
