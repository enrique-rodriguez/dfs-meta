import os
import json


class JsonReadModel:
    def __init__(self, path, auto_commit=False):
        self.path = path
        self.staging = list()
        self.auto_commit = auto_commit
        self.load_objects()

    def load_objects(self):
        self.objects = self.get_objects(self.path)

    def get_objects(self, path):
        if not os.path.exists(path):
            return dict()
        with open(path, "r") as f:
            return json.load(f)

    def insert(self, collection_name, data):
        collection = self.get(collection_name, list())
        collection.append(data)
        self.set(collection_name, collection)

    def delete(self, collection_name, **filters):
        collection = self.get(collection_name, list())
        new_collection = list()

        for item in collection:
            property_found = False
            for key, value in filters.items():
                if item.get(key) == value:
                    property_found = True
                    break
            if not property_found:
                new_collection.append(item)

        self.set(collection_name, new_collection)

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
        self.load_objects()
        return self.objects.get(key, default)

    def rollback(self):
        self.staging.clear()
