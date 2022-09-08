class MongoDbReadModel:
    def __init__(self, db):
        self.db = db
    
    def get(self, collection_name, **filters):
        return self.db[collection_name].find_one(filters, {'_id': 0})

    def insert(self, collection_name, data):
        self.db[collection_name].insert_one(data)

    def all(self, collection_name):
        return list(self.db[collection_name].find({}, {'_id': 0}))
    
    def delete(self, collection_name, many=False, **filters):
        collection = self.db[collection_name]
        op = "delete_many" if many else "delete_one"
        getattr(collection, op)(filters)

        