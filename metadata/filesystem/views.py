def file(fid, uow):
    with uow:
        collection = uow.database.files
        return collection.find_one({"id": fid}, {"_id": 0})


def datanode(did, uow):
    with uow:
        collection = uow.database.datanodes
        return collection.find_one({"id": did}, {"_id": 0})


def list_files(uow):
    with uow:
        collection = uow.database.files
        return list(collection.find({}, {"_id": 0}))


def blocks(fid, uow):
    with uow:
        collection = uow.database.blocks
        return list(collection.find({"file_id": fid}, {"_id": 0}))


def list_datanodes(uow):
    with uow:
        collection = uow.database.datanodes
        return list(collection.find({}, {"_id": 0}))
