def get_object_or_none(collection, condition):
    try:
        return next(item for item in collection if condition(item))
    except StopIteration:
        return None


def file(fid, uow):
    return get_object_or_none(list_files(uow), lambda f: f.get("id") == fid)


def datanode(did, uow):
    return get_object_or_none(list_datanodes(uow), lambda d: d.get("id") == did)


def list_files(uow):
    return uow.read_model.get("files", list())


def blocks(fid, uow):
    blocks = uow.read_model.get("blocks", list())

    return list(filter(lambda b: b.get("file_id") == fid, blocks))


def list_datanodes(uow):
    return uow.read_model.get("datanodes", list())
