from dfs_shared.application.uow import UnitOfWork

from metadata.filesystem.domain import model
from metadata.filesystem.domain import events
from metadata.filesystem.domain import commands
from metadata.filesystem.domain import exceptions
from metadata.filesystem.domain import specifications


def create_datanode(cmd: commands.CreateDataNode, uow: UnitOfWork):
    spec = specifications.DataNodeByHostAndPortSpec(cmd.host, cmd.port)

    if uow.repository.get_by_spec(model.DataNode, spec):
        raise exceptions.DuplicateDataNodeError

    with uow:
        datanode = model.DataNode.new(cmd.host, cmd.port, id=cmd.datanode_id)
        uow.repository.save(datanode)
        uow.commit()


def create_file(cmd: commands.CreateFile, uow: UnitOfWork):
    file = model.File.new(id=cmd.id, name=cmd.name, size=cmd.size)

    with uow:
        uow.repository.save(file)
        uow.commit()


def delete_file(cmd: commands.DeleteFile, uow: UnitOfWork):
    if not (file := uow.repository.get(model.File, id=cmd.file_id)):
        raise FileNotFoundError

    with uow:
        uow.repository.delete(file)
        uow.commit()

    uow.add_event(events.FileDeleted(file))


def add_blocks(cmd: commands.AddBlocks, uow: UnitOfWork):
    if not (file := uow.repository.get(model.File, id=cmd.file_id)):
        raise FileNotFoundError
    node_cache = []
    for block in cmd.blocks:
        b_id, n_id = block.get("id"), block.get("datanode_id")
        if n_id not in node_cache:
            node = uow.repository.get(model.DataNode, id=n_id)
            if not node:
                raise exceptions.DataNodeNotFoundError
            node_cache.append(n_id)
        file.add(b_id, n_id)
    with uow:
        uow.repository.update(file)
        uow.commit()


def add_file_to_read_model(event: events.FileCreated, uow: UnitOfWork):
    f = event.file

    with uow:
        uow.read_model.insert(
            "files",
            {
                "id": f.id,
                "name": f.name,
                "size": f.size,
            }
        )

        uow.commit()


def remove_file_from_read_model(event: events.FileDeleted, uow: UnitOfWork):
    file = event.file

    with uow:
        uow.read_model.delete("files", id=file.id)

        uow.commit()


def add_datanode_to_read_model(event: events.DataNodeCreated, uow: UnitOfWork):
    dnode = event.datanode

    with uow:

        uow.read_model.insert(
            "datanodes",
            {
                "id": dnode.id,
                "host": dnode.address.host,
                "port": dnode.address.port,
            },
        )

        uow.commit()


def add_block_to_read_model(event: events.BlockAdded, uow: UnitOfWork):
    blk = event.block
    dnode = uow.repository.get(model.DataNode, id=blk.datanode_id)

    with uow:
        uow.read_model.insert(
            "blocks",
            {
                "file_id": blk.file_id,
                "id": blk.id,
                "datanode": {
                    "id": dnode.id,
                    "host": dnode.address.host,
                    "port": dnode.address.port,
                },
            },
        )

        uow.commit()


def remove_blocks_from_read_model(event: events.FileDeleted, uow: UnitOfWork):
    file = event.file
    with uow:
        uow.read_model.delete("blocks", file_id=file.id)
        uow.commit()
