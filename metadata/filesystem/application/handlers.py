import time
from dfs_shared.application.uow import UnitOfWork

from metadata.filesystem.domain import model
from metadata.filesystem.domain import events
from metadata.filesystem.domain import commands
from metadata.filesystem.domain import exceptions
from metadata.filesystem.domain import specifications
from metadata.filesystem.application.publisher import EventPublisher


def create_datanode(cmd: commands.CreateDataNode, uow: UnitOfWork, **deps):
    spec = specifications.DataNodeByHostAndPortSpec(cmd.host, cmd.port)

    datanode = uow.repository.get_by_spec(model.DataNode, spec)

    if datanode:
        datanode.set_timestamp(time.time())
    else:
        datanode = model.DataNode.new(cmd.host, cmd.port, id=cmd.datanode_id)

    with uow:
        uow.repository.save(datanode)
        uow.commit()


def create_file(cmd: commands.CreateFile, uow: UnitOfWork, **deps):
    file = model.File.new(id=cmd.id, name=cmd.name, size=cmd.size)

    with uow:
        uow.repository.save(file)
        uow.commit()


def delete_file(cmd: commands.DeleteFile, uow: UnitOfWork, **deps):
    if not (file := uow.repository.get(model.File, id=cmd.file_id)):
        raise FileNotFoundError

    with uow:
        uow.repository.delete(file)
        uow.commit()

    uow.add_event(events.FileDeleted(file))


def add_blocks(cmd: commands.AddBlocks, uow: UnitOfWork, **deps):
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


def update_datanode_timestamp_from_read_model(
    event: events.DataNodeUpdated, uow: UnitOfWork, **deps
):
    datanodes = uow.read_model.get("datanodes", list())

    datanode = next(d for d in datanodes if d.get("id") == event.datanode.id)
    datanode["timestamp"] = event.datanode.timestamp

    uow.read_model.commit()


def add_file_to_read_model(event: events.FileCreated, uow: UnitOfWork, **deps):
    files = uow.read_model.get("files", list())
    f = event.file

    files.append(
        {
            "id": f.id,
            "name": f.name,
            "size": f.size,
        }
    )

    uow.read_model.set("files", files)
    uow.read_model.commit()


def remove_file_from_read_model(event: events.FileDeleted, uow: UnitOfWork, **deps):
    file = event.file
    files = uow.read_model.get("files", list())

    files = list(filter(lambda f: f.get("id") != file.id, files))

    uow.read_model.set("files", files)
    uow.read_model.commit()


def add_datanode_to_read_model(event: events.DataNodeCreated, uow: UnitOfWork, **deps):
    datanodes = uow.read_model.get("datanodes", list())
    dnode = event.datanode

    datanodes.append(
        {
            "id": dnode.id,
            "host": dnode.address.host,
            "port": dnode.address.port,
            "timestamp": dnode.timestamp,
        }
    )

    uow.read_model.set("datanodes", datanodes)
    uow.read_model.commit()


def add_block_to_read_model(event: events.BlockAdded, uow: UnitOfWork, **deps):
    blk = event.block
    blocks = uow.read_model.get("blocks", list())
    dnode = uow.repository.get(model.DataNode, id=blk.datanode_id)

    blocks.append(
        {
            "file_id": blk.file_id,
            "id": blk.id,
            "datanode": {
                "id": dnode.id,
                "host": dnode.address.host,
                "port": dnode.address.port,
            },
        }
    )

    uow.read_model.set("blocks", blocks)
    uow.read_model.commit()


def remove_blocks_from_read_model(event: events.FileDeleted, uow: UnitOfWork, **deps):
    file = event.file
    blocks = uow.read_model.get("blocks", list())
    blocks = list(filter(lambda b: b.get("file_id") != file.id, blocks))

    uow.read_model.set("blocks", blocks)
    uow.read_model.commit()


def publish_file_deleted_event(
    event: events.FileDeleted, publisher: EventPublisher, **deps
):

    file = event.file

    publisher.publish(
        "metadata",
        "file_deleted",
        {
            "id": file.id,
            "name": file.name,
            "size": file.size,
        },
    )
