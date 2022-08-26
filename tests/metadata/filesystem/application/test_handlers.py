import uuid
import pytest
from metadata.filesystem.domain import model
from metadata.filesystem.domain import commands
from metadata.filesystem.domain import exceptions


def test_create_datanode(bus, uow):
    bus.handle(commands.CreateDataNode("1", "127.0.0.1", 8000))

    datanode = uow.repository.get(model.DataNode, id="1")

    assert uow.committed
    assert datanode.address.port == 8000
    assert datanode.address.host == "127.0.0.1"


def test_raises_error_if_datanode_with_host_and_port_exists(bus):
    bus.handle(commands.CreateDataNode("1", "127.0.0.1", 8000))

    with pytest.raises(exceptions.DuplicateDataNodeError):
        bus.handle(commands.CreateDataNode("1", "127.0.0.1", 8000))


def test_creates_file(bus, uow):
    bus.handle(commands.CreateFile(id="1", name="file.txt", size=50))
    file = uow.repository.get(model.File, id="1")

    assert uow.committed
    assert file.id == "1"
    assert file.size == 50
    assert file.name == "file.txt"


def test_delete_nonexisting_file_raises_file_not_found(bus, uow):
    with pytest.raises(FileNotFoundError):
        bus.handle(commands.DeleteFile(file_id="1"))


def test_deletes_file(bus, uow):
    fid = uuid.uuid4().hex

    history = [
        commands.CreateFile(id=fid, name="file.txt", size=50),
        commands.DeleteFile(file_id=fid),
    ]

    for cmd in history:
        bus.handle(cmd)

    assert uow.committed
    assert uow.repository.get(model.File, id=fid) == None


def test_adds_blocks_to_file_raises_error_if_datanode_not_found(bus, uow):
    fid = uuid.uuid4().hex
    did1 = uuid.uuid4().hex
    did2 = uuid.uuid4().hex

    bus.handle(
        commands.CreateFile(id=fid, name="file.txt", size=50),
    )

    with pytest.raises(exceptions.DataNodeNotFoundError):
        bus.handle(
            commands.AddBlocks(
                file_id=fid,
                blocks=[
                    {"id": "1", "datanode_id": did1},
                    {"id": "2", "datanode_id": did2},
                ],
            )
        )


def test_adds_blocks_to_file(bus, uow):
    fid = uuid.uuid4().hex
    did1 = uuid.uuid4().hex
    did2 = uuid.uuid4().hex

    history = [
        commands.CreateDataNode(datanode_id=did1, host="127.0.0.1", port=8000),
        commands.CreateDataNode(datanode_id=did2, host="127.0.0.1", port=9000),
        commands.CreateFile(id=fid, name="file.txt", size=50),
        commands.AddBlocks(
            file_id=fid,
            blocks=[
                {"id": "1", "datanode_id": did1},
                {"id": "2", "datanode_id": did2},
            ],
        ),
    ]

    for cmd in history:
        bus.handle(cmd)

    file = uow.repository.get(model.File, id=fid)

    assert uow.committed
    assert len(file.blocks) == 2


def test_add_blocks_raises_error_if_file_not_found(bus, uow):
    fid = uuid.uuid4().hex
    did1 = uuid.uuid4().hex
    did2 = uuid.uuid4().hex

    with pytest.raises(FileNotFoundError):
        bus.handle(
            commands.AddBlocks(
                file_id=fid,
                blocks=[
                    {"id": "1", "datanode_id": did1},
                    {"id": "2", "datanode_id": did2},
                ],
            )
        )
