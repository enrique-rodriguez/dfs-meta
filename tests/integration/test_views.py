import uuid
from metadata.filesystem import views
from metadata.filesystem.domain import commands


def test_adds_file_to_read_model(bus):
    fid = uuid.uuid4().hex
    name = "file.txt"
    size = 50

    bus.handle(commands.CreateFile(id=fid, name=name, size=size))

    assert views.list_files(bus.uow) == [{"id": fid, "name": name, "size": size}]


def test_removes_file_and_blocks_from_read_model(bus):
    fid = uuid.uuid4().hex
    did1 = uuid.uuid4().hex

    history = [
        commands.CreateDataNode(datanode_id=did1, host="127.0.0.1", port=8000),
        commands.CreateFile(id=fid, name="file.txt", size=50),
        commands.AddBlocks(
            file_id=fid,
            blocks=[
                {"id": "1", "datanode_id": did1},
            ],
        ),
        commands.DeleteFile(file_id=fid),
    ]

    for cmd in history:
        bus.handle(cmd)

    assert views.list_files(bus.uow) == []
    assert views.blocks(fid, bus.uow) == []


def test_adds_datanode_to_read_model(bus):
    did = uuid.uuid4().hex
    host = "127.0.0.1"
    port = 8000

    bus.handle(commands.CreateDataNode(datanode_id=did, host=host, port=port))
    nodes = views.list_datanodes(bus.uow)
    nodes[0].pop("timestamp")

    assert nodes == [{"id": did, "host": host, "port": port}]


def test_updates_datanode_timestamp_from_read_model_when_node_exists(bus):
    def get_timestamp(did, host, port):
        bus.handle(commands.CreateDataNode(datanode_id=did, host=host, port=port))
        nodes = views.list_datanodes(bus.uow)
        return nodes[0].get("timestamp")

    did = uuid.uuid4().hex
    host = "127.0.0.1"
    port = 8000

    timestamp1 = get_timestamp(did, host, port)
    timestamp2 = get_timestamp(did, host, port)

    assert timestamp1 != timestamp2


def test_adds_blocks_to_read_model(bus):
    fid = uuid.uuid4().hex
    did1 = uuid.uuid4().hex
    did2 = uuid.uuid4().hex
    host = "127.0.0.1"

    history = [
        commands.CreateDataNode(datanode_id=did1, host=host, port=8000),
        commands.CreateDataNode(datanode_id=did2, host=host, port=9000),
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

    assert views.blocks(fid, bus.uow) == [
        {
            "file_id": fid,
            "id": "2",
            "datanode": {"id": did2, "host": host, "port": 9000},
        },
        {
            "file_id": fid,
            "id": "1",
            "datanode": {"id": did1, "host": host, "port": 8000},
        },
    ]
