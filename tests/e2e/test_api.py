import os
import time
import uuid
import pytest
from . import api_client as client


def remove_property(dictionary, property):
    dictionary.pop(property, None)
    return dictionary


@pytest.fixture
def api_client(tmp_path):
    client.set_config(
        {
            "basedir": str(tmp_path),
            "db": {"dbpath": "data.bin", "read_model_path": "data.json"},
            "server": {},
        }
    )

    return client


############################################################################################
# File
############################################################################################


def test_no_files_gives_empty_list(api_client):
    res = api_client.list_files()

    assert res.status_code == 200
    assert res.json == []


def test_list_files(api_client):
    res = api_client.create_file(name="file.txt", size=50)

    fid = res.json.get("id")

    list_res = api_client.list_files()

    assert list_res.status_code == 200
    assert list_res.json == [{"id": fid, "name": "file.txt", "size": "50"}]


def test_gives_404_if_file_not_found(api_client):
    fid = uuid.uuid4().hex

    res = api_client.get_file(fid, expect_errors=True)

    assert res.status_code == 404


def test_create_file(api_client):
    res = api_client.create_file(name="file.txt", size=50)

    fid = res.json.get("id")

    res = api_client.get_file(fid)

    assert res.status_code == 200
    assert res.json.get("name") == "file.txt"
    assert res.json.get("size") == "50"


def test_create_file_without_data_gives_400(api_client):
    res = api_client.create_file(name=None, size=None, expect_errors=True)

    assert res.status_code == 400


def test_delete_file(api_client):
    create_res = api_client.create_file(name="file.txt", size=50)

    fid = create_res.json.get("id")

    delete_res = api_client.delete_file(fid)

    get_file_res = api_client.get_file(fid, expect_errors=True)
    get_blocks_res = api_client.get_blocks(fid)

    assert delete_res.status_code == 200
    assert get_file_res.status_code == 404
    assert get_blocks_res.json == []

def test_delete_file_not_found_gives_404(api_client):
    delete_res = api_client.delete_file(uuid.uuid4().hex, expect_errors=True)

    assert delete_res.status_code == 404



##########################################################################################
# DataNode
##########################################################################################


def test_no_datanodes_gives_empty_list(api_client):
    res = api_client.list_datanodes()

    assert res.status_code == 200
    assert res.json == []


def test_list_datanodes(api_client):
    res = api_client.create_datanode(host="127.0.0.1", port=8000)
    did = res.json.get("id")
    list_response = api_client.list_datanodes()

    # Remove timestamp property to avoid assertion failures
    json = list(map(lambda x: remove_property(x, "timestamp"), list_response.json))

    assert list_response.status_code == 200
    assert json == [{"id": did, "host": "127.0.0.1", "port": "8000"}]

def test_gives_404_if_datanode_not_found(api_client):
    did = uuid.uuid4().hex

    res = api_client.get_datanode(did, expect_errors=True)

    assert res.status_code == 404


def test_create_datanode(api_client):
    res = api_client.create_datanode(host="127.0.0.1", port=8000)
    did = res.json.get("id")
    res = api_client.get_datanode(did)

    assert res.status_code == 200
    assert res.json.get("host") == "127.0.0.1"
    assert res.json.get("port") == "8000"


def test_create_datanode_gives_400_if_address_and_port_not_given(api_client):
    res = api_client.create_datanode(expect_errors=True)

    assert res.status_code == 400


##########################################################################################
# Blocks
##########################################################################################


def test_add_blocks_to_nonexisting_file_gives_404(api_client):
    fid = uuid.uuid4().hex
    blocks = []
    res = api_client.add_blocks(fid, blocks, expect_errors=True)

    assert res.status_code == 404


def test_add_blocks_with_nonexisting_datanode_gives_404(api_client):
    create_res = api_client.create_file(name="file.txt", size=50)
    fid = create_res.json.get("id")
    did = uuid.uuid4().hex

    blocks = [{"id": uuid.uuid4().hex, "datanode_id": did}]
    res = api_client.add_blocks(fid, blocks, expect_errors=True)

    assert res.status_code == 404


def test_adds_blocks_to_file_successfully(api_client):
    create_node_res = api_client.create_datanode(host="127.0.0.1", port=8000)
    did = create_node_res.json.get("id")
    create_file_res = api_client.create_file(name="file.txt", size=50)
    fid = create_file_res.json.get("id")
    bid = uuid.uuid4().hex

    blocks = [{"id": bid, "datanode_id": did}]
    add_blocks_res = api_client.add_blocks(fid, blocks)
    get_blocks_res = api_client.get_blocks(fid)

    assert add_blocks_res.status_code == 200
    assert get_blocks_res.status_code == 200
    assert get_blocks_res.json == [
        {
            "file_id": fid,
            "id": bid,
            "datanode": {
                "id": did,
                "host": "127.0.0.1",
                "port": "8000",
            },
        }
    ]
