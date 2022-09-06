import json
from app import get_app
from webtest import TestApp


config = {}


def set_config(new_config):
    global config
    config = new_config


def use_client(func):
    def inner(*args, **kwargs):
        return func(client=TestApp(get_app(config)), *args, **kwargs)

    return inner


@use_client
def list_files(client: TestApp):
    res = client.get("/dfs/files")

    assert res.status_code == 200

    return res


@use_client
def create_file(client, name=None, size=None, expect_errors=True):
    data = {}

    if name:
        data["name"] = name

    if size:
        data["size"] = size

    res = client.post("/dfs/files", data, expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 201

    return res


@use_client
def get_file(fid, client: TestApp, expect_errors=False):
    res = client.get(f"/dfs/files/{fid}", expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 200

    return res


@use_client
def delete_file(fid, client: TestApp, expect_errors=False):
    res = client.delete(f"/dfs/files/{fid}", expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 200

    return res


@use_client
def create_datanode(client: TestApp, host=None, port=None, expect_errors=False):
    data = {}

    if host:
        data["host"] = host

    if port:
        data["port"] = port

    res = client.post("/dfs/datanodes", data, expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 201

    return res


@use_client
def list_datanodes(client: TestApp):
    res = client.get(f"/dfs/datanodes")

    assert res.status_code == 200

    return res


@use_client
def get_datanode(did, client: TestApp, expect_errors=False):
    res = client.get(f"/dfs/datanodes/{did}", expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 200

    return res


@use_client
def add_blocks(fid, blocks, client: TestApp, expect_errors=False):
    res = client.post(
        f"/dfs/files/{fid}/blocks",
        {"blocks": json.dumps(blocks)},
        expect_errors=expect_errors,
    )

    if not expect_errors:
        assert res.status_code == 200

    return res


@use_client
def get_blocks(fid, client: TestApp, expect_errors=False):
    res = client.get(f"/dfs/files/{fid}/blocks", expect_errors=expect_errors)

    if not expect_errors:
        assert res.status_code == 200

    return res
