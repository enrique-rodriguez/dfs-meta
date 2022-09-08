import pytest
from metadata.filesystem.infrastructure.mongodb_readmodel import MongoDbReadModel

try:
    import pymongo

    PYMONGO_INSTALLED = True
except:
    PYMONGO_INSTALLED = False


@pytest.fixture
def readmodel():
    client = pymongo.MongoClient()
    database = client["test_readmodel"]

    yield MongoDbReadModel(database)

    client.drop_database(database)
    client.close()


@pytest.mark.skipif(not PYMONGO_INSTALLED, reason="pymongo not installed")
def test_insert(readmodel):
    readmodel.insert(
        "mycollection",
        {
            "field1": "value1",
            "field2": "value2",
        },
    )

    assert readmodel.all("mycollection") == [
        {
            "field1": "value1",
            "field2": "value2",
        }
    ]


@pytest.mark.skipif(not PYMONGO_INSTALLED, reason="pymongo not installed")
def test_get_gives_none_if_not_found(readmodel):
    assert readmodel.get("mycollection", name="john") == None


@pytest.mark.skipif(not PYMONGO_INSTALLED, reason="pymongo not installed")
def test_get(readmodel):
    readmodel.insert("mycollection", {"name": "john"})
    readmodel.insert("mycollection", {"name": "bob"})

    assert readmodel.get("mycollection", name="john") == {"name": "john"}


@pytest.mark.skipif(not PYMONGO_INSTALLED, reason="pymongo not installed")
def test_delete(readmodel):
    collection_name = "mycollection"

    to_insert = [{"name": "john"}, {"name": "bob"}]

    for d in to_insert:
        readmodel.insert(collection_name, d)

    readmodel.delete(collection_name, name="john")

    assert readmodel.all(collection_name) == [{"name": "bob"}]


@pytest.mark.skipif(not PYMONGO_INSTALLED, reason="pymongo not installed")
def test_delete_many(readmodel):
    collection_name = "mycollection"

    to_insert = [{"name": "john"}, {"name": "john"}]

    for d in to_insert:
        readmodel.insert(collection_name, d)

    readmodel.delete(collection_name, many=True, name="john")

    assert readmodel.all(collection_name) == []
