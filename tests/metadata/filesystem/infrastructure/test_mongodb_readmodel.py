import pytest
import pymongo
from metadata.filesystem.infrastructure.mongodb_readmodel import MongoDbReadModel


@pytest.fixture
def readmodel():
    client = pymongo.MongoClient()
    database = client["test_readmodel"]

    yield MongoDbReadModel(database)

    client.drop_database(database)
    client.close()


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


def test_get_gives_none_if_not_found(readmodel):
    assert readmodel.get("mycollection", name="john") == None


def test_get(readmodel):
    readmodel.insert("mycollection", {"name": "john"})
    readmodel.insert("mycollection", {"name": "bob"})

    assert readmodel.get("mycollection", name="john") == {"name": "john"}


def test_delete(readmodel):
    collection_name = "mycollection"

    to_insert = [{"name": "john"}, {"name": "bob"}]

    for d in to_insert:
        readmodel.insert(collection_name, d)

    readmodel.delete(collection_name, name="john")

    assert readmodel.all(collection_name) == [{"name": "bob"}]


def test_delete_many(readmodel):
    collection_name = "mycollection"

    to_insert = [{"name": "john"}, {"name": "john"}]

    for d in to_insert:
        readmodel.insert(collection_name, d)

    readmodel.delete(collection_name, many=True, name="john")

    assert readmodel.all(collection_name) == []
