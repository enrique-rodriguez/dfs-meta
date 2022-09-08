import pytest
import pymongo


@pytest.fixture(autouse=True)
def setup_mongodb_database():
    client = pymongo.MongoClient()
    db = client["metadata_test"]
    yield db
    client.drop_database(db)
    client.close()