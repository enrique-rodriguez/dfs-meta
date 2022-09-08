import pytest

try:
    import pymongo
    PYMONGO_INSTALLED = True
except:
    PYMONGO_INSTALLED = False


@pytest.fixture(autouse=True)
def setup_mongodb_database():
    if not PYMONGO_INSTALLED:
        yield False
    else:
        client = pymongo.MongoClient()
        db = client["metadata_test"]
        yield db
        client.drop_database(db)
        client.close()