import os
import pytest
from metadata.filesystem.infrastructure.json_db import JsonDatabase


@pytest.fixture
def get_repo(tmp_path):
    path = os.path.join(tmp_path, "db.json")

    def factory(auto_commit=False):
        return JsonDatabase(path, auto_commit)

    return factory


def test_persistence(get_repo):
    db = get_repo()

    db.set("entity", [{"id": "1"}])
    db.commit()

    db = get_repo()

    assert db.get("entity") == [{"id": "1"}]


def test_does_not_persist_if_commit_not_called(get_repo):
    db = get_repo()

    db.set("entity", [{"id": "1"}])

    db = get_repo()

    assert db.get("entity") == None


def test_rollback(get_repo):
    db = get_repo()

    db.set("entity", [{"id": "1"}])
    db.rollback()

    assert db.get("entity") == None


def test_implicit_commit(get_repo):
    db = get_repo(auto_commit=True)

    db.set("entity", [{"id": "1"}])

    assert db.get("entity") == [{"id": "1"}]
