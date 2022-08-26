import os
import pytest
from dfs_shared.domain import model
from dfs_shared.domain import specification
from metadata.filesystem.infrastructure.pickle_repo import PickleRepository


class Person(model.Entity):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name


class PersonByNameSpec(specification.Specification):
    type = Person

    def __init__(self, name):
        super().__init__(lambda p: p.name == name)


@pytest.fixture
def get_repo(tmp_path):
    def factory(objects=None, auto_commit=True):
        objects = objects or list()
        path = os.path.join(tmp_path, "db.pickle")
        repo = PickleRepository(path, auto_commit=auto_commit, seen=set())
        for obj in objects:
            repo.save(obj)
        return repo

    return factory


@pytest.fixture
def person():
    return Person(name="bob", id="1")


def test_get_id_not_found_gives_none(get_repo):
    repo = get_repo()

    assert repo.get(id="100") == None


def test_persistence(get_repo, person):
    repo = get_repo()

    repo.save(person)

    repo = get_repo()

    assert person == repo.get(id="1")


def test_data_not_persisted_if_no_manual_call_to_commit(get_repo, person):
    get_repo([person], auto_commit=False)

    assert get_repo().get(id="1") == None


def test_data_persisted_after_manual_call_to_commit(get_repo, person):
    repo = get_repo([person], auto_commit=False)

    repo.commit()

    assert get_repo().get(id="1") == person


def test_delete(get_repo, person):
    repo = get_repo([person])

    repo.delete("1")

    assert repo.get(id="1") == None


def test_update(get_repo, person):
    repo = get_repo([person])

    person.name = "new_name.txt"

    repo.update(person)

    assert person.name == repo.get("1").name


def test_does_not_update_if_update_method_not_called(get_repo, person):
    repo = get_repo([person])

    person.name = "john"

    assert repo.get("1").name == "bob"


def test_rollback_save(get_repo, person):
    repo = get_repo(auto_commit=False)
    repo.save(person)
    repo.rollback()

    assert repo.get("1") == None


def test_rollback_delete(get_repo, person):
    repo = get_repo([person], auto_commit=False)
    repo.commit()

    repo.delete(person.id)
    repo.rollback()

    assert repo.get("1") == person

def test_rollback_update(get_repo, person):
    repo = get_repo([person], auto_commit=False)
    repo.commit()

    person.name = "john"
    repo.update(person)
    repo.rollback()

    assert repo.get("1").name == "bob"

def test_get_by_specification_not_found(get_repo, person):
    repo = get_repo([person], auto_commit=True)
    spec = PersonByNameSpec("john")

    assert repo.get_by_spec(spec) == None

def test_get_by_specification(get_repo, person):
    repo = get_repo([person], auto_commit=True)
    spec = PersonByNameSpec("bob")

    assert repo.get_by_spec(spec) == person