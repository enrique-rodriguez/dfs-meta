from metadata.filesystem.domain import model
from metadata.filesystem.domain import events


def test_emits_file_created_event():
    file = model.File.new(id="1", name="file.txt", size=40)

    assert file.events[-1] == events.FileCreated(file=file)


def test_emits_block_added_event():
    file = model.File(id="1", name="file.txt", size=40)
    file.add("1", "123")

    assert file.events[-1] == events.BlockAdded(block=file.blocks[-1])
