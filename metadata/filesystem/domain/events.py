from . import model
from dataclasses import dataclass
from dfs_shared.domain import events


@dataclass(frozen=True)
class FileCreated(events.Event):
    file: model.File


@dataclass(frozen=True)
class FileDeleted(events.Event):
    file: model.File


@dataclass(frozen=True)
class BlockAdded(events.Event):
    block: model.Block


@dataclass(frozen=True)
class DataNodeCreated(events.Event):
    datanode: model.DataNode


@dataclass(frozen=True)
class DataNodeUpdated(events.Event):
    datanode: model.DataNode
