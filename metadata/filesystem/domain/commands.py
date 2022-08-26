from dataclasses import dataclass
from dfs_shared.domain.commands import Command


@dataclass(frozen=True)
class CreateDataNode(Command):
    datanode_id: str
    host: str
    port: int


@dataclass(frozen=True)
class CreateFile(Command):
    id: str
    name: str
    size: int


@dataclass(frozen=True)
class DeleteFile(Command):
    file_id: str


@dataclass(frozen=True)
class AddBlocks(Command):
    file_id: str
    blocks: list
