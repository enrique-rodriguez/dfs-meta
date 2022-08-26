from dfs_shared.domain import model
from metadata.filesystem.domain import events
from metadata.filesystem.domain import values


class DataNode(model.AggregateRoot):
    def __init__(self, host, port, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.address = values.Address(host, port)
    
    @classmethod
    def new(cls, *args, **kwargs):
        obj = super().new(*args, **kwargs)
        obj.events.append(events.DataNodeCreated(obj))
        return obj


class Block(model.Entity):
    def __init__(self, file_id, datanode_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_id = file_id
        self.datanode_id = datanode_id


class File(model.AggregateRoot):
    def __init__(self, name, size, blocks=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not name or not size:
            raise ValueError
        self.name = name
        self.size = size
        self.blocks = blocks or list()

    def add(self, block_id, datanode_id):
        block = Block(id=block_id, file_id=self.id, datanode_id=datanode_id)
        self.blocks.append(block)
        return self.events.append(events.BlockAdded(block))

    @classmethod
    def new(cls, name, size, *args, **kwargs):
        obj = super().new(name=name, size=size,*args, **kwargs)
        obj.events.append(events.FileCreated(obj))
        return obj
