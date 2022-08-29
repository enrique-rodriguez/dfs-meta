from .domain import events
from .domain import commands
from .application import handlers


COMMAND_HANDLERS = {
    commands.AddBlocks: handlers.add_blocks,
    commands.CreateFile: handlers.create_file,
    commands.DeleteFile: handlers.delete_file,
    commands.CreateDataNode: handlers.create_datanode,
}


EVENT_HANDLERS = {
    events.FileCreated: [handlers.add_file_to_read_model],
    events.BlockAdded: [handlers.add_block_to_read_model],
    events.DataNodeCreated: [handlers.add_datanode_to_read_model],
    events.FileDeleted: [
        handlers.remove_file_from_read_model,
        handlers.remove_blocks_from_read_model,
        handlers.publish_file_deleted_event,
    ],
}
