import uuid
from metadata.filesystem.domain import commands


def register_datanode(bus, body, logger):
    did = uuid.uuid4().hex
    host = body.get("host")
    port = body.get("port")
    logger.info(f"Registrating datanode at {host}:{port}")
    bus.handle(commands.CreateDataNode(datanode_id=did, host=host, port=port))
