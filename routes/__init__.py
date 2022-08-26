import bottle

from .files import routes as file_routes
from .datanodes import routes as datanode_routes


def routes(bus):
    root = bottle.Bottle()

    root.mount("/files", file_routes(bus))
    root.mount("/datanodes", datanode_routes(bus))

    return root