import bottle
from routes import routes


def get_app(bus, config):
    bottle.BaseRequest.MEMFILE_MAX = config["server"].get("memfile_max", 1048576)
    app = bottle.Bottle()
    app.mount("/dfs", routes(bus))
    return app
