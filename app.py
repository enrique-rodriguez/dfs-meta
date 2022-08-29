import bottle
from routes import routes
from metadata.bootstrap import bootstrap


def get_app(config, *args, **kwargs):
    bottle.BaseRequest.MEMFILE_MAX = config["server"].get("memfile_max", 1048576)
    bus = bootstrap(config, *args, **kwargs)
    app = bottle.Bottle()
    app.mount("/dfs", routes(bus))
    return app
