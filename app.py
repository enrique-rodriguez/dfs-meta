import bottle
from routes import routes
from metadata.bootstrap import bootstrap


def get_app(config):
    bus = bootstrap(config)
    app = bottle.Bottle()
    app.mount("/dfs", routes(bus))
    return app
