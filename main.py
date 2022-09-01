import json
import logging
import settings
import external
import messaging
from app import get_app
from metadata.bootstrap import bootstrap


def get_config():
    with open("conf.json", "r") as f:
        conf = json.load(f)
    conf["basedir"] = settings.BASE_DIR
    return conf


def start_consumers(bus, logger):
    for exchange, handlers in external.HANDLERS.items():
        for hndlr in handlers:
            callback = messaging.consumer_factory(hndlr, bus, logger)
            messaging.register(exchange, "fanout", callback)


def start_webapp(bus, config):
    host = config["server"].get("host")
    port = config["server"].get("port")

    app = get_app(bus, config)
    app.run(host=host, port=port, debug=config.get("debug", False))


if __name__ == "__main__":
    config = get_config()
    
    logging.basicConfig(
        filename="std.log",
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger()

    bus = bootstrap(config)

    start_consumers(bus, logger)
    start_webapp(bus, config)