import json
import settings
from app import get_app

with open("conf.json", "r") as f:
    config = json.load(f)


config["basedir"] = settings.BASE_DIR

host = config["server"].get("host")
port = config["server"].get("port")

app = get_app(config)

app.run(host=host, port=port, debug=True)
