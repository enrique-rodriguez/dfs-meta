import json
from app import get_app
from pathlib import Path

with open("conf.json", "r") as f:
    config = json.load(f)

BASE_DIR = Path(__file__).resolve().parent

config["basedir"] = str(Path(__file__).resolve().parent)

host = config["server"].get("host")
port = config["server"].get("port")

app = get_app(config)

app.run(host=host, port=port, debug=True, reloader=True)
