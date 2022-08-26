import json
import uuid
import bottle
from bottle import request
from bottle import response
from metadata.filesystem import views
from metadata.filesystem.domain import commands
from metadata.filesystem.domain import exceptions


def routes(bus):
    route = bottle.Bottle()

    @route.get("/")
    def list():
        response.set_header("Content-Type", "application/json")
        return json.dumps(views.list_files(bus.uow))

    @route.post("/")
    def create():
        fid = uuid.uuid4().hex
        name = request.forms.get("name")
        size = request.forms.get("size")
        response.status = 201
        data = {"id": fid}

        try:
            bus.handle(commands.CreateFile(id=fid, name=name, size=size))
        except ValueError:
            response.status = 400
            data = {}

        return data

    @route.get("/<fid>")
    def get(fid):
        file = views.file(fid, bus.uow)
        if not file:
            response.status = 404
            file = {}
        return file

    @route.delete("/<fid>")
    def delete(fid):
        try:
            bus.handle(commands.DeleteFile(fid))
        except FileNotFoundError:
            response.status = 404

        return {}

    @route.post("/<fid>/blocks")
    def add_blocks(fid):
        blks = json.loads(request.forms.get("blocks", "[]"))
        try:
            bus.handle(commands.AddBlocks(fid, blks))
        except FileNotFoundError:
            response.status = 404
        except exceptions.DataNodeNotFoundError:
            response.status = 404
        return {}

    @route.get("/<fid>/blocks")
    def get_blocks(fid):
        blocks = views.blocks(fid, bus.uow)
        response.set_header("Content-Type", "application/json")
        return json.dumps(blocks)

    return route