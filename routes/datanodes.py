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

    @route.post("/")
    def create():
        response.status = 201
        host = request.forms.get("host")
        port = request.forms.get("port")
        did = uuid.uuid4().hex
        data = {"id": did}

        try:
            bus.handle(commands.CreateDataNode(datanode_id=did, host=host, port=port))
        except exceptions.DuplicateDataNodeError:
            response.status = 400
            data = {}

        return data

    @route.get("/")
    def list():
        response.set_header("Content-Type", "application/json")
        return json.dumps(views.list_datanodes(bus.uow))

    @route.get("/<did>")
    def get(did):
        datanode = views.datanode(did, bus.uow)
        if not datanode:
            response.status = 404
            datanode = {}
        return datanode

    return route
