import os
from .uow import PickleUnitOfWork
from metadata.filesystem import EVENT_HANDLERS
from metadata.filesystem import COMMAND_HANDLERS
from dfs_shared.application.message_bus import MessageBus


def get_unit_of_work(basedir, db, **config):
    dbpath = os.path.join(basedir, db.get("dbpath"))
    read_model_path = os.path.join(basedir, db.get("read_model_path"))

    return PickleUnitOfWork(
        seen=set(),
        repo_path=dbpath,
        read_model_path=read_model_path,
    )


def bootstrap(config, **kwargs):
    uow = kwargs.pop("uow", get_unit_of_work(**config))

    return MessageBus(uow, COMMAND_HANDLERS, EVENT_HANDLERS)
