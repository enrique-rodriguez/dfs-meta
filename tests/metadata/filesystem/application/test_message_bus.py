import pytest
from unittest import mock
from dataclasses import dataclass
from dfs_shared.domain.commands import Command
from metadata.filesystem.application.message_bus import MessageBus


@dataclass(frozen=True)
class MockCommand(Command):
    pass


def test_raises_value_error_if_command_handler_not_found_for_command():
    bus = MessageBus(uow=mock.Mock(), command_handlers={}, event_handlers={})

    with pytest.raises(ValueError):
        bus.handle(MockCommand())