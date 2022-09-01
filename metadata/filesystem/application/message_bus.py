from dfs_shared.application import message_bus


class MessageBus(message_bus.MessageBus):
    def __init__(self, uow, command_handlers, event_handlers, **deps):
        super().__init__(uow, command_handlers, event_handlers)
        self.deps = deps

    def handle_command(self, command):
        handler = self.command_handlers.get(type(command))

        if not handler:
            raise ValueError(
                f"Handler for command '{command.__class__.__name__}' not found."
            )

        handler(command, uow=self.uow, **self.deps)

    def handle_event(self, event):
        handlers = self.event_handlers.get(type(event), list())

        for handler in handlers:
            handler(event, uow=self.uow, **self.deps)

