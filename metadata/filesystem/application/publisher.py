import abc


class EventPublisher(abc.ABC):
    @abc.abstractmethod
    def publish(self, channel, data):
        raise NotImplementedError
