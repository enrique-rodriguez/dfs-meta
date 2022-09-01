import json
import pika
import threading


def consumer_factory(func, bus, logger):
    def inner(ch, method, properties, body):
        return func(bus, json.loads(body.decode("utf-8")), logger)

    inner.__name__ = func.__name__
    return inner


class ConsumerRegistry:
    def __init__(self):
        self.consumers = list()

    def register(self, exchange, exchange_type, callback, **kwargs):
        host = kwargs.pop("host", "localhost")
        exchange_type = kwargs.pop("exchange_type", exchange_type)
        consumer = Consumer(host, callback, exchange, exchange_type)
        self.consumers.append(consumer)
        consumer.start()


class Consumer(threading.Thread):
    def __init__(self, host, consumer, exchange, exchange_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(host)
        self.channel = conn.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        queue_name = consumer.__name__
        self.channel.queue_declare(queue_name, durable=True)
        routing_key = consumer.__name__

        self.channel.queue_bind(
            queue=queue_name, exchange=exchange, routing_key=routing_key
        )
        
        self.channel.basic_consume(
            auto_ack=True,
            queue=queue_name,
            on_message_callback=consumer,
        )

    def get_connection(self, host):
        return pika.BlockingConnection(pika.ConnectionParameters(host=host))

    def run(self):
        self.channel.start_consuming()


registry = ConsumerRegistry()


def register(*args, **kwargs):
    registry.register(*args, **kwargs)
