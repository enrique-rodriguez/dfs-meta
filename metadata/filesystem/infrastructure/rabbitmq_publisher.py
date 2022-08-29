import json
import pika
from metadata.filesystem.application import publisher


class RabbitMQPublisher(publisher.EventPublisher):
    def __init__(self, host="localhost"):
        self.host = host
    
    def __enter__(self):
        self.connection = self.get_connection()
        return self.connection
    
    def __exit__(self, *args, **kwargs):
        self.connection.close()

    def publish(self, exchange, route, data):
        with self as conn:
            ch = conn.channel()
            ch.exchange_declare(exchange=exchange, exchange_type="direct")
            body = json.dumps(data)
            ch.basic_publish(exchange=exchange, routing_key=route, body=body)
        
    def get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
