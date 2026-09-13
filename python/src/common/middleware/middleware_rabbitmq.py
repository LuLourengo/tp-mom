import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):

        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None
        self.is_consuming = False

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(f"Fallo de conexion con el broker en {self.host}: {err}")
        except Exception as err:
            raise MessageMiddlewareMessageError(f"No se pudo declarar la cola '{self.queue_name}': {err}")

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as err:
            raise MessageMiddlewareDisconnectedError(
                f"Fallo de conexion al enviar a la cola '{self.queue_name}': {err}"
            )
        except Exception as err:
            raise MessageMiddlewareMessageError(
                f"Error al enviar mensaje a la cola '{self.queue_name}': {err}"
            )

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
 