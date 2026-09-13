import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange


class MessageAcknowledger:
    def __init__(self, channel, delivery_tag):
        self.channel = channel
        self.delivery_tag = delivery_tag

    def ack(self):
        self.channel.basic_ack(delivery_tag=self.delivery_tag)

    def nack(self):
        self.channel.basic_nack(delivery_tag=self.delivery_tag, requeue=True)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None
        self.is_consuming = False
        self._user_callback = None

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

    def _on_message_received(self, channel, method, properties, body):
        handler = MessageAcknowledger(channel, method.delivery_tag)
        self._user_callback(body, handler.ack, handler.nack)

    def start_consuming(self, on_message_callback):
        self._user_callback = on_message_callback
        self.is_consuming = True
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._on_message_received,
            auto_ack=False,
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        if not self.is_consuming:
            return

        if self.consumer_tag and self.channel.is_open:
            self.channel.basic_cancel(self.consumer_tag)
        if self.channel.is_open:
            self.channel.stop_consuming()
        self.is_consuming = False

    def close(self):
        if self.is_consuming:
            self.stop_consuming()

        if self.channel and self.channel.is_open:
            self.channel.close()

        if self.connection and self.connection.is_open:
            self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    