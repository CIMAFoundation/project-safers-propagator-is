import ssl

import pika

from config import RabbitMQConfig
import logging

config = RabbitMQConfig()

class MockPikaClient:
    def __init__(self, exchange=config.RMQ_EXCHANGE):
        self.exchange = exchange

    def write_message(self, routing_key, message, properties=None):
        logging.info(f"Writing message to {self.exchange} with routing key {routing_key}")
        logging.info(f"Message: {message}")
        return True

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

class PikaClient:
    def __init__(self, exchange=config.RMQ_EXCHANGE):
        self.exchange = exchange
        credentials = pika.PlainCredentials(config.RMQ_USERNAME, config.RMQ_PASSWORD)
        ssl_options = pika.SSLOptions(ssl.create_default_context(), config.RMQ_HOST)

        # create the connection parameters
        params = pika.ConnectionParameters(
            host=config.RMQ_HOST,
            port=config.RMQ_PORT,
            virtual_host=config.RMQ_VHOST,
            credentials=credentials,
            ssl_options=ssl_options,
            locale="en_US")


        # create a connection instance and then close it, or use the 'with' scope
        self.conn = pika.BlockingConnection(parameters=params)
        # create channel to the broker
        self.channel = self.conn.channel()
        # declare the exchange to use passively (just checks it exists), in this case a topic exchange
        self.channel.exchange_declare(
            self.exchange, exchange_type="topic", passive=True)

    def write_message(self, routing_key, message, properties=None):
        return self.channel.basic_publish(exchange=self.exchange, routing_key=routing_key, body=message, properties=properties)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.conn.close()
