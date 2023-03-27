import os
import pika
import ssl
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging

load_dotenv(dotenv_path=".env")


BINDING_KEY = "propagator.isochrone.#"

def callback(channel, method, properties, body):
    """
    Simplest callback you can have
    """
    logging.info(f"[{datetime.now()}] Received {method.routing_key}:{body}")


if __name__ == "__main__":
    from config import RabbitMQConfig

    config = RabbitMQConfig()
    logging.info(f"Connecting to {config.RMQ_HOST}:{config.RMQ_PORT}/{config.RMQ_VHOST}")
    logging.info(f"Credentials: {config.RMQ_USERNAME} - {config.RMQ_PASSWORD}")
    # uncomment following line if you encounter troubles with certification authority validation
    # assert os.path.exists(config.CA_FILE) and os.path.isfile(config.CA_FILE)

    # set credentials and SSL params
    credentials = pika.PlainCredentials(config.RMQ_USERNAME, config.RMQ_PASSWORD)
    # uncomment following line if you encounter troubles with certification authority validation
    # ssl_options = pika.SSLOptions(ssl.create_default_context(cafile=config.CA_FILE),config.RMQ_HOST)
    # comment following line if you encounter troubles with certification authority validation
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
    with pika.BlockingConnection(parameters=params) as conn:
        # create channel to the broker
        channel = conn.channel()

        # bind the keys we need to the exchange and predefined queues
        # NOTE: this is **not** necessary if the bindings are defined by hand in the UI
        # passively declare the exchange (check for existence), in this case a topic exchange
        # again, not needed in case of existing queue bindings
        channel.exchange_declare(config.RMQ_EXCHANGE, exchange_type="topic", passive=True)
        channel.queue_bind(queue=config.RMQ_QUEUE, exchange=config.RMQ_EXCHANGE, routing_key=BINDING_KEY)

        logging.info("Waiting for messages, press CTRL+C to exit.")
        try:
            # start listening and consuming messages
            channel.basic_consume(queue=config.RMQ_QUEUE, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
