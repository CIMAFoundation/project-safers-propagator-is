
import json
import ssl
import pika

from config import RabbitMQConfig
from config import PropagatorConfig
from propagator.run_handler import PropagatorRunHandler
import logging
from datetime import datetime
import logging
from propagator.utils import parse_request_body
import os


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("info.log"),
        logging.StreamHandler()
    ]
)

SUPPORTED_DATA_TYPES = [35006, ]#35007, 35008, 35009, 35010]

def callback(channel, method, properties, body):
    user_id = properties.user_id
    routing_key: str = method.routing_key

    logging.info(f"Received message: user_id: {user_id}, routing_key: {routing_key}")
    logging.info(f"body: {body}")
    
    _, datatype, *run_id = routing_key.split('.')
    if int(datatype) not in SUPPORTED_DATA_TYPES: return

    run_id = '.'.join(run_id)

    output_dir_rel = os.path.join(PropagatorConfig.WORK_DIR, run_id + '.' + str(datatype))
    os.makedirs(output_dir_rel, exist_ok=True)
    param_file = os.path.join(output_dir_rel, 'message.json')
    
    with open(param_file, 'w') as fp:
        json.dump(json.loads(body),fp)


    logging.info(f"run_id: {run_id}")
    
    data = parse_request_body(body)
    runner = PropagatorRunHandler(user_id, run_id, data, datatype_id=35006)
    runner.run_propagator()


def main():
    config = RabbitMQConfig()
    logging.info(f"Connecting to {config.RMQ_HOST}:{config.RMQ_PORT}/{config.RMQ_VHOST}")

    # set credentials and SSL params
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
    with pika.BlockingConnection(parameters=params) as conn:
        # create channel to the broker
        channel = conn.channel()

        # bind the keys we need to the exchange and predefined queues
        channel.exchange_declare(config.RMQ_EXCHANGE, exchange_type="topic", passive=True)
        
        #channel.queue_bind(queue=config.RMQ_QUEUE, exchange=config.RMQ_EXCHANGE, routing_key=BINDING_KEY)
        logging.info("Waiting for messages")

        try:
            # start listening and consuming messages
            channel.basic_consume(queue=config.RMQ_QUEUE, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()


if __name__ == "__main__":
    main()
