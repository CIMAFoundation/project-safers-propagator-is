import os
import pika
import ssl
from datetime import datetime
from dotenv import load_dotenv
import random
import logging

from pika.spec import BasicProperties

load_dotenv(dotenv_path=".env", verbose=True)



from config import RabbitMQConfig
config = RabbitMQConfig()

# set credentials and SSL params
credentials = pika.PlainCredentials(
    config.RMQ_USERNAME, config.RMQ_PASSWORD)

# uncomment following line if you encounter troubles with certification authority validation
# ssl_options = pika.SSLOptions(ssl.create_default_context(cafile=config.CA_FILE),config.RMQ_HOST)
# comment following line if you encounter troubles with certification authority validation
ssl_options = pika.SSLOptions(
    ssl.create_default_context(), config.RMQ_HOST)

# create the connection parameters
params = pika.ConnectionParameters(
    host=config.RMQ_HOST,
    port=config.RMQ_PORT,
    virtual_host=config.RMQ_VHOST,
    credentials=credentials,
    ssl_options=ssl_options,
    locale="en_US")

message = '''
{"description": "Test Mirko 21/09/2023 - 2", "do_spotting": false, "probabilityRange": 0.75, "time_limit": 24, "datatype_id": "35008", "boundary_conditions": [{"time": 0, "w_dir": 270, "w_speed": 20, "moisture": 2, "fireBreak": {}}], "start": "2023-09-20T22:00:00Z", "end": "2023-09-21T22:00:00Z", "geometry": {"type": "Point", "coordinates": [1.5034377322904628, 41.30499996684094]}, "request_code": "mr00000125", "title": "Test Mirko 21/09/2023 - 2"}
'''
routing_key = "request.35006.cima.mr00000001"

# create a connection instance and then close it, or use the 'with' scope
with pika.BlockingConnection(parameters=params) as conn:
    # create channel to the broker
    channel = conn.channel()

    # declare the exchange to use passively (just checks it exists), in this case a topic exchange
    channel.exchange_declare(
        config.RMQ_EXCHANGE, 
        exchange_type="topic", 
        passive=True
    )

    # example routing key: this must be compatible with the patterns
    # provided by the exchange

    
    logging.info(f"Sent {routing_key}:{message}")

    # send the message on the given exchange, with the required routing key and body
    channel.basic_publish(
        exchange=config.RMQ_EXCHANGE,
        
        routing_key=routing_key,
        body=message,
        properties=BasicProperties(user_id='cima')
    )

