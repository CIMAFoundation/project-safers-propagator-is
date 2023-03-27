import json
import logging
import os
import pdb
import ssl
from datetime import datetime

import pika
import requests
from dotenv import load_dotenv
from requests import HTTPError

load_dotenv(".env", verbose=True)

REFRESH_TOKEN = None

# Get access token


def get_access_token():
    url = f'{os.getenv("OAUTH_URL")}/api/login'
    body = {
        "loginId": os.getenv("OAUTH_USER"),
        "password": os.getenv("OAUTH_PWD"),
        "applicationId": os.getenv("OAUTH_APP_ID"),
        "noJWT": False
    }
    headers = {
        "Authorization": os.getenv("OAUTH_API_KEY"),
    }
    try:
        response = requests.post(url, json=body, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.error(f"Other error occurred: {err}")  # Python 3.6
    else:
        logging.info("Access Token obtained")
    if response.status_code == 200:
        REFRESH_TOKEN = response.json()["refreshToken"]
    else:
        logging.error('Error:')
        logging.error(response.json())
        raise Exception
    return response.json()["token"]


def refresh_token():
    url = f'{os.getenv("OAUTH_URL")}/jwt/refresh'
    body = {
        "refresh_token": REFRESH_TOKEN
    }
    try:
        response = requests.post(url, data=body)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.error(f"Other error occurred: {err}")  # Python 3.6
    else:
        logging.error("Access Token obtained")

    return response.json()["token"]


def send_notification_example(message):
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

    # create a connection instance and then close it, or use the 'with' scope
    with pika.BlockingConnection(parameters=params) as conn:
        # create channel to the broker
        channel = conn.channel()
        # declare the exchange to use passively (just checks it exists), in this case a topic exchange
        channel.exchange_declare(
            config.RMQ_EXCHANGE, exchange_type="topic", passive=True)

        datatype_id = message.get('datatype_id')
        routing_key = f"newexternaldata.{datatype_id[0]}.{datatype_id[1]}.{datatype_id}"
        # send the message on the given exchange, with the required routing key and body
        channel.basic_publish(exchange=config.RMQ_EXCHANGE,
                              routing_key=routing_key, body=json.dumps(message))
        logging.info(f"Sent {routing_key}:{message}")
