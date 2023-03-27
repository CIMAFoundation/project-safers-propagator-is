import os
import pika
import ssl
from datetime import datetime
from dotenv import load_dotenv
import random
import logging

from pika.spec import BasicProperties

load_dotenv(dotenv_path=".env", verbose=True)


if __name__ == "__main__":
    from config import RabbitMQConfig
    config = RabbitMQConfig()

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
        # declare the exchange to use passively (just checks it exists), in this case a topic exchange
        channel.exchange_declare(config.RMQ_EXCHANGE, exchange_type="topic", passive=True)

        # example routing key: this must be compatible with the patterns
        # provided by the exchange
        keep_going = True
        sim_id = str(int(random.random() * 100))
        try:
            while keep_going:
                routing_key = "propagator.start.3.5." + sim_id
            #     message = """{
            #     "name": "PROPAGATOR Test Run",
            #     "description": "Test run on 16/12/2021",
            #     "time_limit": 1440,
            #     "do_spotting": true,
            #     "boundary_conditions": [
            #         {
            #             "time": 0,
            #             "w_dir": 30,
            #             "w_speed": 10,
            #             "moisture": 0
            #         },
            #         {
            #             "time": 60,
            #             "w_dir": 15,
            #             "w_speed": 40,
            #             "moisture": 0
            #         }

            #     ],
            #     "init_date": "202112121600",
            #     "ignitions": [
            #     "POINT:44.32372526549074;8.45040310174227"
            #     ]
            # }"""
                message =  """{
                "name":"SAFERS test",
                "description": "SAFERS Test run on 26/05/2022",
                "w_dir":0,
                "w_speed":0,
                "time_limit":1440,
                "do_spotting":false,
                "boundary_conditions":[
                    {
                    "time":0,
                    "w_dir":294,
                    "w_speed":20,
                    "moisture":6,
                    "humidity":45
                    },
                    {
                    "time":480,
                    "w_dir":153,
                    "w_speed":7,
                    "moisture":9,
                    "humidity":45
                    },
                    {
                    "time":1020,
                    "w_dir":305,
                    "w_speed":17,
                    "moisture":7,
                    "humidity":45
                    }
                ],
                "grid_dim_km":40,
                "init_date":"202205260900",
                "ignitions":[
                    "POINT:40.625536;23.003266"
                ]
                }
            """


                # send the message on the given exchange, with the required routing key and body
                channel.basic_publish(
                    exchange=config.RMQ_EXCHANGE, 
                    routing_key=routing_key, 
                    body=message, 
                    properties=BasicProperties(user_id='cima')
                )
                logging.info(f"Sent {routing_key}:{message}")
                val = input("Send again? [y]|n: ")
                keep_going = not val or val == "y"
        except KeyboardInterrupt:
            logging.info("\nDone!")
            keep_going = False
