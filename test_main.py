
import json
import ssl
import pika

from config import PropagatorConfig
from propagator.run_handler import PropagatorRunHandler

from framework.data_uploader import (DataUploadException, Uploader, MockUploader)
from framework.pika_client import PikaClient, MockPikaClient


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

SUPPORTED_DATA_TYPES = [ 35006, 35007, 35008, 35009, 35010, 35011, 35012 ]

run_id = 'links.mr10000038'
user_id, routing_key = run_id.split('.')

output_dir_rel = os.path.join(PropagatorConfig.WORK_DIR, run_id)
os.makedirs(output_dir_rel, exist_ok=True)
param_file = 'message.json'

body = open(os.path.join(output_dir_rel, param_file)).read()

data = parse_request_body(body)
runner = PropagatorRunHandler(run_id, 'routing.key', data, datatype_id=35006,
                              uploader_class=MockUploader, messaging_class=MockPikaClient)
runner.run_propagator()


