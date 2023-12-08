
import json
import logging
import os
import ssl
import sys
from datetime import datetime

import pika

from config import PropagatorConfig
from framework.data_uploader import DataUploadException, MockUploader, Uploader
from framework.pika_client import MockPikaClient, PikaClient
from propagator.run_handler import PropagatorRunHandler
from propagator.utils import parse_request_body

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("info.log"),
        logging.StreamHandler()
    ]
)

SUPPORTED_DATA_TYPES = [35006, 35007, 35008,
                        35009, 35010, 35011, 35012, 35013, 35014]

run_id = 'links.mr00000324'  # sys.argv[1]
user_id = run_id.split('.')[0]
routing_key = run_id


output_dir_rel = os.path.join(PropagatorConfig.WORK_DIR, run_id)
os.makedirs(output_dir_rel, exist_ok=True)
param_file = 'message.json'

body = open(os.path.join(output_dir_rel, param_file)).read()

# rm 'running' file from work dir
if os.path.exists(os.path.join(output_dir_rel, 'running')):
    os.remove(os.path.join(output_dir_rel, 'running'))

if os.path.exists(os.path.join(output_dir_rel, 'completed')):
    os.remove(os.path.join(output_dir_rel, 'completed'))


data = parse_request_body(body)

runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35014,
                              uploader_class=MockUploader, messaging_class=MockPikaClient)

# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35006,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)

# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35007,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)

# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35008,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)


# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35009,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)

# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35010,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)


# runner = PropagatorRunHandler(run_id, routing_key, data, datatype_id=35011,
#                               uploader_class=MockUploader, messaging_class=MockPikaClient)


runner.run_propagator()
