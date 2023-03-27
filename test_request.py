
import json
import ssl
import pika

from dotenv import load_dotenv

load_dotenv()


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

SUPPORTED_DATA_TYPES = [35006, ]  # 35007, 35008, 35009, 35010]


body = '''{"description": "aaaa", "start": "2023-02-10T16:12:00.000Z", "end": "2023-02-11T03:12:00.000Z", "time_limit": 12, "probabilityRange": 0.75, "do_spotting": false, "boundary_conditions": [{"time": 0, "w_dir": 30, "w_speed": 40, "moisture": 8, "fireBreak": {}}], "title": "ofn.palneca.2019", "geometry": {"type": "Polygon", "coordinates": [[[8.71109, 42.361403], [8.713635, 42.359588], [8.715881, 42.361327], [8.71109, 42.361403]]]}, "datatype_id": "35006"}'''

user_id = 'astro'
routing_key = 'request.35006.astro.122'

_, datatype, *run_id = routing_key.split('.')
run_id = '.'.join(run_id)

output_dir_rel = os.path.join(
    PropagatorConfig.WORK_DIR, run_id + '.' + str(datatype))
os.makedirs(output_dir_rel, exist_ok=True)
param_file = os.path.join(output_dir_rel, 'message.json')

data = parse_request_body(body)
runner = PropagatorRunHandler(user_id, run_id, data, datatype_id=35006)
runner.run_propagator()


