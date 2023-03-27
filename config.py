import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

def trygetenv(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        message = f"Missing expected environment variable: {name}"
        raise ValueError(message)


class RabbitMQConfig:
    """
    Simple configuration class loading RMQ settings from the environment.
    Enter your credentials
    """
    RMQ_HOST = trygetenv("RMQ_HOSTNAME")
    RMQ_PORT = trygetenv("RMQ_PORT")
    RMQ_VHOST = trygetenv("RMQ_VHOST")
    RMQ_EXCHANGE = trygetenv("RMQ_EXCHANGE")
    RMQ_USERNAME = trygetenv("RMQ_USERNAME")
    RMQ_PASSWORD = trygetenv("RMQ_PASSWORD")
    RMQ_QUEUE = trygetenv("RMQ_QUEUE")
    # uncomment following line if you encounter troubles with certification authority validation
    CA_FILE = "./cacert.pem"

class PropagatorConfig:
    PYTHON_PATH = trygetenv('PYTHON_PATH')
    PROPAGATOR_DIR = trygetenv('PROPAGATOR_DIR')
    WORK_DIR = trygetenv('WORK_DIR')
