# PROPAGATOR Intelligent Service


This repository implements the Intelligent Service used by the SAFERS Project to run simulations of the PROPAGATOR Fire Propagation Model developed by CIMA Research Foundation.


## Installation
Create a virtual environment or venv (python3.9+) and install the requirements by running the command 
`pip install -r requirements.txt`

Make sure that the PROPAGATOR software is installed along its data on the same machine where the Runner will be hosted. 


Create a .env file with the following fields and modify the parameters accordingly. 
```.env
RMQ_HOSTNAME=bus.safers-project.cloud 
RMQ_PORT=5674 
RMQ_EXCHANGE=amq.topic 
RMQ_USERNAME=user 
RMQ_PASSWORD=password 
RMQ_VHOST=safers-test 
RMQ_QUEUE=qcima 

# GEODATA REPOSITORY SETTINGS 
OAUTH_URL=https://auth.safers-project.cloud 
CKAN_URL=https://datalake-test.safers-project.cloud/ 
OAUTH_API_KEY=api-key 
OAUTH_APP_ID=app-id 
OAUTH_USER=user 
OAUTH_PWD=password 
PYTHON_PATH=/share/propagator/propagator_sim/.venv/bin/python 
PROPAGATOR_DIR=/share/propagator/propagator_sim/ 
WORK_DIR=./work/ 
```
 
Run the IS as python script by running `main.py`.
