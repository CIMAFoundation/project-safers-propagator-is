import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from time import sleep
import enum


from framework.pika_client import PikaClient
import threading
from config import PropagatorConfig
from models.datalake import DatalakeMetadata
from framework.data_uploader import upload
import geopandas as gpd
import shapely
from os.path import getmtime

class ErrorCodes(enum.Enum):
    OK = 0
    GENERIC_ERROR = 1
    DOMAIN_ERROR = 2
    IGNITIONS_ERROR = 3
    BC_ERROR = 4

@dataclass
class Wrapper:
    process: any = field(init=False)
    program_cmd: list[str]
    cwd: str
    logger = logging
    end_callback: callable
    progress_callback: callable
    error_callback: callable
    
    def __post_init__(self):
        self.running_file = os.path.join(self.cwd, 'running')
        self.error_file = os.path.join(self.cwd, 'error')

    def __real_thread(self):
        # create running file
        with open(self.running_file, 'w') as f:
            f.write('')

        self.logger.info(f'Executing command: {" ".join(self.program_cmd)}')
        try:
            with subprocess.Popen(
                    self.program_cmd,
                    cwd=self.cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=0,
                    universal_newlines=True
            ) as p:

                self.process = p
                accum_stdout = []
                accum_stderr = []

                # i.e. while the process is running
                while p.poll() is None:
                    line = p.stdout.readline()
                    if line:
                        self.logger.info(line)
                        accum_stdout.append(line)
                        self.progress_callback(line)

                if p.returncode > 0:
                    stderr = p.stderr.read()
                    self.logger.warning(
                        'Error in simulation:\n{}'.format(stderr))
                    accum_stderr.append(stderr)
                    error_code = ErrorCodes(p.returncode).name
                    self.error_callback(f'Error running simulation: {error_code}')

        except Exception as exp:
            self.logger.error('Error running simulation')
            # create an error file
            with open(self.error_file, 'w') as f:
                f.write(str(exp))

            raise

        finally:
            os.remove(self.running_file)
            # remove running file
            self.end_callback()

        

    def __dummy_thread(self):
        """
        Thread that will only check if the simulation is running
        Will terminate when the "running" file is deleted
        """
        while self.is_running():
            sleep(0.5)

        # log the end of the simulation
        self.logger.info('Simulation finished')

        # check if there is an error file
        if os.path.exists(self.error_file):
            
            self.logger.error('Simulation finished with errors')

            with open(self.error_file, 'r') as f:
                error = f.read()
            self.error_callback(error)
            return
        
        # call the callback
        self.end_callback()

    def is_running(self):
        """
        Check if the simulation is running
        """
        return os.path.exists(self.running_file)


    def start(self):
        if self.is_running():
            self.logger.info('Simulation is already running')
            t = threading.Thread(target=self.__dummy_thread)
        else:
            t = threading.Thread(target=self.__real_thread)

        t.start()
        logging.info("Main: before running thread")
        t.join()
        logging.info("Main: Thread finished")





if __name__ == '__main__':
    # create a test wrapper and launch sim
    wrapper = Wrapper(
        program_cmd=['python3', 'test.py'],
        cwd='/home/propagator/propagator',
        logger=logging,
        end_callback=lambda: print('callback'),
        error_callback=lambda: print('error_callback')
    )