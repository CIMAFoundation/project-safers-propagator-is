import json
import logging
import os
import traceback
            
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from os.path import getmtime
from typing import List, Tuple, Union

import geopandas as gpd
import pandas as pd

import rasterio as rio
import shapely
from pika.spec import BasicProperties

from config import PropagatorConfig
from framework.data_uploader import (DataUploadException, Uploader, MockUploader)
from framework.pika_client import PikaClient, MockPikaClient
from models.datalake import DatalakeMetadata
from propagator.utils import mask_on_cutoff
from propagator.wrapper import Wrapper

DEFAULT_RUN_LENGHT = 72

DEFAULT_DATATYPE_ID = 35006

UpdateType = Union['start','update','end','layer']

class RunException(Exception):
    pass


    
@dataclass
class PropagatorRunHandler:
    user_id: str
    run_id: str
    params: dict
    
    datatype_id: field(default=DEFAULT_DATATYPE_ID, init=True)

    output_dir: str = field(init=False)
    title: str = field(init=False)
    notes: str = field(init=False)
    start_date: datetime = field(init=False)
    end_date: datetime = field(init=False)
    probability_range: float = field(init=False)
    
    messaging_class: Union[PikaClient, MockPikaClient] = field(init=True, default=PikaClient)
    uploader_class: Union[Uploader, MockUploader] = field(init=True, default=Uploader)

    _message_properties: BasicProperties = field(init=False)

    

    def __post_init__(self):
        output_dir_rel = os.path.join(PropagatorConfig.WORK_DIR, self.run_id)

        self.output_dir = os.path.abspath(output_dir_rel)

        print('output_dir', self.output_dir)

        
        self.start_date = datetime.now()
        start_date_str = self.params.get('init_date', None)


        if start_date_str is not None:
            self.start_date = datetime.strptime(start_date_str, '%Y%m%d%H%M')
        
        self.title = self.params.get('name', f'Propagator Run {self.start_date}')
        self.notes = self.params.get('description', f'Propagator Run {self.start_date}')

        duration = self.params.get('time_limit', DEFAULT_RUN_LENGHT*60)
        self.end_date = self.start_date + timedelta(minutes=duration)

        self.probability_range = self.params.get('probabilityRange', 0.75)

        self.run_routing_key = f'status.propagator.{self.datatype_id}.{self.run_id}'

        self._message_properties = BasicProperties(
            content_type='application/json', 
            content_encoding='utf-8', 
            user_id='cima', 
            app_id='propagator', 
            delivery_mode=2,
            message_id=self.run_id
        )

    def send_message(self, 
        message: str, 
        status_code: int=200,
        datatype_id: int=None, 
        type: UpdateType='update', 
        urls:List[str]=[],
        routing_key=None
        ):
        """
        Sends a message to the bus
        @param message: the message to send
        @param status_code: the status code of the message
        @param datatype_id: the datatype id
        @param type: the type of the message
        @param urls: optional urls
        @param routing_key: the routing key to use
        """
        if datatype_id is None:
            datatype_id = self.datatype_id

        if routing_key is None:
            routing_key = self.run_routing_key

        message = {
            'datatype_id': datatype_id,
            'status_code': status_code,
            'type': type,
            'urls': urls,
            'message': message
        }
        logging.info(f'Sending message to {routing_key}')

        with self.messaging_class(exchange='safers.b2b') as client:
            client.write_message(
                routing_key=routing_key,
                message=json.dumps(message),
                properties=self._message_properties
            )            

    def send_error_message(self, 
        message: str, 
        exp:Exception=None, 
        status_code: int=500,
        type: UpdateType = 'end'):
        """
        Sends an error message to the bus
        @param message: the message to send
        @param exp: the exception that caused the error
        @param type: the type of the message
        """
        if exp is not None:
            message = f'{message}: {exp}'

        self.send_message(message, status_code=status_code, type=type)

    def upload_and_notify(
            self, 
            metadata_id: str, 
            file_path: str, 
            start_date: datetime, 
            end_date: datetime, 
            format: str,
            datatype_resource: int
        ) -> str:
        """
        Uploads a file to the datalake and notifies the bus about it
        @param metadata_id: the metadata id of the file
        @param file_path: the path of the file to upload
        @param start_date: the start date of the file
        @param end_date: the end date of the file
        @param file_type: the file type
        @param datatype_resource: the datatype resource
        @return: the url of the uploaded file
        """
        
        url = self.uploader_class.upload(
            metadata_id, 
            file_path, 
            start_date,
            end_date, 
            format, 
            request_code=self.run_id, 
            datatype_resource=datatype_resource
        )
        logging.info(f'Uploaded {file_path} to datalake: {url}')
        
        data_routing_key = f'status.propagator.{datatype_resource}.{self.run_id}'
        logging.info(f'Sending message to {data_routing_key}')
        self.send_message(
            message=f'{self.run_id} completed',
            datatype_id=datatype_resource,
            type='end',
            status_code=200,
            routing_key=data_routing_key,
            urls=[url]
        )
        return url

    def run_progress_callback(self, progress_message: str):
        """
        Callback to be called when the progress of the run changes
        @param progress: the progress of the run
        """
        #self.send_message(progress_message, type='update')
        pass

    def run_end_callback(self):
        """
        Callback to be called when the run is finished
        """            
        try:
            isochrone_file = self.get_last_file('isochrone', 'geojson')
            # extract isochrone file for value threshold
            isochrones_gdf, isochrone_file, isochrone_isotime_file = self.extract_isochrones(isochrone_file)
            bbox_geojson = self.get_bbox(isochrones_gdf)

        except ValueError:
            message = 'LOW_PROBABILITY'
            self.send_error_message(message, type='end', status_code=500)
            return
        
        try:

            metadata = DatalakeMetadata(
                title=self.title, 
                notes=self.notes,
                data_temporal_extent_begin_date=self.start_date,
                data_temporal_extent_end_date=self.end_date,
                temporalReference_dateOfPublication=datetime.now(),
                temporalReference_dateOfLastRevision=datetime.now(),
                temporalReference_dateOfCreation=datetime.now(),
                temporalReference_date=self.start_date,
                spatial=bbox_geojson,
                external_attributes={
                    'request_code': self.run_id
                }
            )
            metadata_id = self.uploader_class.upload_metadata(metadata)

            urls = []

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35007:
                isochrone_res_url = self.upload_and_notify(metadata_id, isochrone_file, self.start_date,
                                        self.end_date, 'GeoJSON', datatype_resource=35007)
                urls.append(isochrone_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35012:
                isochrone_isotime_res_url = self.upload_and_notify(metadata_id, isochrone_isotime_file, self.start_date,
                                        self.end_date, 'GeoJSON', datatype_resource=35012)
                urls.append(isochrone_isotime_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35010:
                ros_mean_file = self.get_last_file('RoS_mean', 'tiff')
                ros_mean_file = mask_on_cutoff(ros_mean_file, isochrones_gdf, self.probability_range)
                ros_mean_res_url = self.upload_and_notify(metadata_id, ros_mean_file, self.start_date, self.end_date, 'tiff', datatype_resource=35010)
                urls.append(ros_mean_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35011:
                ros_max_file = self.get_last_file('RoS_max', 'tiff')
                ros_max_file = mask_on_cutoff(ros_max_file, isochrones_gdf, self.probability_range)
                ros_max_res_url = self.upload_and_notify(metadata_id, ros_max_file, self.start_date, self.end_date, 'tiff', datatype_resource=35011)
                urls.append(ros_max_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35008:
                fireline_intensity_max_file = self.get_last_file('fireline_intensity_max', 'tiff')
                fireline_intensity_max_file = mask_on_cutoff(fireline_intensity_max_file, isochrones_gdf, self.probability_range)
                intensity_mean_res_url = self.upload_and_notify(metadata_id, fireline_intensity_max_file, self.start_date, self.end_date, 'tiff', datatype_resource=35008)
                urls.append(intensity_mean_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID or self.datatype_id == 35009:
                fireline_intensity_mean_file = self.get_last_file('fireline_intensity_mean', 'tiff')
                fireline_intensity_mean_file = mask_on_cutoff(fireline_intensity_mean_file, isochrones_gdf, self.probability_range)
                intensity_max_res_url = self.upload_and_notify(metadata_id, fireline_intensity_mean_file, self.start_date, self.end_date, 'tiff', datatype_resource=35009)
                urls.append(intensity_max_res_url)

            if self.datatype_id == DEFAULT_DATATYPE_ID:                
                message = f'{self.run_id} completed'
                self.send_message(message, urls=urls, status_code=200, type='end')
        
        except Exception as exp:
            traceback.print_exc()
            logging.error(exp)
            message = f'{self.run_id} error: {exp}'
            self.send_error_message(message, exp=exp, type='end')

    def run_error_callback(self, error):
        """
        Callback for error: sends error message to the queue
        @param error: error message
        """
        self.send_error_message(f'{self.run_id} error: {error}', type='end')


    def get_last_file(self, output_prefix: str, output_type: str):
        """
        Returns the last file in the output directory with the given prefix and type
        @param output_prefix: prefix of the file
        @param output_type: type of the file
        """
        output_files = os.listdir(self.output_dir)
        
        files = list(
            map(lambda f: f'{self.output_dir}/{f}',
                filter(lambda f: f.startswith(output_prefix) and f.endswith(output_type), 
                output_files
        )))

        files.sort(key=lambda f: getmtime(f))
        if len(files) == 0:
            raise ValueError()

        last_file = files[-1]
        return last_file

    def extract_isochrones(self, isochrone_file: str):
        """
        Extracts isochrones with desired value from the isochrone file and saves them to a new file
        @param isochrone_file: path to the source isochrone file
        @return gdf: the extracted isochrones
        @return isochrone_file: path to the new isochrone file
        @return isochrone_isotime_file: path to the new isochrone file with isotime
        """
        gdf = gpd.read_file(isochrone_file)
        # filter out features with property 'value' == self.probabilityRange
        try:
            gdf = gdf.query(f'value == {self.probability_range}')
        except:
            raise ValueError()
        
        if gdf.empty:
            raise ValueError()
        
        isochrone_file = f'{self.output_dir}/extracted_isochrone_prob_{self.probability_range}.geojson'
        gdf['timeString'] = gdf['time'].apply(
            lambda x: (self.start_date + pd.Timedelta(hours=x)).strftime('%H:%M')
        )
        gdf.to_file(isochrone_file, driver='GeoJSON')

        isochrone_isotime_file = f'{self.output_dir}/extracted_isochrone_prob_{self.probability_range}_isotime.geojson'
        gdf_iso = gdf.copy()
        gdf_iso['time'] = gdf['time'].apply(
            lambda x: (self.start_date + pd.Timedelta(hours=x)).isoformat())
        gdf_iso.to_file(isochrone_isotime_file, driver='GeoJSON')

        return gdf, isochrone_file, isochrone_isotime_file


    def get_bbox(self, gdf: gpd.GeoDataFrame):
        """
        Returns bounding box of the isochrones
        """
        # save filtered features to new file
        lonmin, latmin, lonmax, latmax = gdf.total_bounds            
        shapely_polygon = shapely.geometry.box(lonmin, latmin, lonmax, latmax)
        bbox_geojson = shapely.geometry.mapping(shapely_polygon)            

        return bbox_geojson

    def run_propagator(self):
        os.makedirs(self.output_dir, exist_ok=True)

        param_file = os.path.join(self.output_dir, self.run_id + '.json')
        with open(param_file, 'w') as fp:
            json.dump(self.params, fp)

        propagator_main = os.path.join(PropagatorConfig.PROPAGATOR_DIR, 'main.py')

        wrapper = Wrapper(
            program_cmd=[
                PropagatorConfig.PYTHON_PATH,
                '-u', propagator_main,
                '-id', self.run_id,
                '-f', param_file,
                '-of', self.output_dir,
                '-tl', str((self.end_date - self.start_date).seconds//3600)
            ], 
            cwd=PropagatorConfig.PROPAGATOR_DIR,
            run_dir=self.output_dir,
            end_callback=self.run_end_callback,
            progress_callback=self.run_progress_callback,
            error_callback=self.run_error_callback
        )

        wrapper.start()
