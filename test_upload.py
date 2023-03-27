
import logging
from datetime import datetime

import geopandas as gpd
import shapely

from framework.data_uploader import upload
from models.datalake import DatalakeMetadata

file = 'work/5/isochrones_120.0.geojson'
gdf = gpd.read_file(file)

shapely_polygon = shapely.geometry.box(*gdf.total_bounds)
bbox_geojson = shapely.geometry.mapping(shapely_polygon)

metadata = DatalakeMetadata(
    title='test', 
    notes='test',
    data_temporal_extent_begin_date=datetime.now(),
    data_temporal_extent_end_date=datetime.now(),
    temporalReference_dateOfPublication=datetime.now(),
    temporalReference_dateOfLastRevision=datetime.now(),
    temporalReference_dateOfCreation=datetime.now(),
    temporalReference_date=datetime.now(),
    spatial=bbox_geojson
)

metadata_id = upload(metadata, filepath=file, file_date_start=datetime.now(), file_date_end=datetime.now())
logging.info(metadata_id)
