import json
import logging
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime

PRIVATE = True
IDENTIFICATION_RESOURCE_TYPE = "dataset"
DATATYPE_ID = 35005
OWNER_ORG = "safers"
IDENTIFICATION_COUPLED_RESOURCE = ""
IDENTIFICATION_RESOURCE_LANGUAGE = "eng"
CLASSIFICATION_TOPIC_CATEGORY = "environment"
CLASSIFICATION_SPATIAL_DATA_SERVICE_TYPE = ""
KEYWORD_KEYWORD_VALUE = "Wildfire, Fire, Forest Fire, Land Fire, Hazard, Delineation Map"
KEYWORD_ORIGINATING_CONTROLLED_VOCABULARY = "ontology"
QUALITY_AND_VALIDITY_LINEAGE = "Quality approved"
QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_LATITUDE = "0"
QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_LONGITUDE = "0"
QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_SCALE = "20"
QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_MEASUREUNIT = "m"
CONFORMITY_SPECIFICATION_TITLE = "COMMISSION REGULATION (EU) No 1089/2010 of 23 November 2010 implementing Directive 2007/2/EC of the European Parliament and of the Council as regards interoperability of spatial data sets and services"
CONFORMITY_SPECIFICATION_DATETYPE = "publication"
CONFORMITY_SPECIFICATION_DATE = "2010-12-08T00:00:00"
CONFORMITY_DEGREE = "true"
CONSTRAINTS_CONDITIONS_FOR_ACCESS_AND_USE = "cc-by"
CONSTRAINTS_LIMITATION_ON_PUBLIC_ACCESS = ""
RESPONSABLE_ORGANIZATION_NAME = "Copernicus EMS Rapid Mapping Team"
RESPONSABLE_ORGANIZATION_EMAIL = "mapping@copernicus.com"
RESPONSABLE_ORGANIZATION_ROLE = "author"
POINT_OF_CONTACT_NAME = "CIMA Research Foundation"
POINT_OF_CONTACT_EMAIL = "incendi@cimafoundation.org"
METADATA_LANGUAGE = "eng"
COORDINATE_SYSTEM_REFERENCE_CODE = "4326"
COORDINATESYSTEMREFERENCE_CODESPACE ="EPSG"
CHARACTER_ENCODING ="UTF-8"

class DatalakeJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        if isinstance(o, datetime):
            return o.isoformat()

        return super().default(o)

@dataclass
class DatalakeMetadata:
    name: str = field(init=False, default_factory=lambda: str(uuid.uuid4()))

    title: str = field(init=True)
    notes: str = field(init=True)
    data_temporal_extent_begin_date: datetime  = field(init=True)
    data_temporal_extent_end_date: datetime = field(init=True)
    temporalReference_dateOfPublication: datetime = field(init=True)
    temporalReference_dateOfLastRevision: datetime = field(init=True)
    temporalReference_dateOfCreation: datetime = field(init=True)
    temporalReference_date: datetime = field(init=True)
    spatial: dict = field(init=True)

    external_attributes: dict = field(default_factory=dict, init=True)

    private: bool = field(default=PRIVATE, init=False)
    datatype_id: int = field(default=DATATYPE_ID, init=True)
    owner_org: str = field(default=OWNER_ORG, init=False)

    identification_ResourceType: str = field(default=IDENTIFICATION_RESOURCE_TYPE, init=False)
    identification_CoupledResource: str = field(default=IDENTIFICATION_COUPLED_RESOURCE, init=False)
    identification_ResourceLanguage: str = field(default=IDENTIFICATION_RESOURCE_LANGUAGE, init=False)
    classification_TopicCategory: str = field(default=CLASSIFICATION_TOPIC_CATEGORY, init=False)
    classification_SpatialDataServiceType: str = field(default=CLASSIFICATION_SPATIAL_DATA_SERVICE_TYPE, init=False)
    keyword_KeywordValue: str = field(default=KEYWORD_KEYWORD_VALUE, init=False)
    keyword_OriginatingControlledVocabulary: str = field(default=KEYWORD_ORIGINATING_CONTROLLED_VOCABULARY, init=False)
    quality_and_validity_lineage: str = field(default=QUALITY_AND_VALIDITY_LINEAGE, init=False)
    quality_and_validity_spatial_resolution_latitude: int = field(default=QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_LATITUDE, init=False)
    quality_and_validity_spatial_resolution_longitude: int = field(default=QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_LONGITUDE, init=False)
    quality_and_validity_spatial_resolution_scale: int = field(default=QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_SCALE, init=False)
    quality_and_validity_spatial_resolution_measureunit: str = field(default=QUALITY_AND_VALIDITY_SPATIAL_RESOLUTION_MEASUREUNIT, init=False)
    conformity_specification_title: str = field(default=CONFORMITY_SPECIFICATION_TITLE, init=False)
    conformity_specification_dateType: str = field(default=CONFORMITY_SPECIFICATION_DATETYPE, init=False)
    conformity_specification_date: datetime = field(default=CONFORMITY_SPECIFICATION_DATE, init=False)
    conformity_degree: bool = field(default=CONFORMITY_DEGREE, init=False)
    constraints_conditions_for_access_and_use: str = field(default=CONSTRAINTS_CONDITIONS_FOR_ACCESS_AND_USE, init=False)
    constraints_limitation_on_public_access: str = field(default=CONSTRAINTS_LIMITATION_ON_PUBLIC_ACCESS, init=False)
    responsable_organization_name: str = field(default=RESPONSABLE_ORGANIZATION_NAME, init=False)
    responsable_organization_email: str = field(default=RESPONSABLE_ORGANIZATION_EMAIL, init=False)
    responsable_organization_role: str = field(default=RESPONSABLE_ORGANIZATION_ROLE, init=False)
    point_of_contact_name: str = field(default=POINT_OF_CONTACT_NAME, init=False)
    point_of_contact_email: str = field(default=POINT_OF_CONTACT_EMAIL, init=False)
    metadata_language: str = field(default=METADATA_LANGUAGE, init=False)
    coordinatesystemreference_code: int = field(default=COORDINATE_SYSTEM_REFERENCE_CODE, init=False)
    coordinatesystemreference_codespace: str = field(default=COORDINATESYSTEMREFERENCE_CODESPACE, init=False)
    character_encoding: str = field(default=CHARACTER_ENCODING, init=False)
    


    def as_json_string(self):
        json_string = json.dumps(self, cls=DatalakeJSONEncoder)
        return json_string

    def as_json_dict(self):
        data = json.loads(self.as_json_string())
        return data

@dataclass
class DatalakeResourceMetadata:
    notes:str = field(init=True)
    package_id:str = field(init=True)
    file_date_start: datetime = field(init=True)
    file_date_end: datetime = field(init=True)
    name: str = field(init=True)
    format: str = field(init=True)
    request_code: str = field(init=True)
    datatype_resource: str = field(init=True)

    def as_json_string(self):
        json_string = json.dumps(self, cls=DatalakeJSONEncoder)
        return json_string

    def as_json_dict(self):
        data = json.loads(self.as_json_string())
        return data

