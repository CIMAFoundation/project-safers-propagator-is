import json

from datetime import datetime

from geojson import FeatureCollection
from geomet import wkt


def parse_request_body(body):
    data = json.loads(body)
    params = data.copy()

    if 'start' in data:
        start_date_str = data['start']
        iso_start_date_str = start_date_str.rsplit('.')[0]
        start_date = datetime.fromisoformat(iso_start_date_str)
        propagator_date = start_date.strftime('%Y%m%d%H%M')
        params['init_date'] = propagator_date
        # delete start date from params
        del params['start']

        if 'end' in data:
            end_date_str = data['end']
            iso_end_date_str = end_date_str.rsplit('.')[0]
            end_date = datetime.fromisoformat(iso_end_date_str)
            duration = round((end_date - start_date).total_seconds() / 60)
            params['time_limit'] = duration

            # delete end date from params
            del params['end']


    if 'geometry' in data:
        params['ignitions'] = transform_features_to_propagator_strings(data['geometry'])
        # delete geometry from params
        del params['geometry']

    if 'boundary_conditions' not in data:
        return params
    
    # transform actions to suitable propagator format
    for bc in data['boundary_conditions']:
        if 'fireBreak' not in bc:
            continue

        for action_type, wkt in bc['fireBreak'].items():
            if action_type == 'vehicle':
                action_type = 'heavy_action'
            if action_type == 'waterLine':
                action_type = 'waterline_action'

            bc[action_type] = wkt_to_propagator_strings(wkt)
            
        del bc['fireBreak']

    return params


def read_actions(imp_points_string):
    strings = imp_points_string.split('\n')

    polys, lines, points = [], [], []

    for s in strings:
        f_type, values = s.split(':')
        values = values.replace('[', '').replace(']', '')
        if f_type == 'POLYGON':
            s_lats, s_lons = values.split(';')
            lats = [float(sv) for sv in s_lats.split()]
            lons = [float(sv) for sv in s_lons.split()]
            polys.append((lats, lons))

        elif f_type == 'LINE':
            s_lats, s_lons = values.split(';')
            lats = [float(sv) for sv in s_lats.split()]
            lons = [float(sv) for sv in s_lons.split()]
            lines.append((lats, lons))

        elif f_type == 'POINT':
            s_lat, s_lon = values.split(';')
            lat, lon = float(s_lat), float(s_lon)
            points.append((lat, lon))

    return polys, lines, points 



def get_geometry_string(coordinates, geometry_type):
    """
    Transform geometry to propagator string
    :param geometry: geometry coordinates
    :param geometry_type: geometry type
    :return: propagator string
    """
    propagator_type = None
    if geometry_type == 'Point':
        propagator_type = 'POINT'
    elif geometry_type == 'LineString':
        propagator_type = 'LINE'
    elif geometry_type ==  'Polygon':
        propagator_type = 'POLYGON'
    
    if propagator_type is None:
        raise ValueError(f'Unknown geometry type: {geometry_type}')

    # extract lats and lons from geometry
    if propagator_type == 'POINT':
        lon, lat = coordinates
        propagator_string = f'{propagator_type}:{lat};{lon}'
    else:
        if propagator_type == 'POLYGON':
            # skip holes
            coordinates = coordinates[0]

        lons, lats = list(zip(*coordinates))
        propagator_string = f"{propagator_type}: [{' '.join(map(str, lats))}];[{' '.join(map(str, lons))}]"


    return propagator_string


def wkt_to_propagator_strings(wkt_string: str):
    """
    Transform wkt string to propagator string
    :param wkt_string: wkt string
    :return: propagator string
    """
    geom = wkt.loads(wkt_string)

    return transform_features_to_propagator_strings(geom)

def transform_features_to_propagator_strings(features: FeatureCollection):
    """
    Exctract geometry type and coordinates from features
    :param features: geojson features
    :return: propagator string array
    """
    propagator_strings = []
    if features['type'] != 'FeatureCollection':
        features = [features]
    else:
        features = features['geometries']

    for feature in features:
        feature_type = feature['type']

        if feature_type == 'Polygon':
            poly = feature['coordinates']
            propagator_strings.append(get_geometry_string(poly, 'Polygon'))

        elif feature_type == 'LineString':
            line = feature['coordinates']
            propagator_strings.append(
                    get_geometry_string(line, 'LineString'))

        elif feature_type == 'MultiPolygon':
            for poly in feature['coordinates']:
                propagator_strings.append(get_geometry_string(poly, 'Polygon'))

        elif feature_type == 'MultiLineString':
            for line in feature['coordinates']:
                propagator_strings.append(
                    get_geometry_string(line, 'LineString'))

        elif feature_type == 'MultiPoint':
            for point in feature['coordinates']:
                propagator_strings.append(get_geometry_string(point, 'Point'))

        else:
            coordinates = feature['coordinates']
            propagator_strings.append(
                get_geometry_string(coordinates, feature_type))

    return propagator_strings


# %%


body = json.dumps({
  'description': 'my description',
  'start': '2023-01-02T18:51:00.000Z',
  'end': '2023-01-02T20:51:00.000Z',
  'time_limit': 2,
  'probabilityRange': 0.75,
  'do_spotting': False,
  'boundary_conditions': [
    {
      'time': 0,
      'w_dir': 276,
      'w_speed': 10,
      'moisture': 10,
      'fireBreak': {
        'canadair': 'LINESTRING (9.244365 42.445386, 9.266281 42.414662)',
        'helicopter': 'LINESTRING (9.237521 42.438990, 9.275045 42.434104)',
        'waterLine': 'LINESTRING (9.233106 42.430032, 9.265635 42.441839)',
        'vehicle': 'LINESTRING (9.255702 42.442653, 9.249080 42.430440)'
      }
    },
    {
      'time': 1,
      'w_dir': 276,
      'w_speed': 20,
      'moisture': 5,
      'fireBreak': {
        
      }
    }
  ],
  'title': 'my title',
  'geometry': {
    'type': 'Polygon',
    'coordinates': [
      [
        [
          9.271048,
          42.450671
        ],
        [
          9.227102,
          42.418248
        ],
        [
          9.306205,
          42.424734
        ],
        [
          9.271048,
          42.450671
        ]
      ]
    ]
  },
  'datatype_id': '35006'
})

parse_request_body(body)
# %%
