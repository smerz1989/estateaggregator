import numpy as np
import requests
import geopandas
from shapely.geometry import shape, Point
import pandas as pd

def get_neighborhoods(city,state):
    apiurl = 'https://data.opendatasoft.com/api/records/1.0/search/'
    response = requests.get(apiurl,
                           params={'dataset': 'zillow-neighborhoods@public',
			           'rows': '100',
				   'facet': 'state',
				   'facet': 'county',
				   'facet': 'city',
				   'facet': 'name',
				   'refine.state': state,
				   'refine.state': city})
    records_data = {k: [dic[k] for dic in records] for k in records[0]}
    return(pd.DataFrame({k: [dic[k] for dic in records_data['fields']] for k in records_data['fields'][0]}))

def get_random_point_in_shape(polygon):
    min_x, min_y, max_x, max_y = polygon.bounds
    is_inside=False
    while not is_inside:
        point = Point(np.random.uniform(min_x,max_x),np.random.uniform(min_y,max_y))
        is_inside = polygon.contains(point)
    return(point)
