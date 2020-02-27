import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import geopandas
from shapely.geometry import shape, Point
import pandas as pd
import os
import xml.etree.ElementTree as ET
from ratelimit import limits, sleep_and_retry
from geopy import distance
from scipy.spatial.distance import cdist
from exceptions import NotInZillowDatabase 
import json

zwsid = os.environ['ZWSID']

def sample_city(city,state,num_samples_per_neighborhood,exclude_neighbors=[]):
    neighborhoods = get_neighborhoods(city,state)
    for i,neighborhood in neighborhoods.iterrows():
        print("Analyzing Neighborhood {}:\n\n".format(neighborhood['name']))
        if os.path.exists(neighborhood['name']+'_data.csv') or neighborhood['name'] in exclude_neighbors:
            pass
        else:
            sample_neighborhood(neighborhood,num_samples_per_neighborhood,save_as_csv=True)

def sample_neighborhood(neighborhood,num_samples,save_as_csv=False,max_tries=20,fields=['latitude','longitude',
                                                           'lastSoldDate','lastSoldPrice',
                                                           'yearBuilt','taxAssessment',
                                                           'taxAssessmentYear','amount',
                                                           'low','high',
                                                           'last-updated','bathrooms',
                                                           'bedrooms','finishedSqFt',
                                                           'lotSizeSqFt','useCode']):
    housing_df = pd.DataFrame(columns=fields.extend(['house_number','road','city','state','postcode']))
    avoid_points = []
    address=None
    while address==None:
        try:
            address,house_data = get_random_house_data(neighborhood,fields,avoid_points)
            avoid_points.append([float(house_data['longitude']),float(house_data['latitude'])])
        except NotInZillowDatabase as e:
            print(e)
            #print("Adding address to avoid points to avoid sampling in future")
            try:
                address_coords = get_gps_by_address(e.address)
                print("Adding longitude {} and latitude {}".format(address_coords[1],address_coords[0]))
                avoid_points.append([float(address_coords[1]),float(address_coords[0])])
            except OSError as e:
                print("Could not connect to Nomatim server")
            except IndexError:
                print("No address found skipping adding to avoid points") 
    for i in range(num_samples):
        missed_calls=0
        while (not housing_df.empty) and  (housing_df[housing_df['house_number']==address['house_number']].shape[0]>0) and (housing_df[housing_df['road']==address['road']].shape[0]>0):
            print("Address already in database getting new address")
            try:
                address,house_data = get_random_house_data(neighborhood,fields,avoid_points)  
            except NotInZillowDatabase as e:
                print(e)
                #print("Adding address to avoid points to avoid sampling in future")
                try:
                    address_coords = get_gps_by_address(e.address)
                    print("Adding longitude {} and latitude {}".format(address_coords[1],address_coords[0]))
                    avoid_points.append([float(address_coords[1]),float(address_coords[0])])
                except OSError as e:
                    print("Could not connect to Nomatim server")
                except IndexError:
                    print("Address not found skipping adding address to avoid points")
            missed_calls+=1
            print("{} missed calls since last success".format(missed_calls))
            if missed_calls>max_tries:
               if save_as_csv:
                   housing_df.to_csv(neighborhood['name']+'_data.csv')
               return(housing_df)
        house_data['house_number'] = address['house_number']
        house_data['road'] = address['road']
        house_data['city'] = address['city']
        house_data['state'] = address['state']
        house_data['postcode']= address['postcode']
        print('Processing data point {}. for address {} {}, zipcode:{}'.format(i,address['house_number'],address['road'],address['postcode']))
        housing_df = housing_df.append(house_data,ignore_index=True)
    if save_as_csv:
        housing_df.to_csv(neighborhood['name']+'_data.csv')
    return(housing_df)        

def get_random_house_data(neighborhood,fields,avoid_points):
    found_address_data=False
    while not found_address_data:
        address = get_random_address_in_neighborhood(neighborhood,avoid_points)
        zillow_search_request = requests.get('http://www.zillow.com/webservice/GetDeepSearchResults.htm',
                                             params={'zws-id': zwsid,
                                                    'address': address['house_number']+' '+address['road'],
                                               'citystatezip': 'Pittsburgh, PA'})
        if zillow_search_request.status_code == 200:
            zillow_search_tree = ET.ElementTree(ET.fromstring(zillow_search_request.content.decode('utf-8'))) 
            search_status = int([code.text for code in zillow_search_tree.find('message').iter('code')][0])
        else: 
            print("Zillow is not responding (returning status code {}) trying again".format(zillow_search_request.status_code))
            continue
        if search_status==0:
            found_address_data=True
        else:
            raise NotInZillowDatabase(address)
    house_data = {}
    for field in fields:
        field_entry = [entry.text for entry in zillow_search_tree.iter(field)]
        if len(field_entry)>0:
            house_data[field] = field_entry[0]
        else:
            house_data[field] = "NA"
    return((address,house_data))

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
                   'refine.city': city})
    records = response.json()['records']
    records_data = {k: [dic[k] for dic in records] for k in records[0]}
    neighborhoods_df = pd.DataFrame({k: [dic[k] for dic in records_data['fields']] for k in records_data['fields'][0]})
    neighborhoods_df['geo_shape']=neighborhoods_df['geo_shape'].apply(shape)
    neighborhoods_df['geo_point_2d']=neighborhoods_df['geo_point_2d'].apply(lambda x: Point(x[1],x[0]))
    return(neighborhoods_df)

@sleep_and_retry
@limits(calls=3,period=4)
def get_gps_by_address(address):
    try:
        address_request = requests_session().get('https://nominatim.openstreetmap.org/search',
                                   params={'format': 'json',
                                           'street': address['house_number']+' '+address['road'],
                                           'city': address['city'],
                                           'state': address['state'],
                                           'postalcode': address['postcode'],
                                           'addressdetails': '1'})
        coords_json = address_request.json()[0]
        return(coords_json['lat'],coords_json['lon'])
    except OSError as e:
        raise OSError("Could not connect to Nomatim server")



@sleep_and_retry
@limits(calls=3,period=4)
def get_address_by_gps(latitude,longitude):
    try:
        address_request = requests_session().get('https://nominatim.openstreetmap.org/reverse',
                                   params={'format': 'json',
                                           'lat': latitude,
                                           'lon': longitude,
                                           'zoom': '18',
                                           'addressdetails': '1'})
        return(address_request.json()['address'])
    except OSError as e:
        raise OSError("Could not connect to Nomatim server")
    except json.decoder.JSONDecodeError as e:
        print("JSON Decoder Error, request content {}".format(address_request.content))

def get_random_address_in_neighborhood(neighborhood,avoid_points):
    random_point = get_random_point_in_shape(neighborhood['geo_shape'],avoid_points).coords[0]
    is_address_valid=False
    while not is_address_valid:
        try:
            address = get_address_by_gps(random_point[1],random_point[0])
            if ('house_number' in address.keys()) and ('road' in address.keys()) and ('city' in address.keys()) and ('postcode' in address.keys()):
                is_address_valid=True
            else:
                random_point = get_random_point_in_shape(neighborhood['geo_shape']).coords[0]
        except OSError:
            print("Could not connect to Nomatim server for current address, trying again with different coords")
    return(address)

def requests_session(retries=3,backoff_factor=0.5,status_forcelist=(500,502,504),session=None):
    session = session or requests.Session()
    retry_obj = Retry(total=retries,
                      read=retries,
                      connect=retries,
                      backoff_factor=backoff_factor,
                      status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry_obj)
    session.mount('http://',adapter)
    session.mount('https://',adapter)
    return session

def get_random_point_in_shape(polygon,avoid_points=[],critical_distance=0.02):
    min_x, min_y, max_x, max_y = polygon.bounds
    is_inside=False
    is_already_sampled=True
    avoid_points = np.array(avoid_points)
    number_points=avoid_points.shape[0]
    while not is_inside or is_already_sampled:
        point = Point(np.random.uniform(min_x,max_x),np.random.uniform(min_y,max_y))
        is_inside = polygon.contains(point)
        if number_points>0:
            min_dist=cdist([point.coords[0]],avoid_points,lambda u, v: distance.distance(u,v).km).min()
            is_already_sampled=min_dist<critical_distance
        else:
            is_already_sampled=False
    return(point)
