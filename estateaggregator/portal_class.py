from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

def zillowcsv_to_portalcsv(zillow_file,portal_file):
    data = pd.read_csv(zillow_file)
    getting_fields=True
    i=0
    while getting_fields:
        try:
            property_data = get_address_data(data.iloc[i,:]['house_number'],' '.join(data.iloc[i,:]['road'].split(' ')[:-1]).strip().upper())
            getting_fields=False
        except ValueError:
            print("Address {} {} not found".format(house_number,road))
            i+=1
    portalframe = pd.DataFrame(property_data,index=[0])
    for i, (house_number, road) in enumerate(zip(data['house_number'],data['road'])):
        print("Processing data point {}".format(i))
        try:
            property_data = get_address_data(house_number,' '.join(road.split(' ')[:-1]).strip().upper())
            portalframe = portalframe.append(property_data,ignore_index=True)
            print(portalframe)
        except ValueError:
            print("Address {} {} not found".format(house_number,road))
    portalframe.to_csv(portal_file)

def extract_viewstates(soup):
    viewstate_data = {} 
    viewstate_data['__VIEWSTATEFIELDCOUNT']=int(soup.find(id='__VIEWSTATEFIELDCOUNT')['value'])
    viewstate_data['__VIEWSTATEGENERATOR']=soup.find(id='__VIEWSTATEGENERATOR')['value']
    viewstate_data['__VIEWSTATE']=soup.find(id='__VIEWSTATE')['value']
    for i in range(1,viewstate_data['__VIEWSTATEFIELDCOUNT']):
        viewstate_data['__VIEWSTATE{}'.format(i)] = soup.find(id='__VIEWSTATE{}'.format(i))['value']
    return viewstate_data

def process_data_text(label,text):
    replacement_characters = [(u'\xa0',' '),(u'\n',' '),(u'$',''),(u',',''),(' SQFT',''),(' SqFt','')]
    for char1, char2 in replacement_characters:
        text = text.replace(char1,char2)
    text = text.strip()
    label = label.replace('BasicInfo1_','').replace('lbl','')
    if label=='Address':
        text_list = text.split(' ')
        city_index = text_list.index('PITTSBURGH')
        street_name = text_list[1:city_index]
        house_number = text_list[0]
        zip_code = text_list[-1]
        return((['house_number','road','postcode'],[house_number,' '.join(street_name).title().strip(),zip_code]))
    else:
        return(([label],[text]))

def get_building_information(session,address_data,street_name):
    url = 'http://www2.county.allegheny.pa.us/RealEstate/Building.aspx'
    request_data = {'ParcelID':address_data['ParcelID'].replace('-',''),
                    'SearchNum':address_data['house_number'],
                    'pin':address_data['ParcelID'].replace('-',''),
                     'SearchStreet':street_name,
                     'SearchType': 2, 'CurrRow': 0}
    building_request = session.get(url,params=request_data)
    search_soup = BeautifulSoup(BeautifulSoup(building_request.content.decode('utf-8'),'html.parser').prettify(),'html.parser')
    data_spans = search_soup.find_all('span',re.compile('DATA',re.IGNORECASE))
    building_data = {}
    for span in data_spans:
        labels,data = process_data_text(span['id'],span.text)
        for label, data_point in zip(labels,data):
            building_data[label]=data_point
    return(building_data)


def get_address_data(street_number,street_name):
    url = 'http://www2.county.allegheny.pa.us/RealEstate/search.aspx'

    with requests.session() as s:
        s.headers['user-agent'] = 'Mozilla/5.0'
        homepage_request = s.get(url)
        viewstate_data = extract_viewstates(BeautifulSoup(homepage_request.content,'html.parser'))
        viewstate_data['radio1'] = 'Address'
        viewstate_data['txtStreetNum'] = street_number
        viewstate_data['txtStreetName'] = street_name
        viewstate_data['btnSearch'] = 'Search'
        viewstate_data['hiddenInputToUpdateATBuffer_CommonToolkitScripts'] = 1
        search_request = s.post(url,viewstate_data)
        search_soup = BeautifulSoup(search_request.content.decode('utf-8'),'html.parser')
        search_soup = BeautifulSoup(search_soup.prettify(),'html.parser')
        if not search_soup.find('div',id='pnlNoRecords')==None:
            raise ValueError('Address not found in database')
        if not search_soup.find('table',id='dgSearchResults')==None:
            search_table = search_soup.find('table',id='dgSearchResults')
            first_search_url = search_table.find('a')['href']
            search_request = s.get(first_search_url)
            search_soup = BeautifulSoup(BeautifulSoup(search_request.content.decode('utf-8'),'html.parser').prettify(),'html.parser')
        property_data = {}
        data_labels = ['BasicInfo1_lblParcelID','BasicInfo1_lblAddress','lblUse','lblNeighbor','lblRecDate','lblSalePrice','lblSaleDate','lblLot']
        for row in search_soup.find_all('span',re.compile('DATA',re.IGNORECASE)):
            if row['id'] in data_labels:
                labels, data = process_data_text(row['id'],row.text)
                for label, data_point in zip(labels,data):
                    property_data[label]=data_point

        building_data = get_building_information(s,property_data,street_name)
        #print(property_data)
        property_data.update(building_data)
        return(property_data)



