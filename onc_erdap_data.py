"""
Created on Mon Nov  20 11:13:52 2023

@author: Jack Bullen

This file contains some useful functions to deal with 
http://dap.onc.uvic.ca/erddap/tabledap/index.html

Overview:

get_available_datasets()
get_dataset(dataset)

---

get_dates(soup)
get_prime_numbers(N)
prime_factorization(number)

"""

from datetime import datetime
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
import pandas as pd
import requests

def get_available_datasets():
    '''
        Returns a dictionary containing metadata on dap.onc.uvic.ca/erddap datasets
    '''
    req = requests.get("http://dap.onc.uvic.ca/erddap/tabledap/index.html?page=1&itemsPerPage=5000")
    soup = BeautifulSoup(req.content, features='html.parser')
    table_rows = [x for x in soup.find_all('tr')[4:-1]]

    scalars = {}
    mobiles = {}
    surfaces = {}
    currents = {}
    profiles = {}
    progress_bar = tqdm(total=len(table_rows)//2, desc="Processing", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}')
    for i, row in enumerate(table_rows):
        try:
            link = [x for x in row.find_all('a') if 'data' in x][0]['href']
            parameters, dates = get_parameters_and_dates(link)
            start, end = dates
            id = link.split('/')[-1].split('.')[0]
            title = str(row.find_all('td')[6]).split('<td>')[1].strip('\n').split('(')[0].split('|')[0]
            date = title.split(' ')[-2]
            dataset = {
                'title': title,
                'url': link,
                'parameters': parameters,
                'start_date': start.isoformat(),
                'end_date': end.isoformat()
            }
            if 'scalar' in link:
                scalars[id] = dataset
            elif 'currents' in link:
                currents[id] = dataset
            elif 'mobilesurface' in link:
                surfaces[id] = dataset
            elif 'mobile' in link:
                mobiles[id] = dataset
            elif 'profile' in link:
                profiles[id] = dataset
        except Exception as e:
            pass
        progress_bar.update(0.5)
    progress_bar.close()
    return {
            'scalars': scalars,
            'mobiles': mobiles,
            'surfaces': surfaces,
            'currents': currents,
            'profiles': profiles
            }

def get_dataset(dataset, start=-1, end=-1):
    '''
        Return DataFrame containing data at url, and units
    '''
    dataset_url = dataset['url']
    parameters = dataset['parameters']

    if start==-1:
        start = dataset['start_date']
    if end==-1:
        end = dataset['end_date']

    start_date = datetime.fromisoformat(start)
    end_date = datetime.fromisoformat(end)

    request_url = build_url('.'.join(dataset_url.split('.')[:-1]) + '.csv', start_date, end_date, parameters)
    req = requests.get(request_url)

    decoded_string = req.content.decode('utf-8')
    rows = decoded_string.split('\n')
    headers = rows[0]
    units = rows[1]
    data = rows[2:]

    string_io_obj = StringIO('\n'.join([headers] + data))
    df = pd.read_csv(string_io_obj)
    return df, units

def get_dates(soup):
    '''
        Return the date range for a http://dap.onc.uvic.ca/erddap/tabledap URL
    '''
    start_date_str = soup.text.split('String time_coverage_start "')[1].split('"')[0]
    end_date_str = soup.text.split('String time_coverage_end "')[1].split('"')[0]
    format_str = '%Y-%m-%dT%H:%M:%S.%fZ'
    start_date = datetime.strptime(start_date_str, format_str)
    end_date = datetime.strptime(end_date_str, format_str)
    return start_date, end_date

def get_parameters_and_dates(url):
    '''
        Return the parameters inside of the dataset at url
    '''
    req = requests.get(url)
    soup = BeautifulSoup(req.content, features='html.parser')

    start_date, end_date = get_dates(soup)

    opts = set()
    for opt in soup.find_all('table')[3].find_all('option')[1:]:
        attr = str(opt).split('<option>')[1].split('\n</option>')[0]
        if attr != '</option>' and not attr.startswith('orderBy'):
            opts.add(str(opt).split('<option>')[1].split('\n</option>')[0])
    return [x.strip('\n') for x in list(opts)], [start_date, end_date]

def build_url(base, start_date, end_date, parameters):
    '''
        Return the url to retrieve data containing parameters from base through dates
    '''
    parameters_str = "%2C".join(parameters)
    time_range_str = f"time%3E={start_date.isoformat()}Z&time%3C={end_date.isoformat()}Z"
    return f"{base}?{parameters_str}&{time_range_str}"