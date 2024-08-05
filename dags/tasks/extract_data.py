import pandas as pd
import requests
import logging

def extract_data(path_or_url, data_type):
    data = None
    if data_type == 'csv':
        data = pd.read_csv(path_or_url)
    elif data_type == 'api':
        data = requests.get(path_or_url)
    else: 
        raise Exception('Data type undefined')

    return pd.DataFrame(data)