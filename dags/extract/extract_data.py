import pandas as pd
import requests

def extract_data(path_or_url, data_type):
    data = None
    if data_type == 'csv':
        data = pd.read_csv('path')
    elif data_type == 'api':
        data = requests.get(path_or_url)
    else: print('Data type undefined')

    return pd.DataFrame(data)