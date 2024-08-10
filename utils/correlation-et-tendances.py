# Importation des bibliothèques nécessaires
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

def get_date(filename):
    return datetime.strptime(filename, 'weather-%Y-%m-%d-T-%H-%M.csv').date()

#print(get_date('weather-2024-08-10-T-06-09.csv'))

OUTPUT_DATA_PATH = f'{os.getenv('AIRFLOW_HOME')}/data/output'
def extract_all_out_data(output_data_path):
    all_data_file = os.listdir(output_data_path)
    all_data = pd.DataFrame()

    for file in all_data_file:
        data = pd.DataFrame(pd.read_csv(os.path.join(output_data_path, file)))
        data['date'] = get_date(file)

        if all_data.size == 0:
            all_data = data
        else:
            all_data = all_data._append(data, ignore_index=True)

    return all_data.sort_values(by='date', ascending=True, ignore_index=True)

APPENDED_DATA_PATH = f'{os.getenv('AIRFLOW_HOME')}/data/appended'
def save_appended_data(data, appended_data_path):
    if not os.path.isdir(appended_data_path):
        os.mkdir(appended_data_path)

    data.to_csv(os.path.join(appended_data_path, 'all_data.csv'), index=False)

save_appended_data(extract_all_out_data(OUTPUT_DATA_PATH), APPENDED_DATA_PATH)

def extract_appended_data(file_path):
    return pd.read_csv(file_path)

#print(extract_appended_data(os.path.join(APPENDED_DATA_PATH, 'all_data.csv')))

def sync_s3_bucket_to_local(bucket_name, local_directory):
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Create local directory if it does not exist
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # List all objects in the S3 bucket
    bucket_objects = s3_client.list_objects_v2(Bucket=bucket_name)

    if 'Contents' in bucket_objects:
        for obj in bucket_objects['Contents']:
            key = obj['Key']
            local_file_path = os.path.join(local_directory, key)

            # Check if directory, if so, create directory
            if '/' in key:
                directory = os.path.dirname(local_file_path)
                if not os.path.exists(directory):
                    os.makedirs(directory)
                if key.endswith('/'):
                    # It's a directory, no need to download
                    continue

            # Download file from S3 to local directory
            print(f"Downloading {key} to {local_file_path}")
            s3_client.download_file(bucket_name, key, local_file_path)

    else:
        print("No objects found in the bucket.")

#sync_s3_bucket_to_local(os.getenv('BUCKET_NAME'), OUTPUT_DATA_PATH)