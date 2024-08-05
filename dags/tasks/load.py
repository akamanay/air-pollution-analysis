import pandas as pd
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import logging

def upload_file(file_name, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    BUCKET_NAME = os.getenv('BUCKET_NAME')

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, BUCKET_NAME, object_name)

        print(f'---> File {object_name} uploaded successfully')
        return True
    except ClientError as e:
        logging.error(f'An error occured while uploading the file {object_name} on S3:\n   >>> {e.__cause__}')
        return False

def create_file(dataframe, full_output_path):
    """Cteate the file on local

    :param dataframe: pd.DataFrame, the DataFrame to be saved
    :param output_path: str, the base path for the output file
    :return: True if file was created, else False
    """

    parent_dir = os.path.dirname(full_output_path)
    if not os.path.isdir(parent_dir):
        os.mkdir(parent_dir)

    try:
        # Save the DataFrame to CSV
        dataframe.to_csv(full_output_path, index=False)
        print(f'---> File {full_output_path} created successfully')

        return True
    except Exception as e:
        logging.error(f'An error occured while saving the file {filename}:\n   >>> {e.__cause__}')
        return False
        
def load_to_csv(dataframe, output_path):
    """
    Loads the transformed DataFrame into a CSV file with a dynamic filename.

    :param dataframe: pd.DataFrame, the DataFrame to be saved
    :param output_path: str, the base path for the output file
    """

    # Get the current datetime in YYYY-MM-DD-HH-MM format
    current_date_time = datetime.now().strftime("%Y-%m-%d-T-%H-%M")

    # Construct the filename with the current date
    filename = f'weather-{current_date_time}.csv'

    # Full path for the output file
    full_output_path = os.path.join(output_path, filename)

    if create_file(dataframe, full_output_path) == False:
        raise Exception('Creating file failed, see more in logs...')
    if upload_file(full_output_path, filename) == False:
        raise Exception('Uploading file failed, see more in logs...')
