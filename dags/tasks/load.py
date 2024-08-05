import pandas as pd
import os
from datetime import datetime

def load_to_csv(dataframe, output_path):
    """
    Loads the transformed DataFrame into a CSV file with a dynamic filename.

    :param dataframe: pd.DataFrame, the DataFrame to be saved
    :param output_path: str, the base path for the output file
    """
    # Get the current datetime in YYYY-MM-DD-HH-MM format
    current_date_time = datetime.now().strftime("%Y-%m-%d-%H-%M")

    # Construct the filename with the current date
    filename = f'weather-{current_date_time}.csv'

    # Full path for the output file
    full_output_path = os.path.join(output_path, filename)

    # Save the DataFrame to CSV
    dataframe.to_csv(full_output_path, index=False)
    print(f'Data successfully loaded to {full_output_path}')