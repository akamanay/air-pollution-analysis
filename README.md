# Air Pollution Analysis

This repository contains our project focused on collecting and analyzing air pollution data from various cities around the world. We utilize the OpenWeatherMap Air Pollution API to track data on several polluting gases, including:

- Carbon monoxide (CO)
- Nitrogen monoxide (NO)
- Nitrogen dioxide (NO2)
- Ozone (O3)
- Sulphur dioxide (SO2)
- Ammonia (NH3)
- Particulate matter (PM2.5 and PM10)
In addition, we analyze Air Quality Index (AQI) levels for these locations.
## Clone the repository 
First, clone this repository to your local machine:
```sh
git clone https://github.com/your-username/air-pollution-analysis.git
cd air-pollution-analysis
```
## Install Required Software
Ensure you have Python and Apache Airflow installed on your machine. To install the Apache airflow, go to the next link: [apache airflow installation](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

## Configure Environment Variables
You need to set up several environment variables to run this project. You can find a template for these variables in the `.env.template` file. Copy this file to create your own `.env` file and replace the placeholder values with your own or set the env var on the system.
 - **API key**: Your API key from [OpenWeatherMap](https://openweathermap.org).You need to create an account to obtain an API key.
 - **AIRFLOW_HOME**: The path to the cloned repository. This sets the working directory for Airflow.
 - **AWS Credentials and Bucket info** to store the data on S3

 ## Project structure
- **dags/**: Contains the Airflow DAGs for the ETL pipeline.
- **notebooks/**: Includes Jupyter notebooks for data analysis and visualization.
- **.env.template**: A template for setting up the necessary environment variables.
- **tasks/**: Python scripts for extracting, transforming, and loading data.
- **requirements.txt**: Dependecies for the project
