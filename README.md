# Open Brewery DB 

### Required softwares

Python and Docker are required for this project. Below are the instructions for installing Python and Docker on Mac, Windows, and Linux. This project was developed on Windows.

- Docker:
    - Windows: https://docs.docker.com/desktop/install/windows-install/
    - Mac: https://docs.docker.com/desktop/install/mac-install/
    - Linux: https://docs.docker.com/desktop/install/linux-install/

- Python (3.12.1):
    - https://realpython.com/installing-python/


## Usage Guide

### 1 - Clone this repository

- `git clone https://github.com/Flaviohnb/open_brewery_db.git`

### 2 - Install requirements

- `pip install -r requirements.txt`

### 3 - Execute docker-compose

To execute the "Spark" and "Airflow" services on Docker, you need to be in the docker_airflow_spark folder and follow the commands below. This process is based on the following article: [Steps to install Apache airflow using docker-compose](https://medium.com/@Shamimw/steps-to-install-apache-airflow-using-docker-compose-9d663ea2e740). Our docker-compose file includes the PySpark container, so it will be deployed with Airflow.

- `cd docker_airflow_spark/`
- `echo -e "AIRFLOW_UID=$(id -u)" > .env`
- `AIRFLOW_UID=50000`
- `docker-compose up airflow-init`
- `docker-compose up &`

### 4 - Access UI

- PySpark with Jupyternotebook: http://localhost:8888/
- Airflow: http://localhost:8080/