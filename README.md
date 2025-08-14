# Open Brewery DB 

### Required softwares

Python, Docker and Astro CLI are required for this project. This project was developed on Windows and based on this tutorial "[How to run PySpark with Apache Airflow: The new way](https://www.youtube.com/watch?v=L3VuPnBQBCM)". In addition to the services offered by this tutorial, we are running an image with *Jupyter Notebook* for developing some PySpark scripts.

- Docker:
    - Windows: https://docs.docker.com/desktop/install/windows-install/
    - Mac: https://docs.docker.com/desktop/install/mac-install/
    - Linux: https://docs.docker.com/desktop/install/linux-install/

- Python (3.12.1):
    - https://realpython.com/installing-python/

- Astro CLI:
    - https://www.astronomer.io/docs/astro/cli/install-cli/


## Usage Guide

### 1 - Clone this repository

- `git clone https://github.com/Flaviohnb/open_brewery_db.git`

### 2 - Deploy

After the repository clone is done, only the command is needed:

- `astro dev start`

If everything goes as planned, your Dockerfile and services in Docker-Compose will run. After that, you'll be able to access the interface.

### 3 - Access UI

- *PySpark with Jupyternotebook* 
    1. Access: http://localhost:8888/;
    2. Run the command `docker exec -it jupyternotebook_dev jupyter server list`, to get the token;
    3. Copy the info after `/?token=` and fill (password or token) on Jupyter UI;
    
- *Airflow*
    1. Access: http://localhost:8080/;
    2. User: admin, Password: admin;