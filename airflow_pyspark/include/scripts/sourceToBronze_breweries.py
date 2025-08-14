import requests
import math
import time
import json
import os
import gzip
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

utc_minus_3 = timezone(timedelta(hours=-3))
start_time = datetime.now(utc_minus_3)
_year = datetime.now(utc_minus_3).strftime("%Y")
_month = datetime.now(utc_minus_3).strftime("%m")
_day = datetime.now(utc_minus_3).strftime("%d")

base_url = 'https://api.openbrewerydb.org/v1/breweries'
base_path = '/usr/local/airflow'
per_page = 200
output_file_path = f'{base_path}/include/datalake/bronze/breweries/{_year}/{_month}/{_day}/'

if not os.path.exists(output_file_path):
    os.makedirs(output_file_path)

output_file = os.path.join(output_file_path, "breweries.json.gz")

print(f"Start {os.path.basename(__file__)} | Time: {start_time}")

def fetch_page(page):
    '''
    About:
        Returns the JSON response as a list of brewery records
    Args:
        page (int): The page number to fetch (1-based indexing).
    Returns:
        list: A list of brewery records (dictionaries) from the API response or an empty list if the request fails.  
    '''

    try:
        response = requests.get(base_url, params={"per_page": per_page, "page": page})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"fetching page {page}: {e}")
        return []

meta_response = requests.get(f"{base_url}/meta")
total = int(meta_response.json().get("total", 0)) if meta_response.status_code == 200 else 0
num_pages = math.ceil(total / per_page) if total > 0 else 0

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(lambda p: (time.sleep(0.2 * p), fetch_page(p))[1], page) for page in range(1, num_pages + 1)]
    with gzip.open(output_file, "wt", encoding="utf-8") as f:  
                for future in as_completed(futures):
                    data_breweries = future.result()
                    for item in data_breweries:
                        f.write(json.dumps(item) + "\n")

end_time = datetime.now(utc_minus_3)
print(f"End {os.path.basename(__file__)} | Time: {end_time}")

total_time = end_time - start_time
print(f"Total time of execution: {total_time}")