import pandas as pd
from datetime import datetime, timedelta, timezone

utc_minus_3 = timezone(timedelta(hours=-3))
_year = datetime.now(utc_minus_3).strftime("%Y")
_month = datetime.now(utc_minus_3).strftime("%m")
_day = datetime.now(utc_minus_3).strftime("%d")

base_path = '/usr/local/airflow'

file_path = f'{base_path}/datalake/bronze/breweries/{_year}/{_month}/{_day}/breweries.json.gz'

df = pd.read_json(file_path, lines=True)
df_cols = df.columns.tolist()

default_cols = ['id', 'name', 'brewery_type', 'website_url', 'phone', 'postal_code', 'state_province', 'country', 'state', 'city', 'street', 'longitude', 'latitude', 'address_1', 'address_2', 'address_3']

def check_missing_columns(df_cols, default_cols):
    '''
    About:
        Compares the column list of a DataFrame with a standard column list and returns messages about differences found.
    Args:
        df_cols (list): List of dataframe columns.
        default_cols (list): List of default columns.
    Returns:
        (str): Message with test result.
    '''

    set_df = set(df_cols)
    set_default = set(default_cols)
    len(set_df.difference(set_default))
    len(set_default.difference(set_df))
    
    if sorted(df_cols) == sorted(default_cols):
        print('No missing columns')
        return 'No missing columns'
    elif len(set_df.difference(set_default)) > 0:
        print(f'There are more columns in the bronze layer: {set_df.difference(set_default)}')
        return f'There are more columns in the bronze layer: {set_df.difference(set_default)}'
    elif len(set_default.difference(set_df)) > 0:
        print(f'Columns are missing in the bronze layer: {set_default.difference(set_df)}')
        return f'Columns are missing in the bronze layer: {set_default.difference(set_df)}'        
    
def check_null_values(df, col):
    '''
    About:
        Check if there is any null values by column.
    Args:
        df (Pandas Dataframe): Dataframe.
        col (string): Column name.
    Returns:
        (str): Message with test result.
    '''
    if df[col].isnull().any():
        null_count = df[col].isnull().sum()        
        print(f"Column '{col}' has {null_count} null values.")
        return f"Column '{col}' has {null_count} null values."
    else:
        print(f"Column '{col}' has no null values.")
        return f"Column '{col}' has no null values."

def check_duplicate_values(df, col):
    '''
    About:
        Check if there is any duplicated values by column.
    Args:
        df (Pandas Dataframe): Dataframe.
        col (string): Column name.
    Returns:
        (str): Message with test result.
    '''
    if df[col].duplicated().any():
        duplicated_count = df[col].duplicated().sum()        
        print(f"Column '{col}' has {duplicated_count} duplicated values.")
        return f"Column '{col}' has {duplicated_count} duplicated values."
    else:
        print(f"Column '{col}' has no duplicated values.")
        return f"Column '{col}' has no duplicated values."

check_missing_columns(df_cols, default_cols)
check_null_values(df, 'id')
check_duplicate_values(df, 'country')