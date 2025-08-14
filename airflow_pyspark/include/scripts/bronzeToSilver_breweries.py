import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone

spark = (
    SparkSession
    .builder
    .appName("Bronze To Silver - Breweries")
    .getOrCreate()
)

sc = spark.sparkContext

utc_minus_3 = timezone(timedelta(hours=-3))
_year = datetime.now(utc_minus_3).strftime("%Y")
_month = datetime.now(utc_minus_3).strftime("%m")
_day = datetime.now(utc_minus_3).strftime("%d")

base_path = '/usr/local/airflow'

df_bronzeBreweries = spark.read.option("multiLine", False).json(f'{base_path}/include/datalake/bronze/breweries/{_year}/{_month}/{_day}/breweries.json.gz')

df_silverBreweries = (
    df_bronzeBreweries
    .select(
        F.col('id').cast('string'),
        F.col('name').cast('string'),
        F.col('brewery_type').cast('string'),
        F.col('website_url').cast('string'),
        F.col('phone').cast('string'),
        F.col('postal_code').cast('string'),
        F.col('state_province').cast('string'),
        F.col('country').cast('string'),
        F.col('state').cast('string'),
        F.col('city').cast('string'),
        F.col('street').cast('string'),
        F.col('longitude').cast('string'),
        F.col('latitude').cast('string'),
        F.col('address_1').cast('string'),
        F.col('address_2').cast('string'),
        F.col('address_3').cast('string')
    )    
)

(
    df_silverBreweries
    .coalesce(1)
    .write
    .partitionBy("country")
    .mode('overwrite')
    .parquet(f"{base_path}/datalake/silver/breweries/")
)