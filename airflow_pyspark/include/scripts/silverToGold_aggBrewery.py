import pyspark.sql.functions as F
from pyspark.sql import SparkSession

base_path = '/usr/local/airflow'

spark = (
    SparkSession
    .builder
    .appName("Silver To Gold - Breweries")
    .getOrCreate()
)

sc = spark.sparkContext

df_silverBreweries = (
    spark
    .read
    .option("basePath", f"{base_path}/datalake/silver/breweries/")
    .parquet(f"{base_path}/datalake/silver/breweries/*")
)

df_aggBreweries = (
    df_silverBreweries
    .groupBy(
        F.col('brewery_type'),
        F.col('country')
    )
    .count()
    .orderBy(
        F.col('count').desc(),
        F.col('brewery_type').desc(),
        F.col('country').desc()
    )
)

df_aggBreweries.show(n=5, truncate=False)

(
    df_aggBreweries
    .coalesce(1)
    .write    
    .mode('overwrite')
    .parquet(f"{base_path}/datalake/gold/aggBreweries/")
)