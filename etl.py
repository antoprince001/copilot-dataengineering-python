from pyspark.sql import SparkSession
from src.jobs.extract import read_csv_file
from src.jobs.transform import transform
from src.jobs.load import load

APP_NAME = 'ECommerce ETL Pipeline'
input_path = 'data_source/raw/ecommerce_data.csv'
staging_path = 'data_source/staging/ecommerce'
db_path = 'data_source/processed/ecommerce.db'
table_name = 'ecommerce'

if __name__ == '__main__':
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext

    # Extract raw data
    df = read_csv_file(spark, input_path)

    # Transform data and store in staging
    df = transform(spark, staging_path, df)
    df.show()

    print(df.count())

    # Load data to processed database
    df = load(spark, staging_path, db_path, table_name)

    spark.stop()
