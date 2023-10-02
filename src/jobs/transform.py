def transform(spark, staging_path, df):
    df.write.parquet(staging_path, mode='overwrite')
    return df
