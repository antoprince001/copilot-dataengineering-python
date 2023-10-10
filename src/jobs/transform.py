def transform(spark, staging_path, df):
    # Add transformations here
    df.write.parquet(staging_path, mode='overwrite')
    return df
