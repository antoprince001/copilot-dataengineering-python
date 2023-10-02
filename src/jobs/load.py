import sqlite3


def write_to_sqlite(df, db_path, table_name):
    con = sqlite3.connect(db_path)
    pandas_df = df.toPandas()
    pandas_df.to_sql(table_name, con, if_exists="replace")
    con.close()


def load(spark, staging_path, db_path, table_name):
    df = spark.read.parquet(staging_path)
    write_to_sqlite(df, db_path, table_name)
    return df
