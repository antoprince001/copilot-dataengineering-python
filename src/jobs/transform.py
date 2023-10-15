# function to divide aging column into  discrete time intervals representing shipping speed categories (e.g., express, standard, delayed)

# import pyspark when
from pyspark.sql.functions import when, col

def transform_aging_column(df):
    df = df.withColumn("shipping_speed", when(col("aging") <= 3, "express").when(col("aging") <= 7, "standard").otherwise("delayed"))
    return df

# Apply a transformation to categorize customers as high-value, medium-value, and low-value based on  profit which is between 0.5 and 168.0.
def transform_profit_column(df):
    df = df.withColumn("customer_value", when(col("profit") <= 50, "low").when(col("profit") <= 100.0, "medium").otherwise("high"))
    return df

# transformation to calculate the cumulative discount for each customer's order sequence.
def transform_cumulative_discount(df):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import sum, col
    window = Window.partitionBy("customer_id").orderBy("order_date")
    df = df.withColumn("cumulative_discount", sum(col("discount")).over(window))
    return df

# Develop a formula to dynamically calculate shipping costs based on the combination of product category, order priority, and customer location. 
# The following code snippet shows how to apply the formula to the DataFrame:
from pyspark.sql.functions import when, col
def transform_shipping_cost(df):
    df = df.withColumn("shipping_cost", when((col("product_category") == "electronics") & (col("order_priority") == "high"), 0.1 * col("order_total")) \
    .when((col("product_category") == "electronics") & (col("order_priority") == "medium"), 0.07 * col("order_total")) \
    .when((col("product_category") == "electronics") & (col("order_priority") == "low"), 0.05 * col("order_total")) \
    .when((col("product_category") == "clothing") & (col("order_priority") == "high"), 0.08 * col("order_total")) \
    .when((col("product_category") == "clothing") & (col("order_priority") == "medium"), 0.06 * col("order_total")) \
    .when((col("product_category") == "clothing") & (col("order_priority") == "low"), 0.04 * col("order_total")) \
    .when((col("product_category") == "groceries") & (col("order_priority") == "high"), 0.05 * col("order_total")) \
    .when((col("product_category") == "groceries") & (col("order_priority") == "medium"), 0.03 * col("order_total")) \
    .when((col("product_category") == "groceries") & (col("order_priority") == "low"), 0.01 * col("order_total")) \
    .when((col("product_category") == "cosmetics") & (col("order_priority") == "high"), 0.07 * col("order_total")) \
    .when((col("product_category") == "cosmetics") & (col("order_priority") == "medium"), 0.05 * col("order_total")) \
    .when((col("product_category") == "cosmetics") & (col("order_priority") == "low"), 0.03 * col("order_total")) \
    .when((col("product_category") == "auto parts") & (col("order_priority") == "high"), 0.09 * col("order_total")) \
    .when((col("product_category") == "auto parts") & (col("order_priority") == "medium"),
            0.07 * col("order_total")) \
    .when((col("product_category") == "auto parts") & (col("order_priority") == "low"), 0.05 * col("order_total")) \
    .when((col("product_category") == "furniture") & (col("order_priority") == "high"), 0.06 * col("order_total")) \
    .when((col("product_category") == "furniture") & (col("order_priority") == "medium"), 0.04 * col("order_total")) \
    .when((col("product_category") == "furniture") & (col("order_priority") == "low"), 0.02 * col("order_total")) \
    .when((col("product_category") == "office supplies") & (col("order_priority") == "high"), 0.04 * col("order_total")) \
    .when((col("product_category") == "office supplies") & (col("order_priority") == "medium"), 0.02 * col("order_total")) \
    .when((col("product_category") == "office supplies") & (col("order_priority") == "low"), 0.01 * col("order_total")) \
    .when((col("product_category") == "pharmaceuticals") & (col("order_priority") == "high"), 0.03 * col("order_total")) \
    .when((col("product_category") == "pharmaceuticals") & (col("order_priority") == "medium"), 0.01 * col("order_total")) \
    .when((col("product_category") == "pharmaceuticals") & (col("order_priority") == "low"), 0.005 * col("order_total")) \
    .when((col("product_category") == "snacks") & (col("order_priority") == "high"), 0.02 * col("order_total")) \
    .when((col("product_category") == "snacks") & (col("order_priority") == "medium"), 0.01 * col("order_total")) \
    .when((col("product_category") == "snacks") & (col("order_priority") == "low"), 0.005 * col("order_total")) \
    .when((col("product_category") == "vegetables") & (col("order_priority") == "high"), 0.01 * col("order_total")) \
    .when((col("product_category") == "vegetables") & (col("order_priority") == "medium"), 0.005 * col("order_total")) \
    .when((col("product_category") == "vegetables") & (col("order_priority") == "low"), 0.0025 * col("order_total")) \
    .otherwise(0))
    return df

# optimize the above function to be in lower lines
def transform_shipping_cost_2(df):
    from pyspark.sql.functions import when, col
    df = df.withColumn("shipping_cost", when(col("order_priority") == "high", 0.1 * col("order_total")) \
    .when(col("order_priority") == "medium", 0.07 * col("order_total")) \
    .when(col("order_priority") == "low", 0.05 * col("order_total")))
    df = df.withColumn("shipping_cost", when(col("product_category") == "electronics", col("shipping_cost")) \
    .when(col("product_category") == "clothing", 0.08 * col("order_total")) \
    .when(col("product_category") == "groceries", 0.05 * col("order_total")) \
    .when(col("product_category") == "cosmetics", 0.07 * col("order_total")) \
    .when(col("product_category") == "auto parts", 0.09 * col("order_total")) \
    .when(col("product_category") == "furniture", 0.06 * col("order_total")) \
    .when(col("product_category") == "office supplies", 0.04 * col("order_total")) \
    .when(col("product_category") == "pharmaceuticals", 0.03 * col("order_total")) \
    .when(col("product_category") == "snacks", 0.02 * col("order_total")) \
    .when(col("product_category") == "vegetables", 0.01 * col("order_total")) \
    .otherwise(0))
    return df

# Apply a transformation to identify the top three product categories preferred by each gender. 

def transform_top3_product_categories(df):
    pass

# Implement a time-series transformation to forecast sales for each product category, considering the historical data of sales on different devices
def transform_sales_forecast(df):
    df = df.withColumn("sales_forecast", when(col("device_type") == "desktop", 1.1 * col("sales")) \
    .when(col("device_type") == "mobile", 1.2 * col("sales")) \
    .when(col("device_type") == "tablet", 1.3 * col("sales")) \
    .otherwise(0))
    return df

# Devise a formula to calculate a customer engagement score based on the frequency of logins, order priorities, and total sales
def transform_customer_engagement(df):
    df = df.withColumn("customer_engagement", when(col("order_priority") == "high", 1.1 * col("sales")) \
    .when(col("order_priority") == "medium", 1.05 * col("sales")) \
    .when(col("order_priority") == "low", 1.01 * col("sales")) \
    .otherwise(0))
    return df


# Apply a transformation to categorize payment methods into high, medium, and low sensitivity groups
def transform_payment_sensitivity(df):
    df = df.withColumn("payment_sensitivity", when(col("payment_method") == "credit card", "high") \
    .when(col("payment_method") == "cash on delivery", "medium") \
    .when(col("payment_method") == "e-wallet", "low") \
    .otherwise(0))
    return df

    
def transform(spark, staging_path, df):
    # Add transformations here
    df = transform_aging_column(df)
    df = transform_profit_column(df)
    df = transform_cumulative_discount(df)
    df.write.parquet(staging_path, mode='overwrite')
    return df

