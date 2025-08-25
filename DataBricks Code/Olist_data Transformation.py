# Databricks notebook source
print("Hello")

# COMMAND ----------

spark

# COMMAND ----------

# Variables
storage_account = "olistdatastorageacctiru"
application_id = "6aac6741-7e3c-4cf4-a72f-80e0c3dd69ab"
directory_id = "40403321-36cb-42c3-9eee-845d472e4ab0"
client_secret = "q.M8Q~TYK_NI5bToDLpmyt7ze9ZLceZt54k34a2o"  # or better, use dbutils.secrets.get()

# Spark config for ADLS Gen2 OAuth
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

customer_df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("abfss://olistdata@olistdatastorageacctiru.dfs.core.windows.net/Bronze/olist_customers_dataset.csv")

display(customer_df)

# COMMAND ----------

base_path = "abfss://olistdata@olistdatastorageacctiru.dfs.core.windows.net/Bronze/"

orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(payments_path)
reviews_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data from Pymongo

# COMMAND ----------

# importing module
from pymongo import MongoClient
import pandas as pd

hostname = "13k5ik.h.filess.io"
database = "olistnametranslatedata_silverdoor"
port = "27018"
username = "olistnametranslatedata_silverdoor"
password = "f0fed097735440fbd89fb63dff1d671fec890620"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']

mongo_data = pd.DataFrame(list(collection.find()))
mongo_data.head()

# COMMAND ----------

## # Check for nulls in critical fields
from pyspark.sql.functions import col,when ,count
customers_df.select([count(when(col(c).isNull(),1)).alias(c) for c in customers_df.columns]).show()

# COMMAND ----------

# Cache Frequently used Data for Better Performance
orders_df.cache()
customers_df.cache()
items_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Optimized Joins For Data integration

# COMMAND ----------

from pyspark.sql.functions import *
orders_items_joined_df = orders_df.join(items_df,'order_id','inner')
orders_items_products_df = orders_items_joined_df.join(products_df,'product_id','inner')
orders_items_products_sellers_df = orders_items_products_df.join(broadcast(sellers_df),'seller_id','inner')
full_orders_df = orders_items_products_sellers_df.join(customers_df,'customer_id','inner')
full_orders_df = full_orders_df.join(broadcast(geolocation_df),full_orders_df.customer_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix,'left')
full_orders_df = full_orders_df.join(broadcast(reviews_df),'order_id','left')
full_orders_df = full_orders_df.join(payments_df,'order_id','left')
full_orders_df.cache()

# COMMAND ----------

display(full_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Total Orders Per Customer

# COMMAND ----------

customer_order_count_df = full_orders_df.groupBy('customer_id')\
.agg(count('order_id').alias('total_orders'))\
.orderBy(desc('total_orders'))
         
customer_order_count_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Average Review Score Per Seller

# COMMAND ----------

seller_review_df = full_orders_df.groupBy('seller_id')\
.agg(avg('review_score').alias('avg_review_score'))\
.orderBy(desc('avg_review_score'))

seller_review_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Top 10 Most Sold Products

# COMMAND ----------

top_products_df = full_orders_df.groupBy('product_id')\
.agg(count('order_id').alias('total_sold'))\
.orderBy(desc('total_sold'))\
.limit(10)

top_products_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window Function and Ranking

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Rank Top Selling Products Per seller

# COMMAND ----------

from pyspark.sql.window import Window
window_spec = Window.partitionBy('seller_id').orderBy(desc('price'))
top_seller_products_df = full_orders_df.withColumn('rank',rank().over(window_spec)).filter(col('rank')<=5)

top_seller_products_df.select('seller_id','price','rank').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Advance Aggregation and Enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Total Revenue & Average Order Value (AOV) per Customer
# MAGIC

# COMMAND ----------

customer_spending_df = full_orders_df.groupBy('customer_id')\
.agg(
    count('order_id').alias('total_orders'),
    sum('price').alias('total_spent'),
    round(avg('price'),2).alias('AOV')
)\
.orderBy(desc('total_spent'))

customer_spending_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Seller Performance Metrics ( Revenue, Average Review, Order Count)
# MAGIC

# COMMAND ----------

seller_performance_df = full_orders_df.groupBy('seller_id') \
.agg(
    count('order_id').alias('total_orders'),
    sum('price').alias('total_revenue'),
    round(avg('review_score'),2).alias('avg_review_score'),
    round(stddev('price'),2).alias('price_variability')
)\
.orderBy(desc('total_revenue'))

seller_performance_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Adding Column Order day is weekend/weekday ?

# COMMAND ----------

full_orders_df = full_orders_df.withColumn('order_day_type',\
                                           when(dayofweek('order_purchase_timestamp').isin(1,7),lit('Weekend')).otherwise(lit('weekday')))
full_orders_df.select('order_purchase_timestamp','order_day_type').show(5)                                           

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Order Status Flags

# COMMAND ----------

full_orders_df = full_orders_df.withColumn('is_delivered', when(col('order_status')== 'delivered',lit(1)).otherwise(lit(0)))\
.withColumn('is_canceled', when(col('order_status')== 'canceled',lit(1)).otherwise(lit(0)))

full_orders_df.where(full_orders_df['order_status']=='canceled').select('order_status','is_delivered','is_canceled').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Product Popularity Metrics
# MAGIC

# COMMAND ----------

product_metrics_df = full_orders_df.groupBy('product_id')\
.agg(
    count('order_id').alias('total_sales'),
    sum('price').alias('total_revenue'),
    round(avg('price'),2).alias('avg_price'),
    round(stddev('price'),2).alias('price_volatility'),\
    collect_set('seller_id').alias('unique_sellers')    
)\
.orderBy(desc('total_sales'))

product_metrics_df.show(5)

# COMMAND ----------

full_orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join full_orders_df with MongoDB category names

# COMMAND ----------

mongo_data = mongo_data.drop(columns=["_id"])

# COMMAND ----------

mongo_data = mongo_data.drop(columns=["_id"], errors="ignore")
mongo_data = mongo_data.drop_duplicates(subset=["product_category_name"])
mongo_spark_df = spark.createDataFrame(mongo_data)

# COMMAND ----------

display(mongo_spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### # 💾 Write the enriched DataFrame to ADLS Gen2 in Parquet format (Silver Layer)
# MAGIC

# COMMAND ----------

full_orders_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageacctiru.dfs.core.windows.net/Silver")

# COMMAND ----------

