


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark = SparkSession.builder.appName("ETL-pipeline-using-Airflow-and-AWS-EMR-cluster").getOrCreate()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",20)


# COMMAND ----------

#cache() for some runtime optimization (even though all three data sets are small, but we can use cache to hold the small datasets in-memory for quick availability and fater runtimes)
items = spark.read.csv("s3a://olist-project/data/olist_order_items_dataset.csv",header=True,inferSchema=True).cache()
orders = spark.read.csv("s3a://olist-project/data/olist_orders_dataset.csv",header=True,inferSchema=True).cache()
products = spark.read.csv("s3a://olist-project/data/olist_products_dataset.csv",header=True,inferSchema=True)

# COMMAND ----------

#items.show(5)
#orders.printSchema()
#products.printSchema()


# COMMAND ----------

# # order/seller/product info for orders where the
# seller missed the deadline to deliver the shipment to the carrier
orders_info = items.join(orders,items.order_id == orders.order_id)\
    .join(products,items.product_id == products.product_id)\
    .select(items.order_id, items.seller_id, items.shipping_limit_date, items.price,                     items.freight_value,products.product_id,products.product_category_name,orders.customer_id, orders.order_status, orders.order_purchase_timestamp, orders.order_delivered_carrier_date,orders.order_delivered_customer_date, orders.order_estimated_delivery_date)

# COMMAND ----------

orders_info.printSchema()

# COMMAND ----------

orders_info.coalesce(3).write.csv("s3a://olist-project/output_data/missed_shipping_limit_orders/",header=True)

# COMMAND ----------


