# Databricks notebook source
# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os, shutil
import uuid
import pandas as pd
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Logging**

# COMMAND ----------

# DBTITLE 1,Creating zomato_summary_log logging Table
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.infocepts.zomato_summary_log (
# MAGIC   `job_id` string,
# MAGIC   `job_step` string,
# MAGIC   `job_start_time` timestamp,
# MAGIC   `job_end_time` timestamp,
# MAGIC   `job_status` string,
# MAGIC   `note` string
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Inserting Logging Data
def logging(step, start_time, status, note):
    job_id = str(uuid.uuid4())
    end_time = datetime.now()

    spark.sql(f"""
    INSERT INTO workspace.infocepts.zomato_summary_log(job_id, job_step, job_start_time, job_end_time, job_status, note)
    VALUES ('{job_id}', '{step}', '{start_time}', '{end_time}', '{status}', '{note}')          
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC **Data**

# COMMAND ----------

# DBTITLE 1,Data Path
base_dir = os.getcwd()
raw_files = os.path.join(base_dir, "zomato_raw_files/")
source_files = os.path.join(base_dir, "zomato_etl/source/json/")
target_files = os.path.join(base_dir, "zomato_etl/source/csv/")

os.makedirs(source_files, exist_ok=True)
os.makedirs(target_files, exist_ok=True)

for file in os.listdir(raw_files):
    if file.endswith(".json"):
        shutil.move(os.path.join(raw_files, file), os.path.join(source_files, file))

# COMMAND ----------

# DBTITLE 1,File Mapping
file_mapping = {
    "file1.json": "20190609",
    "file2.json": "20190610",
    "file3.json": "20190611",
    "file4.json": "20190612",
    "file5.json": "20190613"
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Table Creation**

# COMMAND ----------

# DBTITLE 1,Table Creation
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.infocepts;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.infocepts.zomato (
# MAGIC   `Restaurant ID` string,
# MAGIC   `Restaurant Name` string,
# MAGIC   `Country Code` string,
# MAGIC   `City` string,
# MAGIC   `Address` string,
# MAGIC   `Locality` string,
# MAGIC   `Locality Verbose` string,
# MAGIC   `Longitude` string,
# MAGIC   `Latitude` string,
# MAGIC   `Cuisines` string,
# MAGIC   `Average Cost for two` string,
# MAGIC   `Currency` string,
# MAGIC   `Has Table booking` string,
# MAGIC   `Has Online delivery` string,
# MAGIC   `Is delivering now` string,
# MAGIC   `Switch to order menu` string,
# MAGIC   `Price range` string,
# MAGIC   `Aggregate rating` string,
# MAGIC   `Rating text` string,
# MAGIC   `Votes` string,
# MAGIC   `filedate` string
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (`filedate`)
# MAGIC TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.infocepts.dim_country (
# MAGIC   `Country Code` int,
# MAGIC   `Country` string
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.infocepts.zomato_summary (
# MAGIC   `Restaurant ID` bigint,
# MAGIC   `Restaurant Name` string,
# MAGIC   `Country Code` int,
# MAGIC   `City` string,
# MAGIC   `Address` string,
# MAGIC   `Locality` string,
# MAGIC   `Locality Verbose` string,
# MAGIC   `Longitude` double,
# MAGIC   `Latitude` double,
# MAGIC   `Cuisines` string,
# MAGIC   `Average Cost for two` double,
# MAGIC   `Currency` string,
# MAGIC   `Has Table booking` boolean,
# MAGIC   `Has Online delivery` boolean,
# MAGIC   `Is delivering now` boolean,
# MAGIC   `Switch to order menu` boolean,
# MAGIC   `Price range` int,
# MAGIC   `Aggregate rating` double,
# MAGIC   `Rating text` string,
# MAGIC   `Votes` int,
# MAGIC   `m_rating_colour` string,
# MAGIC   `m_cuisines` string,
# MAGIC   `p_filedate` bigint,
# MAGIC   `p_country_name` string,
# MAGIC   `create_datetime` timestamp,
# MAGIC   `user_id`string
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (`p_filedate`, `p_country_name`)
# MAGIC TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

# COMMAND ----------

# MAGIC %md
# MAGIC **ETL**

# COMMAND ----------

# DBTITLE 1,Flatten Dataframe
def flatten_dataframe(df):
    while True:
        fields = df.schema.fields
        complex_fields = [f for f in fields if isinstance(f.dataType, (ArrayType, StructType))]

        if not complex_fields:
            break

        for field in complex_fields:
            if isinstance(field.dataType, ArrayType):
                df = df.withColumn(field.name, explode_outer(field.name))

            elif isinstance(field.dataType, StructType):
                for subfield in field.dataType.fields:
                    df = df.withColumn(f"{field.name}-{subfield.name}", col(f"{field.name}.{subfield.name}"))
                df = df.drop(field.name)
    return df

# COMMAND ----------

# DBTITLE 1,Writing CSV Files
start_time = datetime.now()

try:
    for file in os.listdir(source_files):
        df = spark.read.format("json").json(os.path.join(source_files, file))
        
        df = flatten_dataframe(df)
        for column in df.columns:
            df = df.withColumnRenamed(column, column.split("-")[-1])

        df = df.select(
            col("res_id").alias("Restaurant ID"),
            col("name").alias("Restaurant Name"),
            col("country_id").alias("Country Code"),
            col("city").alias("City"),
            col("address").alias("Address"),
            col("locality").alias("Locality"),
            col("locality_verbose").alias("Locality Verbose"),
            col("longitude").alias("Longitude"),
            col("latitude").alias("Latitude"),
            col("cuisines").alias("Cuisines"),
            col("average_cost_for_two").alias("Average Cost for two"),
            col("currency").alias("Currency"),
            col("has_table_booking").alias("Has Table booking"),
            col("has_online_delivery").alias("Has Online delivery"),
            col("is_delivering_now").alias("Is delivering now"),
            col("switch_to_order_menu").alias("Switch to order menu"),
            col("price_range").alias("Price range"),
            col("aggregate_rating").alias("Aggregate rating"),
            col("rating_text").alias("Rating text"),
            col("votes").alias("Votes")
        )

        df = df.filter((col("Restaurant ID").isNotNull()) & (col("Restaurant Name").isNotNull()))

        filedate = file_mapping[file]
        df = df.withColumn("filedate", lit(filedate))

        df_partition = df.toPandas()
        partition_path = f"{target_files}/filedate={filedate}"
        os.makedirs(partition_path, exist_ok=True)
        df_partition.to_csv(f"{partition_path}/zomato_{filedate}.csv", index=False)

    logging("writing CSV Files", start_time, True, None)
except Exception as e:
    logging("writing CSV Files", start_time, False, e)

# COMMAND ----------

# DBTITLE 1,Loading zomato Table
start_time = datetime.now()

try:
    df = spark.read.format("csv").option("header", "true").load(f"{target_files}*").distinct()
    df.write.mode("overwrite").option("mergeschema", "true").option("overwriteschema", "true").saveAsTable("workspace.infocepts.zomato")

    logging("Loading zomato Table", start_time, True, None)
except Exception as e:
    logging("Loading zomato Table", start_time, False, e)

# COMMAND ----------

# DBTITLE 1,Loading dim_country Table
start_time = datetime.now()

try:
    df = spark.read.format("csv").option("header", "true").load(os.path.join(raw_files, "country_code.csv"))
    df.write.mode("overwrite").option("mergeschema", "true").option("overwriteschema", "true").saveAsTable("workspace.infocepts.dim_country")

    logging("Loading dim_country Table", start_time, True, None)
except Exception as e:
    logging("Loading dim_country Table", start_time, False, e)

# COMMAND ----------

# DBTITLE 1,Creating zomato_summary Table
start_time = datetime.now()

try:
    zs_df = spark.read.table("workspace.infocepts.zomato")
    country_df = spark.read.table("workspace.infocepts.dim_country")

    zs_df = zs_df.join(country_df, ["Country Code"], "left") \
            .withColumnRenamed("filedate", "p_filedate") \
            .withColumnRenamed("Country", "p_country_name")

    zs_df = zs_df.withColumn("Aggregate rating", col("Aggregate rating").cast("double")) \
            .withColumn("m_rating_colour", 
        when((col("Aggregate rating") >= 1.9) & (col("Aggregate rating") <= 2.4) & (col("Rating text") == "Poor"), "Red")
        .when((col("Aggregate rating") >= 2.5) & (col("Aggregate rating") <= 3.4) & (col("Rating text") == "Average"), "Amber")
        .when((col("Aggregate rating") >= 3.5) & (col("Aggregate rating") <= 3.9) & (col("Rating text") == "Good"), "Light Green")
        .when((col("Aggregate rating") >= 4.0) & (col("Aggregate rating") <= 4.4) & (col("Rating text") == "Very Good"), "Green")
        .when((col("Aggregate rating") >= 4.5) & (col("Aggregate rating") <= 5) & (col("Rating text") == "Excellent"), "Gold")
        .otherwise(None)
    )

    indian_list = [
        "north indian", "south indian", "indian", "andhra", "goan", "hyderabadi", "biryani", "mughlai", "awadhi", "rajasthani",
        "maharashtrian", "gujarati", "bengali", "kerala", "kashmiri", "chettinad", "malwani", "lucknowi", "parsi", "assamese", "bihari", "oriya", "naga"
    ]
    world_cuisines = [
        "american", "new american", "southern", "mexican", "tex-mex", "italian", "french", "spanish", "portuguese", "greek",
        "german", "belgian", "scottish", "irish", "british", "turkish", "middle eastern", "arabian", "lebanese", "persian",
        "moroccan", "african", "south african", "chinese", "asian", "asian fusion", "japanese", "korean",    "thai", "vietnamese", "taiwanese", "malaysian", "malay", "indonesian", "singaporean", "burmese", "filipino", "nepalese", "tibetan", "sri lankan", "brazilian", "argentine", "peruvian", "latin american", "south american", "cuban", "caribbean", "australian", "modern australian", "kiwi", "cantonese", "dim sum", "sushi", "ramen", "steak", "burger", "pizza", "seafood", "continental", "european", "mediterranean", "international", "world cuisine", "fusion"
    ]

    zs_df = zs_df.withColumn("m_cuisines", transform(split(col("Cuisines"), ","), lambda x: trim(lower(x))))
    zs_df = zs_df.withColumn("m_cuisines", 
                    when(size(array_intersect(col("m_cuisines"), lit(indian_list))) > 0, "Indian")
                    .when(size(array_intersect(col("m_cuisines"), lit(world_cuisines))) > 0, "World Cuisines")
                    .otherwise(None)
    )
    zs_df = zs_df.filter((col("m_cuisines").isNotNull()) & (col("m_cuisines") != "") & (col("Cuisines").isNotNull()) & (col("Cuisines") != ""))

    zs_df = zs_df.withColumn("create_datetime", current_timestamp()) \
                .withColumn("user_id", current_user())

    logging("Preparing zomato_summary Table", start_time, True, None)
except Exception as e:
    logging("Preparing zomato_summary Table", start_time, False, e)

# COMMAND ----------

# DBTITLE 1,Loading zomato_summary Table
start_time = datetime.now()

try:
        df = spark.read.table("workspace.infocepts.zomato_summary")

        for field in df.schema.fields:
                zs_df = zs_df.withColumn(field.name, col(field.name).cast(field.dataType))
                if isinstance(field.dataType, StringType):
                        zs_df = zs_df.withColumn(field.name,
                                when((col(field.name).isNull()) & (col(field.name) == ""), lit("NA"))
                                .otherwise(col(field.name))
                        )

        zs_df.write.mode("overwrite").saveAsTable("workspace.infocepts.zomato_summary")

        logging("Loading zomato_summary Table", start_time, True, None)
except Exception as e:
        logging("Loading zomato_summary Table", start_time, False, e)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from workspace.infocepts.zomato_summary_log