from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql import functions as F
import logging
import boto3
import json
from botocore.exceptions import ClientError

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("customer_data_phase2")

def get_secret():

    secret_name = "rds/postgres/test_db"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

try:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Phase 2 started")

    customers_df = spark.read.parquet("s3://glue-test-dev-bucket0000/capstone/refined/")
    logger.info(f"customers_df cnt :{customers_df.count()}")

    logger.info("Getting secrets")
    secret_dict = get_secret()

    username = secret_dict['username']
    password = secret_dict['password']
    host = secret_dict['host']
    port = secret_dict['port']
    dbname = secret_dict['dbname']

    logger.info("secrets fetched")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    jdbc_props = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    
    logger.info(f"Connecting to Postgres - {jdbc_url}")

    transactions_df = spark.read.jdbc(url=jdbc_url, table="transactions", properties=jdbc_props)

    geo_location_df = glueContext.create_dynamic_frame.from_catalog(
        database="customer_db",
        table_name="geolocation_dataset"
    )
    geolocation_df = geo_location_df.toDF()

    cust_trans_df = customers_df.join(transactions_df, "customer_id", "left")

    cust_trans_geo_df = cust_trans_df.join(
        geolocation_df.select("zip_code", "city", "state"),
        "zip_code",
        "left"
    )

    agg_df = cust_trans_geo_df.groupBy(
        "customer_id", "email", "zip_code", "city", "state"
    ).agg(
        F.sum("transaction_amount").alias("total_transaction_amount"),
        F.count("transaction_id").alias("transaction_count")
    )

    year_df = cust_trans_geo_df.groupBy("customer_id").agg(F.min(F.year("transaction_date")).alias("year"))

    final_df = agg_df.join(year_df, "customer_id", "left")

    logger.info(f"Final Count:{final_df.count()}")

    output_path = "s3://glue-test-dev-bucket0000/capstone/publish/"

    # logger.info("Null state count:", final_df.filter(final_df.state.isNull() | (final_df.state == "")).count())
    # final_df_filled = final_df.withColumn(
    #     "state", F.when(final_df.state.isNull() | (final_df.state == ""), "NULL").otherwise(final_df.state)
    # ).withColumn(
    #     "year", F.when(final_df.year.isNull() | (final_df.year == ""), "NULL").otherwise(final_df.year)
    #     )

    final_df.write.mode("overwrite").partitionBy("state", "year").parquet(output_path)

    logger.info(f"Final custoemr data written successfully! - {output_path}")

    logger.info("Phase 2 completed")

except Exception as e:
    logger.error(f"Error occurred : {e}")
    raise Exception(e)

job.commit()
