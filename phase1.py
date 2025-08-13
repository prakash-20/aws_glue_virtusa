from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
from pyspark.sql.functions import col,trim
import logging

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("customer_data_phase1")

try:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Phase 1 started")
    customers_glue_df = glueContext.create_dynamic_frame.from_catalog(
        database="customer_db",
        table_name="customers_dataset"
    )

    customers_df = customers_glue_df.toDF()

    customers_dedup = customers_df.dropDuplicates(['customer_id'])

    customers_clean = customers_dedup.filter(
        (col("email").isNotNull()) & (trim(col("email")) != "") &
        (col("zip_code").isNotNull()) & (trim(col("zip_code").cast("string")) != "")
    )

    output_path = "s3://glue-test-dev-bucket0000/capstone/refined/"

    logger.info(f"customers_clean cnt :{customers_clean.count()}")

    customers_clean.write.mode("overwrite").parquet(output_path)

    logger.info(f"Cleaned customer data written to {output_path}")

    logger.info("Phase 1 completed")
except Exception as e:
    logger.error(f"Error occurred : {e}")
    raise Exception(e)


job.commit()
