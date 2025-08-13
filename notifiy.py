import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

import boto3
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_sns_message():
    try:
        sns = boto3.client('sns')
        topic_arn = 'arn:aws:sns:us-east-1:872112795087:notification'

        message = "Glue Workflow completed successfully."
        subject = "Capstone ETL Success"

        sns.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )
        print("Mail sent")
    except Exception as e:
        logger.error(f"Error occurred : {e}")
        raise Exception(e)

if __name__ == "__main__":
    send_sns_message()


job.commit()