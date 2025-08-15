# aws_glue_virtusa

As part of this project, I used 
Glue jobs - pyspark script,
Glue Crawler,
RDS - Postgres,
Dbevear/pgAdmin,
Glue Workflow,
AWS SNS,
AWS S3,
Athena,
IAM Policies,
CloudWatch,
Secret Manager
<br>
<br>

Project Details:
Once the Glue Workflow triggered:
1. Crawler will scan the customer and geo location data and update the glue catalog.
2. Phase 1 - Glue Job will run. It will read the customer data set and cleanse the data
3. Phase 2 - Glue Job will triggered once the Phase 1 succeeded. This will read the cleansed data and do joins/transformations as per requirement and put it in s3 bucket.
4. Once Phase 2 succeeded, Crawler will scan the published data and update the glue data catalog.
5. Finally Notification will be sent to user using AWS SNS.
6. Published data can be queried using Athena.

Workflow:<br>
![alt text](https://github.com/prakash-20/aws_glue_virtusa/blob/main/Snapshots/workflow.PNG?raw=true)

Secret Manager to store postgres credentials:<br>
![alt text](https://github.com/prakash-20/aws_glue_virtusa/blob/main/Snapshots/secretManager.PNG?raw=true)

Postgres connection:<br>
![alt text](https://github.com/prakash-20/aws_glue_virtusa/blob/main/Snapshots/dbeaver.png?raw=true)

Crawlers:<br>
![alt text](https://github.com/prakash-20/aws_glue_virtusa/blob/main/Snapshots/crawler.PNG?raw=true)

Athena:<br>
![alt text](https://github.com/prakash-20/aws_glue_virtusa/blob/main/Snapshots/athena.PNG?raw=true)


<br>
<br>
<br>

prakashkathirvel20@gmail.com
