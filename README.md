# Sparkify Analysis - Airflow AWS

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Dataset

- Log data: s3://udacity-dend/log-data
- Song data: s3://udacity-dend/song-data
- Log path: s3://udacity-dend/log_json_path.json

## Project Steps

1. Create an IAM User in AWS.
   Set permissions to this user:

- AdministratorAccess
- AmazonRedshiftFullAccess
- AmazonS3FullAccess

2.  Connect Airflow and AWS.

<img src="images/airflow_aws_connection.png" width="70%">

3. Configure Redshift Serverless in AWS.

- Create Redshift Role in AWS Console

```bash
aws iam create-role --role-name my-redshift-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "redshift.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'
```

- Provide the full access to S3

```bash
aws iam attach-role-policy --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess --role-name my-redshift-service-role
```

- Create a Redshift Cluster
- Turn on Publicly accessible for Redshift Workgroup and Redshift Cluster
- Add more inbound rule for the VPC security group in Redshift Workgroup.

<img src="images/vpc_inbound_rule.png" width="70%">

4. Connect Airflow to AWS Redshift Serverless

<img src="images/airflow_redshift_connection.png" width="70%">

5. Copy S3 data
   The data for the project is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we will copy the data to your bucket in the same AWS Region where you created the Redshift workgroup so that Redshift can access the bucket.

- Copy the data from the udacity bucket to our bucket:

```bash
aws s3 sync s3://udacity-dend/log-data/ s3://sparkify-lake-house/log-data/
aws s3 sync s3://udacity-dend/song-data/ s3://sparkify-lake-house/song-data/
aws s3 cp s3://udacity-dend/log_json_path.json ~/
aws s3 cp ~/log_json_path.json s3://sparkify-lake-house/
```

6. Configure Variables in Airflow for S3

<img src="images/airflow_variables.png" width="70%">

7. Configure the DAG with parameters and setup task dependencies

- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry
- Run once an hour

<img src="images/airflow_DAG.png" width="70%">

8. Build the operators

- Stage Operator `stage_redshift.py` : The stage operator is expected to be able to load any JSON-formatted files from S3 to Amazon Redshift.
- Fact and Dimension Operators `load_fact.py` and `load_dimension.py` : ost of the logic is within the SQL transformations, and the operator is expected to take as input a SQL statement and target database on which to run the query against.
- Data Quality Operator `data_quality.py` : The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests.

## Results

1. Data is loaded into songplays fact table

<img src="images/songplays_table.png" width="70%">

2. The DAG is success for all tasks

<img src="images/sparkify_DAG_success.png" width="70%">
