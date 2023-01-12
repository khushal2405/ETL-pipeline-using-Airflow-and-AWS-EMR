# ETL-pipeline-using-Airflow-and-AWS-EMR-cluster
We Build an ETL pipeline using Airflow that accomplishes the following: Downloads data from an AWS S3 bucket, Triggers a Spark/Spark SQL job in AWS EMR cluster remotely on the downloaded data producing a cleaned-up dataset of delivery deadline missing orders and then Upload the cleaned-up dataset back to the same S3 bucket in a folder primed for higher level analytics

# Problem statement
Retailers in the current landscape are adapting to the digital age. Digital retail behemoths have carved out substantial market shares in the online space at the same time that traditional retail stores are broadly in decline. In this time of digital flux, an omni-channel retail approach is necessary to keep pace. This is especially true for retailers that have invested in an extensive brick-and-mortar store portfolio or have strong relationships with brick-and-mortar partners.

This data engineering project uses a real world retail dataset to explore delivery performance at scale. The primary concern of data engineering efforts in this project is to create a strong foundation onto which data analytics and modeling may be applied as well as provide summary reports for daily ingestion by decision makers.

A series of ETL jobs are programmed as part of this project using python, SQL, Airflow, and Spark to build pipelines that download data from an AWS S3 bucket, apply some manipulations, and then load the cleaned-up data set into another location on the same AWS S3 bucket for higher level analytics.


# Dataset of choice
The dataset of choice for this project is a series of tables provided by the Brazilian Ecommerce company Olist.


# Methodology
Here, we built an ETL pipeline using airflow in order to load the required data and script into S3 data lake.
Then we scheduled the Dag in order to run the script into the EMR cluster by adding job flow steps using boto3 library.
So here we scuccessfully incorporated AWS EMR into the pipeline to run the Spark job on a cluster instead of locally on a machine.

![](https://github.com/khushal2405/ETL-pipeline-using-Airflow-and-AWS-EMR/blob/main/DAG_graph.PNG)

Feel free to check the Dag script!!!
# for more details and help please refer to these following links:
https://github.com/ajupton/big-data-engineering-project

https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/

https://github.com/josephmachado/spark_submit_airflow

# Room for Improvement
1. Here in this script rather than using hardcoded values, we can use agparser in order to parse parameters and use them while execution.
2. We could also use step functions in AWS EMR to execute the same project.
 

