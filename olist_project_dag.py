from datetime import datetime, timedelta
from airflow import DAG
import boto3
#from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


BUCKET_NAME = "Enter the S3 bucket name"
local_data_1 = "/home/xxxxxxxxxxxxx/airflow-local/dags/Olist_Data/olist_order_items_dataset.csv"
local_data_2 = "/home/xxxxxxxxxxxxx/airflow-local/dags/Olist_Data/olist_orders_dataset.csv"
local_data_3 = "/home/xxxxxxxxxxxxx/airflow-local/dags/Olist_Data/olist_order_items_dataset.csv"
s3_data_1 = "data/olist_order_items_dataset.csv"
s3_data_2 = "data/olist_orders_dataset.csv"
s3_data_3 = "data/olist_products_dataset.csv"
local_script = "/home/xxxxxxxxxxxxx/airflow-local/dags/Olist_scripts/spark_missed_deadline_job.py"
s3_script = "script/Olist_script.py"





client = boto3.client('emr',region_name = 'ap-south-1',aws_access_key_id='xxxxxxxxx', aws_secret_access_key='xxxxxxxxxx')

default_args = {'start_date': datetime(2022,xx,xx),'schedule_interval':'@hourly'}
dag = DAG(
    "Olist-project-airflow",
    default_args = default_args,
    max_active_runs=1
)
cluster_id = "j-XXXXXXXXXXXXXXX"

SPARK_STEPS = [
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3n://olist-project/script/Olist_script.py",
            ],
        },
    }
] 

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)


# helper function
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
   


def addjobflowsteps(jobflowid,steps):
    client.add_job_flow_steps(JobFlowId=jobflowid, Steps=steps)
    
    
    
data_to_s3_1 = PythonOperator(
    dag=dag,
    task_id="data_to_s3_1",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_data_1, "key": s3_data_1,},
)

data_to_s3_2 = PythonOperator(
    dag=dag,
    task_id="data_to_s3_2",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_data_2, "key": s3_data_2,},
)

data_to_s3_3 = PythonOperator(
    dag=dag,
    task_id="data_to_s3_3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_data_3, "key": s3_data_3,},
)

script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script,},
)    
    
    
    
insertjobflowsteps = PythonOperator(
    dag=dag,
    task_id="insertjobflowsteps",
    python_callable=addjobflowsteps,
    op_kwargs={"jobflowid": cluster_id, "steps": SPARK_STEPS},
)    


end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)
    
	
start_data_pipeline >> [data_to_s3_1,data_to_s3_2,data_to_s3_3,script_to_s3] >> insertjobflowsteps
insertjobflowsteps >> end_data_pipeline






