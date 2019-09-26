from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

import os
import math
import time
from airflow.utils import timezone

my_email_address = Variable.get('email')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': timezone.datetime(2019, 9, 19),
    'email': [my_email_address],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

chunk_size = Variable.get('chunk_size', default_var=4098)
chunk_size = int(chunk_size)
out_path = Variable.get('out_path')
in_path = Variable.get('in_path')
bash_path=Variable.get('bash_path')


def update_chunks_number():
    """This is a function that will run within the DAG execution"""
    total_size = os.path.getsize(out_path)
    Variable.set("number_of_chunks", math.ceil(total_size / chunk_size))
    time.sleep(5)


dag = DAG(
    'sensor_and_update_chunks_number', catchup=False, default_args=default_args, schedule_interval="30 8 * * * *")

# the following tasks are created by instantiating operators
detect_file = SFTPSensor(task_id='detect_file',
                         poke_interval=10,
                         timeout=3600,
                         sftp_conn_id='sftp_default',
                         path=out_path,
                         dag=dag)

clean_destination_folder = BashOperator(
    task_id='clean_destination_folder',
    bash_command=bash_path+'cleanDestination.sh',
    dag=dag)

update_chunks_variable = PythonOperator(
        task_id='update_chunks_variable',
        python_callable=update_chunks_number,
        dag=dag,)

# These are passed in as args. Seems that they are not sent that way is a bug.
dag.start_date = default_args['start_date']

trigger_distributed_copy = TriggerDagRunOperator(task_id='trigger_distributed_copy',
                                                 trigger_dag_id='distributed_copy')


detect_file >> clean_destination_folder >> update_chunks_variable >> trigger_distributed_copy
