from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.utils import timezone

import os
import math
import time


my_email_address = Variable.get('email')
chunk_size = Variable.get('chunk_size', default_var=4098)
chunk_size = int(chunk_size)
out_path = Variable.get('out_path')
in_path = Variable.get('in_path')
filename = Variable.get('filename')
bash_path = Variable.get('bash_path')


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


def clean_destination_folder():
    """This is a function that will run within the DAG execution"""
    conn = SFTPHook(ftp_conn_id='sftp_default')
    my_conn = conn.get_conn()
    files_to_be_removed = my_conn.listdir(in_path)
    print(files_to_be_removed)
    for file in files_to_be_removed:
        my_conn.remove(in_path + file)


def update_chunks_number():
    """This is a function that will run within the DAG execution"""
    conn = SFTPHook(ftp_conn_id='sftp_default')
    my_conn = conn.get_conn()
    total_size = my_conn.lstat(out_path+filename).st_size
    # total_size = os.path.getsize(out_path)
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

clean_destination_folder = PythonOperator(
        task_id='clean_destination_folder',
        python_callable=clean_destination_folder,
        dag=dag)


update_chunks_variable = PythonOperator(
        task_id='update_chunks_variable',
        python_callable=update_chunks_number,
        dag=dag)

# These are passed in as args. Seems that they are not sent that way is a bug.
dag.start_date = default_args['start_date']

trigger_distributed_copy = TriggerDagRunOperator(task_id='trigger_distributed_copy',
                                                 trigger_dag_id='distributed_copy')


detect_file >> clean_destination_folder >> update_chunks_variable >> trigger_distributed_copy
