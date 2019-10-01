from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook

my_email_address = Variable.get('email')
number_of_tasks = Variable.get('number_of_chunks', default_var=2)
number_of_tasks = int(number_of_tasks)
chunk_size = Variable.get('chunk_size', default_var=4098)
chunk_size = int(chunk_size)
filename = Variable.get('filename')
out_path = Variable.get('out_path')
in_path = Variable.get('in_path')
task_names = ['chunk'+str(j)+'dynamic' for j in range(0, number_of_tasks)]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 19),
    'email': [my_email_address],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'distributed_copy', catchup=False, default_args=default_args, schedule_interval=None)

# the following tasks are created by instantiating operators dynamically


def create_output_file():
    conn = SFTPHook(ftp_conn_id='sftp_default')
    my_conn = conn.get_conn()
    my_conn.sftp_client.file(in_path + filename, 'a+')


def copy_chunk(chunk_number):
    """This is a function that will run within the DAG execution"""
    conn = SFTPHook(ftp_conn_id='sftp_default')
    my_conn = conn.get_conn()

    source_file = my_conn.sftp_client.file(out_path + filename, 'r')
    source_file.seek(chunk_number*chunk_size)
    payload = source_file.read(chunk_size)
    destination_file = my_conn.sftp_client.file(in_path + filename, 'r+')
    destination_file.seek(chunk_number*chunk_size)
    destination_file.write(payload)


def clean_source_folder():
    """This is a function that will run within the DAG execution"""
    conn = SFTPHook(ftp_conn_id='sftp_default')
    my_conn = conn.get_conn()
    files_to_be_removed = my_conn.listdir(out_path)
    for file in files_to_be_removed:
        my_conn.remove(out_path + file)


def get_task(j, task_name):

    return PythonOperator(
        task_id=task_name,
        python_callable=copy_chunk,
        op_kwargs={'chunk_number': j},
        dag=dag,)


dynamic_tasks = [get_task(j, task_name) for j, task_name in enumerate(task_names)]

# the following task is created by instantiating an operator

create_output_file = PythonOperator(
        task_id='create_output_file',
        python_callable=create_output_file,
        dag=dag)

clean_source_folder = PythonOperator(
        task_id='clean_source_folder',
        python_callable=clean_source_folder,
        dag=dag)

create_output_file >> dynamic_tasks >> clean_source_folder
