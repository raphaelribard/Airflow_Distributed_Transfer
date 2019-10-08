from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from common.s3_clean_object_operator import S3CleanObjectOperator
from common.sftp_clean_folder_operator import SFTPCleanFolderOperator
from common.sftp_create_empty_file_operator import SFTPCreateEmptyFileOperator
from common.s3_to_sftp_chunk_transfer_operator import S3toSFTPChunkTransferOperator
from common.airflow_common import dag_prefix

dag_name = "{}_s3_to_sftp_single_file_concurrent_transfer".format(dag_prefix())
dag_config = Variable.get(dag_name)
my_email_address = dag_config['email']
chunk_size = dag_config['chunk_size']
chunk_size = int(chunk_size)
access_key = dag_config['ACCESS_KEY']
secret_key = dag_config['SECRET_KEY']
session_token = dag_config['SESSION_TOKEN']
bucket = dag_config['bucket']
key = dag_config['key']
destination_path = dag_config['destination_path']
filename = dag_config['filename']
number_of_tasks = dag_config['number_of_chunks']
number_of_tasks = int(number_of_tasks)
task_names = ['process_chunk_'+str(j) for j in range(0, number_of_tasks)]

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
    dag_name, catchup=False, default_args=default_args, schedule_interval=None)


# the following tasks are created by instantiating operators dynamically
def get_task(j, task_name_j):

    return S3toSFTPChunkTransferOperator(
        task_id=task_name_j,
        conn_id_destination='sftp_default',
        file_destination_path=destination_path + filename,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        bucket=bucket,
        key=key,
        chunk_number=j,
        chunk_size=chunk_size,
        dag=dag)


dynamic_tasks = [get_task(j, task_name) for j, task_name in enumerate(task_names)]

# the following task is created by instantiating an operator

clean_destination_folder = SFTPCleanFolderOperator(
    task_id='clean_destination_folder',
    conn_id='sftp_default',
    dir_path=destination_path,
    dag=dag)

create_output_file = SFTPCreateEmptyFileOperator(
        task_id='create_output_file',
        conn_id='sftp_default',
        file_path=destination_path + filename,
        dag=dag)

clean_source_object = S3CleanObjectOperator(
        task_id='clean_source_object',
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
        bucket=bucket,
        key=key,
        dag=dag)

clean_destination_folder >> create_output_file >> dynamic_tasks >> clean_source_object
