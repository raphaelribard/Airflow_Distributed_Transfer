from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from clean_folder_operator import CleanFolderOperator
from sftp_create_empty_file_operator import SFTPCreateEmptyFileOperator
from sftp_to_sftp_chunk_transfer_operator import SFTPtoSFTPChunkTransferOperator

my_email_address = Variable.get('email')
number_of_tasks = Variable.get('number_of_chunks', default_var=2)
number_of_tasks = int(number_of_tasks)
chunk_size = Variable.get('chunk_size', default_var=4098)
chunk_size = int(chunk_size)
filename = Variable.get('filename')
source_path = Variable.get('source_path')
destination_path = Variable.get('destination_path')
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
def get_task(j, task_name_j):

    return SFTPtoSFTPChunkTransferOperator(
        task_id=task_name_j,
        conn_id_source='sftp_default',
        conn_id_destination='sftp_default',
        file_source_path=source_path + filename,
        file_destination_path=destination_path + filename,
        chunk_number=j,
        chunk_size=chunk_size,
        dag=dag)


dynamic_tasks = [get_task(j, task_name) for j, task_name in enumerate(task_names)]

# the following task is created by instantiating an operator

create_output_file = SFTPCreateEmptyFileOperator(
        task_id='create_output_file',
        conn_id='sftp_default',
        file_path=destination_path + filename,
        dag=dag)

clean_source_folder = CleanFolderOperator(
        task_id='clean_source_folder',
        conn_id='sftp_default',
        dir_path=source_path,
        dag=dag)

create_output_file >> dynamic_tasks >> clean_source_folder
