from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from common.sftp_clean_folder_operator import SFTPCleanFolderOperator
from common.sftp_to_s3_upload_part_operator import SFTPToS3UploadPartOperator
from common.s3_complete_upload_operator import S3CompleteUploadOperator
from common.airflow_common import dag_prefix

dag_name = "{}_sftp_sensor_and_initiate_multipart_upload".format(dag_prefix())
dag_config = Variable.get(dag_name)
my_email_address = dag_config['email']
chunk_size = dag_config['chunk_size']
chunk_size = int(chunk_size)
sftp_conn = dag_config['sftp_conn']
# access_key = dag_config['ACCESS_KEY']
# secret_key = dag_config['SECRET_KEY']
# session_token = dag_config['SESSION_TOKEN']
bucket = dag_config['bucket']
key = dag_config['key']
upload_id = dag_config['UploadId']
source_path = dag_config['source_path']
filename = dag_config['filename']
number_of_tasks = dag_config['number_of_chunks']
number_of_tasks = int(number_of_tasks)
task_names = ['process_chunk_'+str(k) for k in range(0, number_of_tasks)]

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

    return SFTPToS3UploadPartOperator(
        task_id=task_name_j,
        conn_id_source=sftp_conn,
        file_source_path=source_path + filename,
        # access_key=access_key,
        # secret_key=secret_key,
        # session_token=session_token,
        upload_id=upload_id,
        bucket=bucket,
        key=key,
        chunk_number=j,
        chunk_size=chunk_size,
        dag=dag)


upload_parts = [get_task(i, task_name) for i, task_name in enumerate(task_names)]

# the following task is created by instantiating an operator

complete_upload = S3CompleteUploadOperator(
        task_id='complete_upload',
        # access_key=access_key,
        # secret_key=secret_key,
        # session_token=session_token,
        upload_id=upload_id,
        bucket=bucket,
        key=key,
        dag=dag)


# clean_source_folder = SFTPCleanFolderOperator(
#         task_id='clean_source_folder',
#         conn_id='sftp_default',
#         dir_path=source_path,
#         dag=dag)

# upload_parts >> complete_upload >> clean_source_folder
upload_parts >> complete_upload
