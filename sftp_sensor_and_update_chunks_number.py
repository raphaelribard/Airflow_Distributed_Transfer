from airflow import DAG
from datetime import timedelta
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils import timezone
from common.sftp_update_nb_of_chunks_operator import SFTPUpdateNbOfChunksOperator
from common.airflow_common import dag_prefix

# All the necessary values which could be retrieved from a dictionary contained in an Airflow variable
# instead of multiple variable
dag_name = "{}_sftp_sensor_and_update_chunks_number".format(dag_prefix())
dag_config = Variable.get(dag_name)
my_email_address = dag_config['email']
chunk_size = dag_config['chunk_size']
chunk_size = int(chunk_size)
destination_path = dag_config['destination_path']
source_path = dag_config['source_path']
filename = dag_config['filename']
dag_to_be_triggered = "{}_s3_to_sftp_single_file_concurrent_transfer".format(dag_prefix())

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

dag = DAG(
    dag_name, catchup=False, default_args=default_args, schedule_interval="30 8 * * * *")

# the following tasks are created by instantiating operators
detect_file = SFTPSensor(task_id='detect_file',
                         poke_interval=10,
                         timeout=3600,
                         sftp_conn_id='sftp_default',
                         path=source_path + filename,
                         dag=dag)

update_nb_of_chunks = SFTPUpdateNbOfChunksOperator(
    task_id='update_nb_of_chunks',
    conn_id='sftp_default',
    file_path=source_path + filename,
    master_variable=dag_name,
    chunks_variable_name="number_of_chunks",
    chunk_size=chunk_size,
    dag=dag)

# These are passed in as args. Seems that they are not sent : airflow bug.
dag.start_date = default_args['start_date']

trigger_single_file_concurrent_transfer = TriggerDagRunOperator(task_id='trigger_single_file_concurrent_transfer',
                                                                trigger_dag_id=dag_to_be_triggered)

detect_file >> update_nb_of_chunks >> trigger_single_file_concurrent_transfer
