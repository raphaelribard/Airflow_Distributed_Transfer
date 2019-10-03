from airflow import DAG
from datetime import timedelta
from airflow.contrib.sensors.sftp_sensor import SFTPSensor
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils import timezone
from clean_folder_operator import CleanFolderOperator
from update_nb_of_chunks_operator import UpdateNbOfChunksOperator


# All the necessary values which could be retrieved from a dictionary contained in an Airflow variable
# instead of multiple variable
my_email_address = Variable.get('email')
chunk_size = Variable.get('chunk_size', default_var=4098)
chunk_size = int(chunk_size)
destination_path = Variable.get('destination_path')
source_path = Variable.get('source_path')
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

dag = DAG(
    'sensor_and_update_chunks_number', catchup=False, default_args=default_args, schedule_interval="30 8 * * * *")

# the following tasks are created by instantiating operators
detect_file = SFTPSensor(task_id='detect_file',
                         poke_interval=10,
                         timeout=3600,
                         sftp_conn_id='sftp_default',
                         path=source_path + filename,
                         dag=dag)

clean_destination_folder = CleanFolderOperator(
        task_id='clean_destination_folder',
        conn_id='sftp_default',
        dir_path=destination_path,
        dag=dag)


update_chunks_variable = UpdateNbOfChunksOperator(
        task_id='update_nb_of_chunks_variable',
        conn_id='sftp_default',
        file_path=source_path + filename,
        chunks_variable_name="number_of_chunks",
        chunk_size=chunk_size,
        dag=dag)

# These are passed in as args. Seems that they are not sent : airflow bug.
dag.start_date = default_args['start_date']

trigger_distributed_copy = TriggerDagRunOperator(task_id='trigger_distributed_copy',
                                                 trigger_dag_id='distributed_copy')


detect_file >> clean_destination_folder >> update_chunks_variable >> trigger_distributed_copy
