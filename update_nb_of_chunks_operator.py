from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import Variable
import time
import math


class UpdateNbOfChunksOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 file_path,
                 chunks_variable_name,
                 chunk_size,
                 *args,
                 **kwargs):
        super(UpdateNbOfChunksOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_path = file_path
        self.chunks_variable_name = chunks_variable_name
        self.chunk_size=chunk_size

    def execute(self, context):
        conn = SFTPHook(ftp_conn_id=self.conn_id)
        my_conn = conn.get_conn()
        total_size = my_conn.lstat(self.file_path).st_size
        Variable.set(self.chunks_variable_name, math.ceil(total_size / self.chunk_size))
        time.sleep(5)
