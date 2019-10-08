from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import Variable
import time
import math


class SFTPUpdateNbOfChunksOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 file_path,
                 master_variable,
                 chunks_variable_name,
                 chunk_size,
                 *args,
                 **kwargs):
        super(SFTPUpdateNbOfChunksOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_path = file_path
        self.master_variable = master_variable
        self.chunks_variable_name = chunks_variable_name
        self.chunk_size = chunk_size

    def execute(self, context):
        conn = SFTPHook(ftp_conn_id=self.conn_id)
        my_conn = conn.get_conn()
        total_size = my_conn.lstat(self.file_path).st_size
        master_variable_dict = Variable.get(self.master_variable)
        master_variable_dict[self.chunks_variable_name] = math.ceil(total_size / self.chunk_size)
        Variable.set(self.master_variable, master_variable_dict)
        time.sleep(5)
