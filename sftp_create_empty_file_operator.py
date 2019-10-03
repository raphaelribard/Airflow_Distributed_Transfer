from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook


class SFTPCreateEmptyFileOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 file_path,
                 *args,
                 **kwargs):
        super(SFTPCreateEmptyFileOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.file_path = file_path

    def execute(self, context):
        conn = SFTPHook(ftp_conn_id=self.conn_id)
        my_conn = conn.get_conn()
        my_conn.sftp_client.file(self.file_path, 'a+')
