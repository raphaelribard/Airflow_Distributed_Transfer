from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook


class SFTPCleanFolderOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 dir_path,
                 *args,
                 **kwargs):
        super(SFTPCleanFolderOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.dir_path = dir_path

    def execute(self, context):
        conn = SFTPHook(ftp_conn_id=self.conn_id)
        my_conn = conn.get_conn()
        files_to_be_removed = my_conn.listdir(self.dir_path)
        for file_name in files_to_be_removed:
            my_conn.remove(self.dir_path + file_name)

