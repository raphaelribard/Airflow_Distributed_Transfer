from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook


class SFTPtoSFTPChunkTransferOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id_source,
                 conn_id_destination,
                 file_source_path,
                 file_destination_path,
                 chunk_number,
                 chunk_size,
                 *args,
                 **kwargs):
        super(SFTPtoSFTPChunkTransferOperator, self).__init__(*args, **kwargs)
        self.conn_id_source = conn_id_source
        self.conn_id_destination = conn_id_destination
        self.file_source_path = file_source_path
        self.file_destination_path = file_destination_path
        self.chunk_number = chunk_number
        self.chunk_size = chunk_size

    def execute(self, context):
        conn_source = SFTPHook(ftp_conn_id=self.conn_id_source)
        my_conn_source = conn_source.get_conn()
        conn_destination = SFTPHook(ftp_conn_id=self.conn_id_destination)
        my_conn_destination = conn_destination.get_conn()
        source_file = my_conn_source.sftp_client.file(self.file_source_path, 'r')
        source_file.seek(self.chunk_number * self.chunk_size)
        payload = source_file.read(self.chunk_size)
        destination_file = my_conn_destination.sftp_client.file(self.file_destination_path, 'r+')
        destination_file.seek(self.chunk_number * self.chunk_size)
        destination_file.write(payload)

