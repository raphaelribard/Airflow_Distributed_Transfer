from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook
import boto3


class S3toSFTPChunkTransferOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id_destination,
                 file_destination_path,
                 access_key,
                 secret_key,
                 session_token,
                 bucket,
                 key,
                 chunk_number,
                 chunk_size,
                 *args,
                 **kwargs):
        super(S3toSFTPChunkTransferOperator, self).__init__(*args, **kwargs)
        self.conn_id_destination = conn_id_destination
        self.file_destination_path = file_destination_path
        self.ACCESS_KEY = access_key
        self.SECRET_KEY = secret_key
        self.SESSION_TOKEN = session_token
        self.bucket = bucket
        self.key = key
        self.chunk_number = chunk_number
        self.chunk_size = chunk_size

    def execute(self, context):
        conn_destination = SFTPHook(ftp_conn_id=self.conn_id_destination)
        my_conn_destination = conn_destination.get_conn()

        start_byte = self.chunk_number * self.chunk_size
        stop_byte = (self.chunk_number + 1) * self.chunk_size - 1

        client = boto3.client('s3',
                              aws_access_key_id=self.ACCESS_KEY,
                              aws_secret_access_key=self.SECRET_KEY,
                              aws_session_token=self.SESSION_TOKEN)

        chunk=client.get_object(Bucket=self.bucket,
                                Key=self.key,
                                Range='bytes={}-{}'.format(start_byte, stop_byte))
        payload = chunk['Body'].read()

        destination_file = my_conn_destination.sftp_client.file(self.file_destination_path, 'r+')
        destination_file.seek(self.chunk_number * self.chunk_size)
        destination_file.write(payload)

