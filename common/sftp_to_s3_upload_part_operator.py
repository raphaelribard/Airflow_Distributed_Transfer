from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sftp_hook import SFTPHook
import boto3


class SFTPToS3UploadPartOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id_source,
                 file_source_path,
                 # access_key,
                 # secret_key,
                 # session_token,
                 upload_id,
                 bucket,
                 key,
                 chunk_number,
                 chunk_size,
                 *args,
                 **kwargs):
        super(SFTPToS3UploadPartOperator, self).__init__(*args, **kwargs)
        self.conn_id_source = conn_id_source
        self.file_source_path = file_source_path
        # self.ACCESS_KEY = access_key
        # self.SECRET_KEY = secret_key
        # self.SESSION_TOKEN = session_token
        self.upload_id = upload_id
        self.bucket = bucket
        self.key = key
        self.chunk_number = chunk_number
        self.chunk_size = chunk_size

    def execute(self, context):
        conn_source = SFTPHook(ftp_conn_id=self.conn_id_source)
        my_conn_source = conn_source.get_conn()
        source_file = my_conn_source.sftp_client.file(self.file_source_path, 'r')
        source_file.seek(self.chunk_number * self.chunk_size)
        payload = source_file.read(self.chunk_size)
        client = boto3.client('s3')
                              # aws_access_key_id=self.ACCESS_KEY,
                              # aws_secret_access_key=self.SECRET_KEY,
                              # aws_session_token=self.SESSION_TOKEN)
        client.upload_part(Body= payload,
                           Bucket=self.bucket,
                           Key=self.key,
                           PartNumber=self.chunk_number,
                           UploadId=self.upload_id)

