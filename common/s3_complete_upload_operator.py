from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3


class S3CompleteUploadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 # access_key,
                 # secret_key,
                 # session_token,
                 upload_id,
                 bucket,
                 key,
                 *args,
                 **kwargs):
        super(S3CompleteUploadOperator, self).__init__(*args, **kwargs)
        # self.ACCESS_KEY = access_key
        # self.SECRET_KEY = secret_key
        # self.SESSION_TOKEN = session_token
        self.upload_id = upload_id
        self.bucket = bucket
        self.key = key

    def execute(self, context):
        client = boto3.client('s3')
                              # aws_access_key_id=self.ACCESS_KEY,
                              # aws_secret_access_key=self.SECRET_KEY,
                              # aws_session_token=self.SESSION_TOKEN)
        client.complete_multipart_upload(
                           Bucket=self.bucket,
                           Key=self.key,
                           UploadId=self.upload_id)

