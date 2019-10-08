from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import boto3


class S3InitiateMultipartUploadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 # access_key,
                 # secret_key,
                 # session_token,
                 master_variable,
                 bucket,
                 key,
                 *args,
                 **kwargs):
        super(S3InitiateMultipartUploadOperator, self).__init__(*args, **kwargs)
        # self.ACCESS_KEY = access_key
        # self.SECRET_KEY = secret_key
        # self.SESSION_TOKEN = session_token
        self.master_variable = master_variable
        self.bucket = bucket
        self.key = key

    def execute(self, context):
        client = boto3.client('s3')
                              # aws_access_key_id=self.ACCESS_KEY,
                              # aws_secret_access_key=self.SECRET_KEY,
                              # aws_session_token=self.SESSION_TOKEN)
        multipart = client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        master_variable_dict = Variable.get(self.master_variable)
        master_variable_dict['UploadId'] = multipart['UploadId']
        Variable.set(self.master_variable, master_variable_dict)
