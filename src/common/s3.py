"""Connector and methods accessing S3"""

import os 
import logging
import pandas as pd
import boto3
from io import StringIO
import yaml

class S3BucketConnector:
    """
    class for interacting with S3 buckets
    """
    
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket:str) -> None:
        """_summary_

        Args:
            access_key (str): access key for accessing S3
            secret_key (str): secret key for accessing S3
            endpoint_url (str): endpoint for accessing S3
            bucket (str): bucket in S3
        """
        self._logger = logging.getLogger(__name__)
        
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = access_key, 
                                     aws_secret_access_key = secret_key)
        self._s3 = self.session.resource(service_name='s3')
        self._bucket = self._s3.Bucket(bucket)
        
    def list_files_in_prefix(self, prefix:str) -> list:
        """_summary_

        Args:
            prefix (str): prefix on the S3 bucket that should be filtered with

        Returns:
            files: list of all the file names containing prefix in the name
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    
    def read_csv_to_df(self, obj:str) -> pd.DataFrame:
        
        """Reads the csv from the S3 bucket and converts it to a dataframe

        Returns:
            pd.DataFrame: returns pandas DataFrame
        """
        self._logger.info('Reading File %s/%s', self.endpoint_url)
        csv_obj = self._bucket.Object(key=obj).get().get('Body').read().decode('utf-8')
        data = StringIO(csv_obj)
        df_init = pd.read_csv(data, delimiter=',')
        return df_init
    
    def write_df_to_s3(self, df:pd.DataFrame, target_bucket:str)->None:
        """Writes DataFrame to S3 Target 
        --To-Do
        target_bucket name to be received from .config input file
        """
        
        csv_buffer=StringIO()
        df.to_csv(csv_buffer)
        self._s3.Object(target_bucket, 'df_end_to_end.csv').put(Body=csv_buffer.getvalue())



