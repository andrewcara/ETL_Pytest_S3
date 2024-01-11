""" Test S3BucketConnectorMethods"""

import os 
import pytest

import boto3

from moto import mock_s3

from src.common.s3 import S3BucketConnector


class TestS3BucketConnector:
    """
    Testing the S3 bucket connector
    """
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        
        self.s3_access_key = ''
        self.s3_secret_key = ''
        self.s3_bucket_name = 'test-bucket'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                             CreateBucketConfiguration={'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        
    def teardown(self):
        self.mock_s3.stop() 
    
    def test_list_files_in_prefix_ok(self):
        # Expected results
        prefix_exp = 'prefix/'
        
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        
        csv_content = """col1, col2
        valA, valB"""
        
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        
        assert len(list_result) == 2
        
        
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key':key1_exp
                    },
                    {
                        'Key':key2_exp
                    }
                    
                ]
            }
        )
        # method execution
        # Tests after method execution
        