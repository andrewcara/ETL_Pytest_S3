"""Running the ETL Application"""


import logging
import logging.config
from src.common.s3 import S3BucketConnector
from src.transformers.transformer import XetraETL
import yaml
import argparse



def main():
    """
    entry point to run the xetra ETL job
    """
    parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
    parser.add_argument('config', help='A configuration file in YAML format.')
    args = parser.parse_args()
    #config_path = '/Users/andrewcaravaggio/SideProjects/ETL/configs/xetra_report1_config.yaml'
    
    config = yaml.safe_load(open(args.config))
    
    log_config = config['lol']
    
    logging.config.dictConfig(log_config)
    
    logger = logging.getLogger(__name__)
    
    
    
    connection = S3BucketConnector(access_key=config['s3_credentials']['s3_access_key'],
                                   secret_key=config['s3_credentials']['s3_secret_key'],
                                   endpoint_url=config['s3_credentials']['s3_endpoint_url'],
                                   bucket=config['s3_credentials']['s3_bucket_name'])
    
    file_list = connection.list_files_in_prefix(prefix='2022-12-31')
    
    df = connection.read_csv_to_df(file_list[8])
    
    transformer = XetraETL()
    
    transformed_df = transformer.transform_report1(df)
    
    connection.write_df_to_s3(transformed_df,config['s3_credentials']['target_bucket'])
    # configure logging
    
    logger.info("ETL Process Completed")
    

if __name__ == '__main__':
    main()