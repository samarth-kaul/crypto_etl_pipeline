# import boto3
# import pandas as pd
# import logging
# from botocore.exceptions import ClientError

# def load_to_s3(df, bucket_name, s3_key, region):
#     """Load DataFrame to S3 as Parquet."""
#     try:
#         # Initialize S3 client
#         s3_client = boto3.client("s3", region_name=region)
        
#         # Convert DataFrame to Parquet
#         parquet_buffer = df.to_parquet(index=False)
        
#         # Upload to S3
#         s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer)
#         logging.info(f"Data saved to s3://{bucket_name}/{s3_key}")
    
#     except ClientError as e:
#         logging.error(f"Failed to upload to S3: {str(e)}")
#         raise



import boto3
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataLoader:
    """Handles loading data to S3 as Parquet files."""
    
    def __init__(self, s3_config: Dict):
        self.bucket_name = s3_config['bucket_name']
        self.prefix = s3_config['prefix']
        self.s3_client = boto3.client('s3')
    
    def load(self, df: pd.DataFrame):
        """Save DataFrame as Parquet to S3."""
        try:
            # Generate timestamped file path
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            s3_key = f"{self.prefix}crypto_data_{timestamp}.parquet"
            
            # Save DataFrame to Parquet in memory
            buffer = df.to_parquet(index=False, engine='pyarrow')
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=buffer
            )
            logger.info(f"Successfully loaded data to s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to load data to S3: {e}")
            raise