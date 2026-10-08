"""
MinIO Client Helper Module
Provides reusable functions for interacting with MinIO object storage
"""

import os
import logging
from io import StringIO
from minio import Minio
from minio.error import S3Error
import pandas as pd

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MinIO configuration from environment variables with defaults
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'

def get_minio_client():
    """
    Create and return a MinIO client
    """
    try:
        from urllib3 import disable_warnings
        disable_warnings()
        
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        logger.info(f"MinIO client created successfully for endpoint: {MINIO_ENDPOINT}")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        return None

def upload_file(bucket, object_name, file_path):
    """
    Upload a local file to MinIO
    
    Args:
        bucket: Target bucket name
        object_name: Object name in MinIO (will mirror local path)
        file_path: Local file path to upload
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = get_minio_client()
        if client is None:
            return False
        
        # Check if bucket exists, create if not
        if not client.bucket_exists(bucket):
            logger.info(f"Bucket {bucket} does not exist, creating it...")
            client.make_bucket(bucket)
        
        # Upload file
        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=file_path
        )
        
        # Get file size for logging
        file_size = os.path.getsize(file_path)
        logger.info(f"Uploaded {file_path} to {bucket}/{object_name} ({file_size} bytes)")
        return True
        
    except S3Error as e:
        logger.error(f"MinIO S3 error during upload: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during file upload: {e}")
        return False

def upload_dataframe(bucket, object_name, df):
    """
    Upload a pandas DataFrame directly as CSV to MinIO (in-memory, no temp file)
    
    Args:
        bucket: Target bucket name
        object_name: Object name in MinIO
        df: Pandas DataFrame to upload
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = get_minio_client()
        if client is None:
            return False
        
        # Check if bucket exists, create if not
        if not client.bucket_exists(bucket):
            logger.info(f"Bucket {bucket} does not exist, creating it...")
            client.make_bucket(bucket)
        
        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode('utf-8')
        csv_buffer.seek(0)
        
        # Upload as object
        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=csv_buffer,
            length=len(csv_data),
            content_type='text/csv'
        )
        
        logger.info(f"Uploaded DataFrame to {bucket}/{object_name} ({len(csv_data)} bytes)")
        return True
        
    except S3Error as e:
        logger.error(f"MinIO S3 error during DataFrame upload: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during DataFrame upload: {e}")
        return False

def download_file(bucket, object_name, destination_path):
    """
    Download a file from MinIO
    
    Args:
        bucket: Source bucket name
        object_name: Object name in MinIO
        destination_path: Local path to save the file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = get_minio_client()
        if client is None:
            return False
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        
        # Download file
        client.fget_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=destination_path
        )
        
        logger.info(f"Downloaded {bucket}/{object_name} to {destination_path}")
        return True
        
    except S3Error as e:
        logger.error(f"MinIO S3 error during download: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during file download: {e}")
        return False

def list_objects(bucket, prefix=''):
    """
    List objects in a MinIO bucket with optional prefix filter
    
    Args:
        bucket: Bucket name
        prefix: Optional prefix to filter objects
    
    Returns:
        list: List of object names, or None if error
    """
    try:
        client = get_minio_client()
        if client is None:
            return None
        
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        object_names = [obj.object_name for obj in objects]
        
        logger.info(f"Found {len(object_names)} objects in bucket {bucket} with prefix '{prefix}'")
        return object_names
        
    except S3Error as e:
        logger.error(f"MinIO S3 error during list operation: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during list operation: {e}")
        return None

def delete_object(bucket, object_name):
    """
    Delete an object from MinIO
    
    Args:
        bucket: Bucket name
        object_name: Object name to delete
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = get_minio_client()
        if client is None:
            return False
        
        client.remove_object(bucket, object_name)
        logger.info(f"Deleted {bucket}/{object_name}")
        return True
        
    except S3Error as e:
        logger.error(f"MinIO S3 error during delete: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during delete: {e}")
        return False

def bucket_exists(bucket):
    """
    Check if a bucket exists
    
    Args:
        bucket: Bucket name to check
    
    Returns:
        bool: True if bucket exists, False otherwise
    """
    try:
        client = get_minio_client()
        if client is None:
            return False
        
        return client.bucket_exists(bucket)
        
    except Exception as e:
        logger.error(f"Error checking bucket existence: {e}")
        return False
