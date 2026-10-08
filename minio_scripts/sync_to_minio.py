"""
MinIO Sync Script
Scans all three local data folders (bronze, silver, gold) and uploads every CSV file
to its corresponding MinIO bucket. Can be run manually at any time to force a full re-sync.
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minio_client import upload_file

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_to_minio.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Data directories - use absolute path for reliability
DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")

# Bucket mappings
BUCKET_MAPPING = {
    BRONZE_DIR: "bronze",
    SILVER_DIR: "silver",
    GOLD_DIR: "gold"
}

def scan_directory(directory):
    """
    Scan a directory for CSV files
    
    Args:
        directory: Directory path to scan
    
    Returns:
        list: List of CSV file paths
    """
    csv_files = []
    
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return csv_files
    
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_files.append(os.path.join(directory, filename))
    
    return csv_files

def sync_file_to_minio(file_path, bucket):
    """
    Sync a single file to MinIO
    
    Args:
        file_path: Local file path
        bucket: Target bucket name
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        object_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        logger.info(f"Uploading: {object_name} -> {bucket}/ ({file_size} bytes)")
        
        success = upload_file(bucket, object_name, file_path)
        
        if success:
            logger.info(f"✓ Successfully uploaded: {object_name}")
        else:
            logger.error(f"✗ Failed to upload: {object_name}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error syncing {file_path}: {e}")
        return False

def sync_directory_to_minio(directory, bucket):
    """
    Sync all CSV files in a directory to MinIO
    
    Args:
        directory: Directory path
        bucket: Target bucket name
    
    Returns:
        tuple: (success_count, failure_count)
    """
    logger.info(f"Scanning directory: {directory}")
    
    csv_files = scan_directory(directory)
    
    if not csv_files:
        logger.info(f"No CSV files found in {directory}")
        return 0, 0
    
    logger.info(f"Found {len(csv_files)} CSV files")
    
    success_count = 0
    failure_count = 0
    
    for file_path in csv_files:
        if sync_file_to_minio(file_path, bucket):
            success_count += 1
        else:
            failure_count += 1
    
    return success_count, failure_count

def main():
    """
    Main sync function
    """
    logger.info("=" * 60)
    logger.info("MINIO SYNC SCRIPT")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_success = 0
    total_failure = 0
    
    # Sync each data directory
    for directory, bucket in BUCKET_MAPPING.items():
        logger.info(f"\nSyncing {directory} to bucket '{bucket}'")
        logger.info("-" * 60)
        
        success, failure = sync_directory_to_minio(directory, bucket)
        total_success += success
        total_failure += failure
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("SYNC SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files uploaded successfully: {total_success}")
    logger.info(f"Total files failed: {total_failure}")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    if total_failure > 0:
        logger.warning("Some files failed to sync. Check the log for details.")
        sys.exit(1)
    else:
        logger.info("All files synced successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
