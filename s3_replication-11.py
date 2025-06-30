import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
from typing import List, Dict, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_replication.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class S3Replicator:
    def __init__(self, 
                 source_bucket: str, 
                 dest_bucket: str,
                 source_region: str = None,
                 dest_region: str = None,
                 aws_access_key: Optional[str] = None,
                 aws_secret_key: Optional[str] = None):
        
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        
        # Create session
        if aws_access_key and aws_secret_key:
            self.session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            self.session = boto3.Session()  # Use default credential chain
        
        # Set regions (try auto-detect if not provided, fallback to defaults)
        self.source_region = source_region
        self.dest_region = dest_region
        
        if not self.source_region or not self.dest_region:
            logger.info("Attempting to auto-detect bucket regions...")
            try:
                s3_default = self.session.client('s3')
                
                if not self.source_region:
                    try:
                        source_location = s3_default.get_bucket_location(Bucket=source_bucket)
                        self.source_region = source_location.get('LocationConstraint') or 'us-east-1'
                        logger.info(f"Auto-detected source region: {self.source_region}")
                    except ClientError as e:
                        logger.warning(f"Cannot auto-detect source region: {e}")
                        self.source_region = 'us-east-1'  # Default fallback
                        logger.info(f"Using default source region: {self.source_region}")
                
                if not self.dest_region:
                    try:
                        dest_location = s3_default.get_bucket_location(Bucket=dest_bucket)
                        self.dest_region = dest_location.get('LocationConstraint') or 'us-east-1'
                        logger.info(f"Auto-detected destination region: {self.dest_region}")
                    except ClientError as e:
                        logger.warning(f"Cannot auto-detect destination region: {e}")
                        self.dest_region = 'us-east-1'  # Default fallback
                        logger.info(f"Using default destination region: {self.dest_region}")
                        
            except Exception as e:
                logger.warning(f"Region auto-detection failed: {e}")
                self.source_region = self.source_region or 'us-east-1'
                self.dest_region = self.dest_region or 'us-east-1'
                logger.info(f"Using fallback regions - Source: {self.source_region}, Dest: {self.dest_region}")
        
        try:
            # Create S3 clients with specified/detected regions
            self.source_s3 = self.session.client('s3', region_name=self.source_region)
            self.dest_s3 = self.session.client('s3', region_name=self.dest_region)
            
            logger.info(f"S3 clients initialized - Source: {self.source_region}, Dest: {self.dest_region}")
            
            # Test connectivity without requiring GetBucketLocation
            logger.info("Testing S3 connectivity...")
            
            # Test source bucket access
            try:
                self.source_s3.head_bucket(Bucket=source_bucket)
                logger.info(f"✅ Source bucket '{source_bucket}' accessible")
            except ClientError as e:
                if e.response['Error']['Code'] in ['301', 'PermanentRedirect']:
                    logger.warning(f"⚠️ Source bucket region mismatch. Try setting SOURCE_REGION environment variable")
                    # Try common regions
                    for region in ['us-west-2', 'us-west-1', 'eu-west-1', 'ap-southeast-1']:
                        try:
                            test_s3 = self.session.client('s3', region_name=region)
                            test_s3.head_bucket(Bucket=source_bucket)
                            self.source_region = region
                            self.source_s3 = test_s3
                            logger.info(f"✅ Found source bucket in region: {region}")
                            break
                        except:
                            continue
                    else:
                        raise e
                else:
                    raise e
            
            # Test destination bucket access
            try:
                self.dest_s3.head_bucket(Bucket=dest_bucket)
                logger.info(f"✅ Destination bucket '{dest_bucket}' accessible")
            except ClientError as e:
                if e.response['Error']['Code'] in ['301', 'PermanentRedirect']:
                    logger.warning(f"⚠️ Destination bucket region mismatch. Try setting DEST_REGION environment variable")
                    # Try common regions
                    for region in ['us-west-2', 'us-west-1', 'eu-west-1', 'ap-southeast-1']:
                        try:
                            test_s3 = self.session.client('s3', region_name=region)
                            test_s3.head_bucket(Bucket=dest_bucket)
                            self.dest_region = region
                            self.dest_s3 = test_s3
                            logger.info(f"✅ Found destination bucket in region: {region}")
                            break
                        except:
                            continue
                    else:
                        raise e
                else:
                    raise e
            
        except ClientError as e:
            logger.error(f"Error initializing S3 clients: {e}")
            raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
    
    def get_bucket_objects(self, bucket: str, s3_client) -> List[Dict]:
        """Get all objects from a bucket"""
        objects = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
            logger.info(f"Found {len(objects)} objects in bucket {bucket}")
            return objects
        except ClientError as e:
            logger.error(f"Error listing objects in bucket {bucket}: {e}")
            return []
    
    def object_exists_and_matches(self, key: str, source_etag: str, source_size: int) -> bool:
        """Check if object exists in destination and matches source"""
        try:
            response = self.dest_s3.head_object(Bucket=self.dest_bucket, Key=key)
            dest_etag = response.get('ETag', '')
            dest_size = response.get('ContentLength', 0)
            
            # Compare ETag and size
            if dest_etag == source_etag and dest_size == source_size:
                logger.debug(f"Object {key} already exists and matches source")
                return True
            else:
                logger.info(f"Object {key} exists but differs (ETag: {dest_etag} vs {source_etag}, Size: {dest_size} vs {source_size})")
                return False
                
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.debug(f"Object {key} does not exist in destination")
                return False
            else:
                logger.error(f"Error checking object {key}: {e}")
                return False
    
    def copy_object(self, obj: Dict) -> Tuple[str, bool, str]:
        """Copy a single object from source to destination"""
        key = obj['Key']
        source_etag = obj.get('ETag', '')
        source_size = obj.get('Size', 0)
        
        try:
            # Check if object already exists and matches
            if self.object_exists_and_matches(key, source_etag, source_size):
                return key, True, "Already exists and matches"
            
            # Copy object using copy_object method (server-side copy)
            copy_source = {
                'Bucket': self.source_bucket,
                'Key': key
            }
            
            # Get object metadata from source
            source_metadata = self.source_s3.head_object(Bucket=self.source_bucket, Key=key)
            
            # Prepare copy arguments
            copy_args = {
                'CopySource': copy_source,
                'Bucket': self.dest_bucket,
                'Key': key,
                'MetadataDirective': 'COPY'
            }
            
            # Handle server-side encryption if present
            if 'ServerSideEncryption' in source_metadata:
                copy_args['ServerSideEncryption'] = source_metadata['ServerSideEncryption']
            
            # Perform the copy
            self.dest_s3.copy_object(**copy_args)
            
            # Verify the copy
            if self.object_exists_and_matches(key, source_etag, source_size):
                logger.info(f"Successfully copied {key}")
                return key, True, "Copied successfully"
            else:
                logger.error(f"Copy verification failed for {key}")
                return key, False, "Copy verification failed"
                
        except ClientError as e:
            error_msg = f"Failed to copy {key}: {e}"
            logger.error(error_msg)
            return key, False, error_msg
    
    def replicate_bucket(self, max_workers: int = 10, dry_run: bool = False) -> Dict:
        """Replicate all objects from source to destination bucket"""
        start_time = datetime.now()
        logger.info(f"Starting replication from {self.source_bucket} ({self.source_region}) to {self.dest_bucket} ({self.dest_region})")
        
        if dry_run:
            logger.info("DRY RUN MODE - No actual copying will be performed")
        
        # Get all objects from source bucket
        source_objects = self.get_bucket_objects(self.source_bucket, self.source_s3)
        
        if not source_objects:
            logger.warning("No objects found in source bucket")
            return {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0}
        
        results = {
            'total': len(source_objects),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'failed_objects': []
        }
        
        if dry_run:
            # In dry run, just check what would be copied
            for obj in source_objects:
                key = obj['Key']
                if self.object_exists_and_matches(key, obj.get('ETag', ''), obj.get('Size', 0)):
                    results['skipped'] += 1
                    logger.info(f"DRY RUN: Would skip {key} (already exists)")
                else:
                    results['success'] += 1
                    logger.info(f"DRY RUN: Would copy {key}")
        else:
            # Actual replication with threading
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_object = {executor.submit(self.copy_object, obj): obj for obj in source_objects}
                
                for future in as_completed(future_to_object):
                    obj = future_to_object[future]
                    try:
                        key, success, message = future.result()
                        if success:
                            if "Already exists" in message:
                                results['skipped'] += 1
                            else:
                                results['success'] += 1
                        else:
                            results['failed'] += 1
                            results['failed_objects'].append({'key': key, 'error': message})
                    except Exception as e:
                        results['failed'] += 1
                        error_msg = f"Unexpected error processing {obj['Key']}: {e}"
                        logger.error(error_msg)
                        results['failed_objects'].append({'key': obj['Key'], 'error': error_msg})
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"Replication completed in {duration}")
        logger.info(f"Results: Total: {results['total']}, Success: {results['success']}, Failed: {results['failed']}, Skipped: {results['skipped']}")
        
        if results['failed_objects']:
            logger.error("Failed objects:")
            for failed_obj in results['failed_objects']:
                logger.error(f"  {failed_obj['key']}: {failed_obj['error']}")
        
        return results

def main():
    # Configuration - these should be set via environment variables in GitLab
    SOURCE_BUCKET = os.getenv('SOURCE_BUCKET', 'cert-9898')
    DEST_BUCKET = os.getenv('DEST_BUCKET', 'iteration-technology')
    SOURCE_REGION = os.getenv('SOURCE_REGION')  # Optional - will auto-detect or use default
    DEST_REGION = os.getenv('DEST_REGION')     # Optional - will auto-detect or use default
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '10'))
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    # AWS credentials (preferably from environment variables or IAM role)
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    logger.info("=== S3 REPLICATION STARTING ===")
    logger.info(f"Source bucket: {SOURCE_BUCKET}")
    logger.info(f"Destination bucket: {DEST_BUCKET}")
    logger.info(f"Source region: {SOURCE_REGION or 'auto-detect'}")
    logger.info(f"Destination region: {DEST_REGION or 'auto-detect'}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Dry run: {DRY_RUN}")
    
    try:
        replicator = S3Replicator(
            source_bucket=SOURCE_BUCKET,
            dest_bucket=DEST_BUCKET,
            source_region=SOURCE_REGION,
            dest_region=DEST_REGION,
            aws_access_key=AWS_ACCESS_KEY,
            aws_secret_key=AWS_SECRET_KEY
        )
        
        results = replicator.replicate_bucket(max_workers=MAX_WORKERS, dry_run=DRY_RUN)
        
        # Exit with error code if there were failures
        if results['failed'] > 0:
            logger.error(f"Replication completed with {results['failed']} failures")
            sys.exit(1)
        else:
            logger.info("Replication completed successfully")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
