import boto3
import os
import sys
from botocore.exceptions import ClientError
from datetime import datetime

def main():
    # Get configuration from environment variables
    SOURCE_BUCKET = os.getenv('SOURCE_BUCKET', 'cert-9898')
    DEST_BUCKET = os.getenv('DEST_BUCKET', 'iteration-technology')
    SOURCE_REGION = os.getenv('SOURCE_REGION', 'us-east-1')
    DEST_REGION = os.getenv('DEST_REGION', 'us-central-1')
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    # Print configuration
    print(f"SOURCE_BUCKET: {SOURCE_BUCKET}")
    print(f"DEST_BUCKET: {DEST_BUCKET}")
    print(f"SOURCE_REGION: {SOURCE_REGION}")
    print(f"DEST_REGION: {DEST_REGION}")
    print(f"DRY_RUN: {DRY_RUN}")
    
    start_time = datetime.now()
    
    try:
        # Initialize S3 clients
        source_s3 = boto3.client('s3', region_name=SOURCE_REGION)
        dest_s3 = boto3.client('s3', region_name=DEST_REGION)
        
        print(f"\nStarting replication from {SOURCE_BUCKET} to {DEST_BUCKET}")
        if DRY_RUN:
            print("DRY RUN MODE - No actual copying will be performed")
        
        # Get all objects from source bucket
        print("Getting list of objects from source bucket...")
        paginator = source_s3.get_paginator('list_objects_v2')
        
        copied = 0
        skipped = 0
        failed = 0
        failed_files = []
        total = 0
        
        for page in paginator.paginate(Bucket=SOURCE_BUCKET):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                total += 1
                
                try:
                    # Check if object already exists in destination
                    try:
                        dest_s3.head_object(Bucket=DEST_BUCKET, Key=key)
                        print(f"SKIP: {key} (already exists)")
                        skipped += 1
                        continue
                    except ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            pass  # Object doesn't exist, proceed with copy
                        else:
                            raise e
                    
                    if DRY_RUN:
                        print(f"DRY RUN: Would copy {key}")
                        copied += 1
                        continue
                    
                    # Verify source object exists before copying
                    try:
                        source_s3.head_object(Bucket=SOURCE_BUCKET, Key=key)
                    except ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            print(f"FAIL: {key} - Source object not found")
                            failed += 1
                            failed_files.append(key)
                            continue
                        else:
                            raise e
                    
                    # Copy object
                    copy_source = {'Bucket': SOURCE_BUCKET, 'Key': key}
                    dest_s3.copy_object(CopySource=copy_source, Bucket=DEST_BUCKET, Key=key)
                    print(f"COPY: {key}")
                    copied += 1
                    
                except Exception as e:
                    print(f"FAIL: {key} - {e}")
                    failed += 1
                    failed_files.append(key)
        
        # Summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n=== REPLICATION SUMMARY ===")
        print(f"Duration: {duration}")
        print(f"Total objects: {total}")
        print(f"Copied: {copied}")
        print(f"Skipped: {skipped}")
        print(f"Failed: {failed}")
        
        if failed_files:
            print(f"\nFailed files:")
            for file in failed_files:
                print(f"  - {file}")
        
        # Only exit with error if significant failures
        if failed > 0:
            failure_rate = failed / total if total > 0 else 0
            if failure_rate > 0.1:  # More than 10% failed
                print(f"\nERROR: High failure rate ({failure_rate:.1%})")
                sys.exit(1)
            else:
                print(f"\nWARNING: Some files failed ({failure_rate:.1%}) but continuing")
        
        print("Replication completed successfully")
            
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
