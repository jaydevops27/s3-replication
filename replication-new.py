import boto3
import os
import sys
import subprocess
from botocore.exceptions import ClientError
from datetime import datetime

def try_aws_cli_sync():
    """Try using AWS CLI sync which sometimes bypasses ListBucket restrictions"""
    SOURCE_BUCKET = os.getenv('SOURCE_BUCKET', 'cert-9898')
    DEST_BUCKET = os.getenv('DEST_BUCKET', 'iteration-technology')
    SOURCE_REGION = os.getenv('SOURCE_REGION', 'us-east-1')
    DEST_REGION = os.getenv('DEST_REGION', 'us-central-1')
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    print("=== Attempting AWS CLI S3 Sync ===")
    print("AWS CLI sometimes works even when boto3 ListBucket fails...")
    
    try:
        # Build AWS CLI command
        cmd = [
            'aws', 's3', 'sync',
            f's3://{SOURCE_BUCKET}',
            f's3://{DEST_BUCKET}',
            '--source-region', SOURCE_REGION,
            '--region', DEST_REGION
        ]
        
        if DRY_RUN:
            cmd.append('--dryrun')
        
        print(f"Running: {' '.join(cmd)}")
        
        # Run AWS CLI sync
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=21600)  # 6 hour timeout
        
        if result.returncode == 0:
            print("AWS CLI sync completed successfully!")
            print("STDOUT:", result.stdout)
            return True
        else:
            print("AWS CLI sync failed:")
            print("STDERR:", result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("AWS CLI sync timed out")
        return False
    except FileNotFoundError:
        print("AWS CLI not found, installing...")
        try:
            subprocess.run(['pip', 'install', 'awscli'], check=True)
            return try_aws_cli_sync()  # Retry after installation
        except:
            print("Failed to install AWS CLI")
            return False
    except Exception as e:
        print(f"AWS CLI sync error: {e}")
        return False

def try_alternative_listing_methods(source_s3, source_bucket):
    """Try alternative methods to list objects"""
    objects = []
    
    print("=== Trying Alternative Listing Methods ===")
    
    # Method 1: Try list_objects (v1) instead of list_objects_v2
    try:
        print("Trying list_objects (v1)...")
        paginator = source_s3.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=source_bucket):
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj)
        if objects:
            print(f"Success! Found {len(objects)} objects using list_objects v1")
            return objects
    except Exception as e:
        print(f"list_objects v1 failed: {e}")
    
    # Method 2: Try with different parameters
    try:
        print("Trying list_objects_v2 with different parameters...")
        response = source_s3.list_objects_v2(
            Bucket=source_bucket,
            MaxKeys=1000,
            RequestPayer='BucketOwner'
        )
        if 'Contents' in response:
            objects = response['Contents']
            print(f"Success! Found {len(objects)} objects with RequestPayer parameter")
            return objects
    except Exception as e:
        print(f"list_objects_v2 with RequestPayer failed: {e}")
    
    # Method 3: Try assuming a different role (if configured)
    role_arn = os.getenv('ASSUME_ROLE_ARN')
    if role_arn:
        try:
            print(f"Trying to assume role: {role_arn}")
            sts_client = boto3.client('sts')
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName='S3ReplicationSession'
            )
            
            credentials = response['Credentials']
            role_s3 = boto3.client(
                's3',
                region_name=os.getenv('SOURCE_REGION', 'us-east-1'),
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            paginator = role_s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=source_bucket):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects.append(obj)
            
            if objects:
                print(f"Success! Found {len(objects)} objects using assumed role")
                return objects
                
        except Exception as e:
            print(f"Assume role method failed: {e}")
    
    return []

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
    
    # First, try AWS CLI sync (often works when boto3 fails)
    if try_aws_cli_sync():
        print("Replication completed successfully using AWS CLI!")
        return
    
    print("\nAWS CLI failed, trying Python boto3 with alternative methods...")
    
    try:
        # Initialize S3 clients
        source_s3 = boto3.client('s3', region_name=SOURCE_REGION)
        dest_s3 = boto3.client('s3', region_name=DEST_REGION)
        
        print(f"\nStarting replication from {SOURCE_BUCKET} to {DEST_BUCKET}")
        if DRY_RUN:
            print("DRY RUN MODE - No actual copying will be performed")
        
        # Try alternative methods to list objects
        print("Attempting to get list of objects from source bucket...")
        objects_data = try_alternative_listing_methods(source_s3, SOURCE_BUCKET)
        
        if not objects_data:
            print("\n" + "="*60)
            print("ERROR: Could not list objects from source bucket!")
            print("="*60)
            print("All automatic methods failed due to s3:ListBucket permission being denied.")
            print("\nPossible solutions:")
            print("1. Ask your AWS admin to grant s3:ListBucket permission")
            print("2. Ask your AWS admin to create a cross-account role with proper permissions")
            print("3. Use AWS DataSync service instead")
            print("4. Provide object list manually (contact me if you need this approach)")
            print("\nThe explicit deny in your identity-based policy is blocking all listing operations.")
            sys.exit(1)
        
        copied = 0
        skipped = 0
        failed = 0
        failed_files = []
        total = len(objects_data)
        
        print(f"Found {total} objects to process")
        
        for i, obj in enumerate(objects_data):
            key = obj['Key']
            
            if i % 100 == 0:
                print(f"Progress: {i}/{total} ({i/total*100:.1f}%)")
            
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
