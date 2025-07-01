import boto3
import os
import sys
import itertools
import string
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Thread-safe counters
lock = threading.Lock()
found_objects = []
copied = 0
skipped = 0
failed = 0

def try_copy_object(source_s3, dest_s3, source_bucket, dest_bucket, key, dry_run):
    """Try to copy a single object if it exists"""
    global copied, skipped, failed
    
    try:
        # First check if object exists in source by trying to get metadata
        try:
            source_s3.head_object(Bucket=source_bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None  # Object doesn't exist, skip silently
            else:
                raise e
        
        # Object exists! Add to found list
        with lock:
            found_objects.append(key)
            print(f"FOUND: {key}")
        
        # Check if already exists in destination
        try:
            dest_s3.head_object(Bucket=dest_bucket, Key=key)
            with lock:
                skipped += 1
            print(f"SKIP: {key} (already exists in destination)")
            return "skipped"
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise e
        
        if dry_run:
            with lock:
                copied += 1
            print(f"DRY RUN: Would copy {key}")
            return "would_copy"
        
        # Copy the object
        copy_source = {'Bucket': source_bucket, 'Key': key}
        dest_s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=key)
        
        with lock:
            copied += 1
        print(f"COPIED: {key}")
        return "copied"
        
    except Exception as e:
        with lock:
            failed += 1
        print(f"ERROR with {key}: {e}")
        return "error"

def generate_common_patterns():
    """Generate common file patterns to try"""
    patterns = []
    
    # Common file extensions
    extensions = [
        'txt', 'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
        'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg',
        'mp4', 'avi', 'mov', 'wmv', 'flv',
        'mp3', 'wav', 'flac', 'aac',
        'zip', 'rar', '7z', 'tar', 'gz',
        'json', 'xml', 'csv', 'log',
        'html', 'css', 'js', 'py', 'java', 'cpp', 'c',
        'sql', 'db', 'sqlite',
        'backup', 'bak', 'tmp'
    ]
    
    # Common folder names
    folders = [
        'documents', 'docs', 'files', 'data', 'backup', 'backups',
        'images', 'img', 'photos', 'pictures', 'media',
        'videos', 'audio', 'music',
        'reports', 'exports', 'imports', 'uploads', 'downloads',
        'archive', 'archives', 'old', 'temp', 'tmp',
        'logs', 'config', 'configs', 'settings',
        'public', 'private', 'shared', 'common',
        'assets', 'resources', 'static',
        'db', 'database', 'sql'
    ]
    
    # Year patterns (last 5 years)
    current_year = datetime.now().year
    years = [str(year) for year in range(current_year - 5, current_year + 1)]
    
    # Month patterns
    months = [f"{i:02d}" for i in range(1, 13)]
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                   'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
                   'january', 'february', 'march', 'april', 'may', 'june',
                   'july', 'august', 'september', 'october', 'november', 'december']
    
    # Common file name patterns
    common_names = [
        'index', 'main', 'default', 'readme', 'config', 'settings',
        'data', 'backup', 'export', 'import', 'report', 'log',
        'test', 'sample', 'example', 'template', 'copy',
        'file', 'document', 'image', 'photo', 'video'
    ]
    
    # Generate patterns
    
    # 1. Root level files with common names and extensions
    for name in common_names:
        for ext in extensions:
            patterns.append(f"{name}.{ext}")
    
    # 2. Files in common folders
    for folder in folders:
        for name in common_names:
            for ext in extensions:
                patterns.append(f"{folder}/{name}.{ext}")
                patterns.append(f"{folder}/{name}_1.{ext}")
                patterns.append(f"{folder}/{name}_2.{ext}")
    
    # 3. Date-based patterns
    for year in years:
        for month in months:
            for ext in extensions:
                patterns.append(f"{year}/{month}/data.{ext}")
                patterns.append(f"{year}-{month}/backup.{ext}")
                patterns.append(f"backup/{year}/{month}.{ext}")
                patterns.append(f"reports/{year}-{month}.{ext}")
    
    # 4. Numbered files
    for i in range(1, 101):  # Try numbers 1-100
        for ext in extensions:
            patterns.append(f"file{i:03d}.{ext}")
            patterns.append(f"document{i}.{ext}")
            patterns.append(f"backup{i}.{ext}")
    
    # 5. Common AWS/application patterns
    aws_patterns = [
        'aws-logs/cloudtrail.json',
        'logs/application.log',
        'configs/app.json',
        'static/assets/style.css',
        'uploads/user_data.csv',
        'backups/db_backup.sql',
        'exports/data_export.xlsx'
    ]
    patterns.extend(aws_patterns)
    
    return patterns

def generate_discovery_patterns():
    """Generate patterns to discover actual file structure"""
    discovery_patterns = []
    
    # Try common single character folder names
    for char in string.ascii_lowercase + string.digits:
        discovery_patterns.append(f"{char}/")
    
    # Try two character combinations
    for char1 in string.ascii_lowercase[:5]:  # Limit to first 5 letters
        for char2 in string.ascii_lowercase[:5]:
            discovery_patterns.append(f"{char1}{char2}/")
    
    return discovery_patterns

def smart_object_discovery(source_s3, source_bucket, max_workers=20):
    """Try to discover objects using pattern matching"""
    
    print("Starting smart object discovery...")
    
    # Get user-defined patterns
    user_extensions = os.getenv('FILE_EXTENSIONS', '').split(',')
    user_extensions = [ext.strip().lower() for ext in user_extensions if ext.strip()]
    
    user_folders = os.getenv('FOLDER_PATTERNS', '').split(',')
    user_folders = [folder.strip() for folder in user_folders if folder.strip()]
    
    user_prefixes = os.getenv('KEY_PREFIXES', '').split(',')
    user_prefixes = [prefix.strip() for prefix in user_prefixes if prefix.strip()]
    
    # Generate all patterns to try
    patterns = generate_common_patterns()
    
    # Add user-defined patterns
    if user_extensions:
        print(f"Using user-defined extensions: {user_extensions}")
        for folder in (user_folders if user_folders else ['', 'data', 'files']):
            for ext in user_extensions:
                for name in ['data', 'file', 'document', 'backup', 'export']:
                    if folder:
                        patterns.append(f"{folder}/{name}.{ext}")
                    else:
                        patterns.append(f"{name}.{ext}")
    
    if user_prefixes:
        print(f"Using user-defined prefixes: {user_prefixes}")
        for prefix in user_prefixes:
            for ext in ['txt', 'json', 'csv', 'log', 'pdf']:
                patterns.append(f"{prefix}.{ext}")
                patterns.append(f"{prefix}/data.{ext}")
    
    print(f"Testing {len(patterns)} potential object patterns...")
    
    # Test patterns in parallel
    found_keys = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pattern = {}
        
        for pattern in patterns:
            future = executor.submit(try_object_exists, source_s3, source_bucket, pattern)
            future_to_pattern[future] = pattern
        
        processed = 0
        for future in as_completed(future_to_pattern):
            pattern = future_to_pattern[future]
            processed += 1
            
            if processed % 500 == 0:
                print(f"Tested {processed}/{len(patterns)} patterns, found {len(found_keys)} objects")
            
            try:
                exists = future.result()
                if exists:
                    found_keys.append(pattern)
                    print(f"DISCOVERED: {pattern}")
            except Exception as e:
                pass  # Ignore errors, just means object doesn't exist
    
    print(f"Discovery complete. Found {len(found_keys)} objects.")
    return found_keys

def try_object_exists(s3_client, bucket, key):
    """Check if an object exists without listing"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            # For other errors, assume it doesn't exist
            return False
    except:
        return False

def main():
    global copied, skipped, failed, found_objects
    
    # Get configuration
    SOURCE_BUCKET = os.getenv('SOURCE_BUCKET', 'cert-9898')
    DEST_BUCKET = os.getenv('DEST_BUCKET', 'iteration-technology')
    SOURCE_REGION = os.getenv('SOURCE_REGION', 'us-east-1')
    DEST_REGION = os.getenv('DEST_REGION', 'us-central-1')
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '20'))
    
    print(f"SOURCE_BUCKET: {SOURCE_BUCKET}")
    print(f"DEST_BUCKET: {DEST_BUCKET}")
    print(f"SOURCE_REGION: {SOURCE_REGION}")
    print(f"DEST_REGION: {DEST_REGION}")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"MAX_WORKERS: {MAX_WORKERS}")
    
    print("\n" + "="*60)
    print("SMART S3 REPLICATION WITHOUT LISTBUCKET")
    print("="*60)
    print("This script discovers objects by testing common patterns")
    print("since s3:ListBucket permission is denied.")
    print()
    print("Environment variables you can set:")
    print("FILE_EXTENSIONS=pdf,docx,xlsx,txt")
    print("FOLDER_PATTERNS=documents,reports,data")
    print("KEY_PREFIXES=backup,export,report")
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # Initialize S3 clients
        source_s3 = boto3.client('s3', region_name=SOURCE_REGION)
        dest_s3 = boto3.client('s3', region_name=DEST_REGION)
        
        # Discover objects using smart patterns
        discovered_keys = smart_object_discovery(source_s3, SOURCE_BUCKET, MAX_WORKERS)
        
        if not discovered_keys:
            print("\nNo objects discovered using pattern matching!")
            print("\nTo improve discovery, set these environment variables:")
            print("FILE_EXTENSIONS=pdf,docx,xlsx,txt,csv,json")
            print("FOLDER_PATTERNS=documents,data,reports,backup")
            print("KEY_PREFIXES=backup,export,file,document")
            print("\nExample: If you know you have PDFs in a 'reports' folder:")
            print("FILE_EXTENSIONS=pdf")
            print("FOLDER_PATTERNS=reports")
            return 1
        
        print(f"\nStarting replication of {len(discovered_keys)} discovered objects...")
        
        # Reset counters
        copied = skipped = failed = 0
        
        # Copy discovered objects
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for key in discovered_keys:
                future = executor.submit(
                    try_copy_object, 
                    source_s3, dest_s3, SOURCE_BUCKET, DEST_BUCKET, key, DRY_RUN
                )
                futures.append(future)
            
            for i, future in enumerate(as_completed(futures)):
                if i % 10 == 0:
                    print(f"Processed {i}/{len(discovered_keys)} objects")
                
                try:
                    future.result()
                except Exception as e:
                    print(f"Unexpected error: {e}")
        
        # Summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n=== SMART REPLICATION SUMMARY ===")
        print(f"Duration: {duration}")
        print(f"Objects discovered: {len(discovered_keys)}")
        print(f"Copied: {copied}")
        print(f"Skipped: {skipped}")
        print(f"Failed: {failed}")
        
        # Save discovered objects list for future use
        if discovered_keys:
            with open('discovered_objects.txt', 'w') as f:
                for key in discovered_keys:
                    f.write(f"{key}\n")
            print(f"Discovered objects saved to discovered_objects.txt")
        
        if failed > 0:
            failure_rate = failed / len(discovered_keys) if discovered_keys else 0
            if failure_rate > 0.1:
                print(f"\nERROR: High failure rate ({failure_rate:.1%})")
                return 1
            else:
                print(f"\nWARNING: Some objects failed ({failure_rate:.1%})")
        
        print("Smart replication completed!")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
