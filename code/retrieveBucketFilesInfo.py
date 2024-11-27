import boto3
import logging
import pandas as pd
import os
import re

from multiprocessing import Process

logging.basicConfig(level = logging.INFO)
columns = ["bucket", "file", "last_modified", "size"]

directory_path = '.'
bucket_files = os.listdir(directory_path)
existing_buckets = [f.replace('fileinfo_', '').replace('.csv', '') for f in bucket_files if os.path.isfile(os.path.join(directory_path, f)) and ".csv" in f]

def get_latest_file_version(bucket_name):
    latest_version = -1
    for f in bucket_files:
        if (
            os.path.isfile(os.path.join(directory_path, f)) and
            ".csv" in f and
            f"fileinfo_{bucket_name}" == re.sub('\_([0-9])+.csv', '', f)
        ):
            f_version = int(f.replace(f'fileinfo_{bucket_name}_', '').replace('.csv', ''))
            if f_version > latest_version:
                latest_version = f_version

    return latest_version

def bucket_search(bucket_name):
    s3_client = boto3.client('s3')
    try:
        starting_token = None
        token_filename = f"next_continuation_token_{bucket_name}.txt"
        page_no = get_latest_file_version(bucket_name) + 1
        if page_no > 0:
            with open(token_filename, "r+") as token_file:
                starting_token = token_file.read()
        paginator = s3_client.get_paginator('list_objects_v2')
        if starting_token:
            pages = paginator.paginate(
                Bucket=bucket_name,
                PaginationConfig={
                    "StartingToken": starting_token
                },
            )
        else:
            pages = paginator.paginate(Bucket=bucket_name)
        bucket_results = list()
        results = 0
        page_size = 500000
        next_continuation_token = None
        for page in pages:
            if not 'Contents' in page:
                logging.info(f"{bucket_name}   : No files found.")
                return
            if "NextContinuationToken" in page:
                next_continuation_token = page["NextContinuationToken"]
            for object in page['Contents']:
                object_name = object["Key"]
                last_modified = object["LastModified"]
                size = object["Size"]
                bucket_results.append([bucket_name, object_name, last_modified, size])
                results += 1
            if results - page_size == 0:
                df = pd.DataFrame(bucket_results, columns=columns)
                filename = f"fileinfo_{bucket_name}_{page_no}.csv"
                df.to_csv(filename, index_label="index")
                logging.info(f"{bucket_name}   : file {filename} written with {results + page_no * page_size} entries.")
                bucket_results.clear()
                results = 0
                page_no += 1
                with open(token_filename, "w") as token_file:
                    token_file.write(next_continuation_token)

        filename = f"fileinfo_{bucket_name}_{page_no + 1}.csv"
        df = pd.DataFrame(bucket_results, columns=columns)
        df.to_csv(filename, index_label="index")
        logging.info(f"{bucket_name}   : file {filename} written with {results + page_no * page_size} entries.")
        bucket_results.clear()
        logging.info(f"{bucket_name}   : finished processing.")
    except Exception as e:
        filename = f"fileinfo_{bucket_name}_{page_no + 1}_exception.csv"
        logging.error(f"{bucket_name}    : exception {str(e)}.")
        if next_continuation_token:
            logging.error(f"Resume token for {bucket_name}:   {next_continuation_token}")
            with open(token_filename, "w") as token_file:
                token_file.write(next_continuation_token)
        bucket_results.clear()

if __name__ == '__main__':
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()
    processes = dict()
    for bucket in response['Buckets']:
        bucket_name = bucket["Name"]
        if bucket_name in existing_buckets:
            continue
        logging.info("Main    : create and start process %s.", bucket_name)
        thread = Process(target=bucket_search, args=(bucket_name,))
        processes[bucket_name] = thread
        thread.start()
    
    for bucket_name in processes:
        logging.info("Main    : joining thread %s.", bucket_name)
        processes[bucket_name].join()
        logging.info("Main    : thread %s done.", bucket_name)
