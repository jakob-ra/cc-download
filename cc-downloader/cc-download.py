import boto3
import os
import argparse
import awswrangler as wr
from utils import exponential_backoff
import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, required=True)
    parser.add_argument("--batches_per_partition", type=int, required=True)
    parser.add_argument("--output_bucket", type=str, required=True)
    parser.add_argument("--result_output_path", type=str, required=True)
    args = parser.parse_args()

    if "AWS_BATCH_JOB_ARRAY_INDEX" in os.environ:
        batch_n = os.environ['AWS_BATCH_JOB_ARRAY_INDEX']
        batch_n = int(batch_n)
        print(f'Processing batch {batch_n}.')
    else:
        batch_n = 0
        print('Processing first batch (no array index found).')

    session = boto3.Session(region_name='us-east-1')

    sts = session.client("sts")
    print(sts.get_caller_identity())

    # read cc-index table with warc filenames and byte positions
    partition_n = batch_n//args.batches_per_partition + 1
    batch_n_within_partition = batch_n % args.batches_per_partition
    query = f"""SELECT * FROM urls_merged_cc_to_download 
    ORDER BY crawl, url_host_tld, fetch_time 
    OFFSET {batch_n_within_partition * args.batch_size} LIMIT {args.batch_size}"""

    df = exponential_backoff(wr.athena.read_sql_query, sql=query, database='ccindex', boto3_session=session)
    assert len(df) > 1, "Empty input table!"

    keywords = pd.read_csv(f's3://{args.output_bucket}/keywords/url_keywords.txt',
                           header=None).squeeze().to_list()

    output_path = f's3://{args.output_bucket}/{args.result_output_path}/batch_n_{batch_n}.parquet'

    # initialize s3
    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

    # download processing script from s3
    s3client.download_file(args.output_bucket, 'scripts/process_page.py', 'process_page_script.py')
    from process_page_script import CCDownloader

    cc_downloader = CCDownloader(df, s3client, output_path, keywords=keywords)
    cc_downloader.run()



