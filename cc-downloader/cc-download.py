import boto3
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
import os
import argparse
import awswrangler as wr
from utils import exponential_backoff

def fetch_process_warc_records(row, s3client):
    """Fetch all WARC records defined by filenames and offsets in batch,
    parse the records and the contained HTML, return all paragraphs containing at least one of the
    keywords.csv"""
    
    """ New - keywords argument "full" returns full text rather than just areas around some keywords
        and now we get both things in header and paragraph tags"""

    warc_path, offset, end = row.warc_filename, str(row.warc_record_offset), str(row.warc_record_end)

    rangereq = 'bytes={}-{}'.format(offset, end)

    response = exponential_backoff(s3client.get_object, Bucket='commoncrawl', Key=warc_path, Range=rangereq)

    record_stream = BytesIO(response["Body"].read())

    extracts = []
    
    for record in ArchiveIterator(record_stream):
        page = record.content_stream().read()

        if len(page) > 750000:
            page = page[0:750000]

        extracts += [page] # now trying to get simply the raw text

    return extracts

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
    batch_n_within_partition = batch_n%args.batches_per_partition
    query = f'SELECT * FROM urls_merged_cc_to_download WHERE partition={partition_n} ORDER BY crawl, url_host_tld, fetch_time OFFSET {batch_n_within_partition*args.batch_size} LIMIT {args.batch_size}'

    df = exponential_backoff(wr.athena.read_sql_query, sql=query, database='ccindex', boto3_session=session)
    assert len(df) > 1, "Empty input table!"

    # initialize s3
    s3client = boto3.client('s3', region_name='us-east-1', use_ssl=False)

   # download paragraphs and fill into new column
    print('Starting download...')
    start = time.process_time()
    df['paragraphs'] = df.apply(lambda row: fetch_process_warc_records(row, s3client), axis=1)
    print(f'Success! Finished downloading in {time.process_time() - start} seconds.')

    # drop offsets
    df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)

    # save to S3
    s3_path = f's3://{args.output_bucket}/{args.result_output_path}/batch_n_{batch_n}.csv'
    df.to_csv(s3_path, index=False)
    print(f'Results saved to: {s3_path}')
