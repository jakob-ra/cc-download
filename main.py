from athena_lookup import Athena_lookup
import pandas as pd
from aws_config import aws_configure_credentials
from aws_batch import AWSBatch
import yaml
import boto3
import re

if __name__ == '__main__':
    ## read config file
    with open("config.yml", "r", encoding='utf8') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    ## authenticate to AWS
    aws_configure_credentials(cfg['credentials_csv_filepath'], cfg['region'], cfg['profile_name'])

    # available_crawls = pd.read_csv('common-crawls.txt')

    ## hardcode keywords into process_page.py
    keywords = pd.read_csv(cfg["keywords_path"], header=None)[0].tolist() if cfg["keywords_path"] else None
    with open('process_page.py', 'r') as f:
        filedata = f.read()
    filedata = re.sub(r'keywords = (.+)\n', f'keywords = {keywords}\n', filedata)
    with open('process_page.py', 'w') as f:
        f.write(filedata)

    ## upload process_page.py to s3 to be used by batch jobs
    s3 = boto3.client('s3')
    s3.upload_file('process_page.py', cfg['output_bucket'], 'scripts/process_page.py')

    ## run athena lookup
    result_output_path = cfg['result_output_path'] + '/' + '_'.join(cfg['crawls']) # path in output_bucket to store the downloads in batches
    aws_params = {
        'region': cfg['region'],
        'catalog': 'AwsDataCatalog',
        'database': 'ccindex',
        'bucket': cfg['output_bucket'],
        'path': cfg['index_output_path'],
    }
    if cfg['url_keywords_path']:
        url_keywords = pd.read_csv(cfg['url_keywords_path'], header=None, usecols=[0]).squeeze().tolist()
        cfg['url_keywords_path'] = url_keywords

    answer = input(f'Estimated lookup costs: {0.2*len(cfg["crawls"]):.2f}$-{0.5*len(cfg["crawls"]):.2f} $. Continue? [y]/[n]').lower()
    if answer == 'y':
        athena_lookup = Athena_lookup(aws_params, cfg['s3path_url_list'], cfg['crawls'],
                                      cfg['n_subpages_per_domain'], url_keywords=cfg['url_keywords_path'],
                                      filter_lang=cfg['filter_lang'], limit_cc_table=None,
                                      keep_ccindex=False,
                                      limit_pages_url_keywords=cfg['limit_pages_url_keywords'])
        athena_lookup.run_lookup()
    else:
        raise Exception('Lookup aborted.')

    ## run batch job
    batches_per_partition = athena_lookup.partition_length//cfg["batch_size"] + 1
    req_batches = int(batches_per_partition*100) # 100 is the number of partitions
    print(f'Splitting {athena_lookup.download_table_length:,} subpages into {req_batches:,} batches of size {cfg["batch_size"]:,}.')
    answer = input(f'Estimated download costs: {0.33*athena_lookup.download_table_length*10**-6:.2f}$. Continue? [y]/[n]').lower()

    if answer == 'y':
        aws_batch = AWSBatch(req_batches, cfg["batch_size"], batches_per_partition, cfg['output_bucket'],
                             result_output_path, cfg['image_name'], cfg['batch_role'],
                             retry_attempts=cfg['retry_attempts'],
                             attempt_duration=cfg['attempt_duration'], keep_compute_env_job_queue=False,
                             vcpus=cfg['vcpus'], memory=cfg['memory'])
        aws_batch.run()
    else:
        raise Exception('Download batch job aborted.')