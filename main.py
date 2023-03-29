from athena_lookup import Athena_lookup
import pandas as pd
from aws_config import aws_configure_credentials
from aws_batch import AWSBatch
import yaml
import boto3

if __name__ == '__main__':
    ## read config file
    with open("config.yml", "r", encoding='utf8') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    ## debug settings
    keep_ccindex = True if cfg['debug'] else False
    limit_cc_table = 1e6 if cfg['debug'] else None

    ## authenticate to AWS
    aws_configure_credentials(cfg['credentials_csv_filepath'], cfg['region'], cfg['profile_name'])

    # available_crawls = pd.read_csv('common-crawls.txt')

    ## upload url list to s3
    s3 = boto3.client('s3', region_name=cfg['region'], use_ssl=False)
    s3.upload_file(cfg['url_list_path'], cfg['output_bucket'], 'url-list/url-list.csv')
    cfg['url_list_path'] = f's3://{cfg["output_bucket"]}/url-list/'

    ## upload process_page.py to s3 to be used by batch jobs
    s3.upload_file('process_page.py', cfg['output_bucket'], 'scripts/process_page.py')

    ## upload keywords to s3 to be used by batch jobs
    if cfg['keywords_path']:
        s3.upload_file(cfg['keywords_path'], cfg['output_bucket'], 'keywords/keywords.txt')
        cfg['keywords_path'] = f's3://{cfg["output_bucket"]}/keywords/keywords.txt'


    ## upload url keywords
    if cfg['url_keywords_path']:
        s3.upload_file(cfg['url_keywords_path'], cfg['output_bucket'], 'url-keywords/url-keywords.csv')

    ## upload config file to s3
    s3.upload_file('config.yml', cfg['output_bucket'], 'config/config.yml')

    ## run athena lookup
    aws_params = {
        'region': cfg['region'],
        'catalog': 'AwsDataCatalog',
        'database': 'ccindex',
        'bucket': cfg['output_bucket'],
        'path': 'indexout',
    }

    if cfg['url_keywords_path']:
        url_keywords = pd.read_csv(cfg['url_keywords_path'], header=None, usecols=[0]).squeeze().tolist()
        cfg['url_keywords_path'] = url_keywords

    if not (cfg['debug'] or cfg['keep_urls_merged_cc']):
        answer = input(f'Estimated lookup costs: {0.2*len(cfg["crawls"]):.2f}$-{0.5*len(cfg["crawls"]):.2f} $. Continue? [y]/[n]').lower()
    else:
        answer = 'y'

    if answer == 'y':
        athena_lookup = Athena_lookup(aws_params, cfg['url_list_path'], cfg['crawls'],
                                      cfg['n_subpages_per_domain'], url_keywords=cfg['url_keywords_path'],
                                      limit_pages_url_keywords=cfg['limit_pages_url_keywords'],
                                      filter_lang=cfg['filter_lang'],
                                      one_snapshot_per_url=cfg['one_snapshot_per_url'],
                                      limit_cc_table=limit_cc_table, keep_ccindex=keep_ccindex,
                                      keep_urls_merged_cc=cfg['keep_urls_merged_cc'])
        athena_lookup.run_lookup()
    else:
        raise Exception('Lookup aborted.')

    ## run batch job
    batches_per_partition = athena_lookup.partition_length//cfg["batch_size"] + 1
    req_batches = int(batches_per_partition*100) # 100 is the number of partitions

    if cfg['debug']:
        req_batches = 2
        batches_per_partition = 1
        cfg["batch_size"] = 50
        cfg['retry_attempts'] = 1
        result_output_path = 'test'
    else:
        result_output_path = 'res'

    print(f'Splitting {athena_lookup.download_table_length:,} subpages into {req_batches:,} batches')

    if not cfg['debug']:
        answer = input(f'Estimated download costs: {0.33*athena_lookup.download_table_length*10**-6:.2f}$. Continue? [y]/[n]').lower()
    else:
        answer = 'y'

    if answer == 'y':
        aws_batch = AWSBatch(req_batches, cfg["batch_size"], batches_per_partition, cfg['output_bucket'],
                             result_output_path, cfg['image_name'], cfg['batch_role'],
                             keywords_path=cfg['keywords_path'],
                             retry_attempts=cfg['retry_attempts'],
                             attempt_duration=cfg['attempt_duration'],
                             keep_compute_env_job_queue=cfg['keep_compute_env_job_queue'],
                             vcpus=cfg['vcpus'], memory=cfg['memory'])
        aws_batch.run()
    else:
        raise Exception('Download batch job aborted.')