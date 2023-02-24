import time
import pandas as pd
import sys
from utils import convert_file_size
import boto3

class Athena_lookup():
    """ A class to query the Common Crawl index using Athena.
    Parameters
    ----------
    aws_params : dict
        Dictionary with the following keys:
        - bucket: S3 bucket where the results will be stored
        - path: S3 path where the results will be stored
        - database: Athena database where the results will be stored
    s3path_url_list : str
        S3 path to the list of URLs to be queried (needs to be a folder with csv file(s) with one URL per
        line,
        without a header, and the URLs without the protocol and subdomain, e.g.:
            example1.com
            example2.com
    crawls : list
        List of crawls to be queried
    n_subpages : int
        Number of subpages to be queried (selects the subpages with the shortest URL). If None, only the
        subpages containing URL keywords will be queried.
    url_keywords : list
        List of keywords to be queried when selecting subpages (e.g. 'news' as a url keyword would select
        'apple.com/news'. If None, only the n_subpages with the shortest URL will be queried.
    limit_pages_url_keywords : int
        Maximum number of pages to be queried when selecting subpages based on URL keywords. If None, all
        such subpages will be queried.
    filter_lang: str
        Filter for language (iso 639-3 code) of the subpages to download, set to None if not needed
    one_snapshot_per_url: bool
        If a URL is in more than one of the specified crawls, use this option to only keep the first
        crawl. Only changes behavior if multiple crawls are specified.
    athena_price_per_tb : float
        Price per TB of data queried in Athena (see https://aws.amazon.com/athena/pricing/)
    wait_seconds : int
        Number of seconds to wait before query times out
    limit_cc_table : int
        Limit the number of rows in the ccindex table to keep costs low for debugging
    keep_ccindex : bool
        Keep the ccindex table after querying to speed up future queries
    Returns
    -------
    Athena_lookup object
    """

    def __init__(self, aws_params: dict, s3path_url_list, crawls: list, n_subpages: int,
                 url_keywords: list | None = None, limit_pages_url_keywords=100,
                 filter_lang: str | None = None, one_snapshot_per_url=False, athena_price_per_tb=5,
                 wait_seconds=3600, limit_cc_table=10000, keep_ccindex=False):
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.aws_params = aws_params
        self.s3path_url_list = s3path_url_list
        self.crawls = crawls
        self.n_subpages = n_subpages
        self.url_keywords = url_keywords
        self.filter_lang = filter_lang
        self.one_snapshot_per_url = one_snapshot_per_url
        self.athena_price_per_tb = athena_price_per_tb
        self.wait_seconds = wait_seconds
        self.limit_cc_table = limit_cc_table  # to keep costs low for debugging; this should be None for full table
        self.total_cost = 0
        self.ccindex_table_name = 'ccindex'
        self.keep_ccindex = keep_ccindex
        self.limit_pages_url_keywords = limit_pages_url_keywords

    @staticmethod
    def get_var_char_values(d):
        return [obj['VarCharValue'] if len(obj) > 0 else None for obj in d['Data']]

    def query_results(self, return_results=False):
        try:
            athena_client = self.athena_client
            params = self.aws_params
            wait_seconds = self.wait_seconds
            print('Starting query:\n' + params['query'])
            ## This function executes the query and returns the query execution ID
            response_query_execution_id = athena_client.start_query_execution(QueryString=params['query'],
                 QueryExecutionContext={'Database': params['database']},
                 ResultConfiguration={'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']}
                 )

            while wait_seconds > 0:
                wait_seconds = wait_seconds - 1
                response_get_query_details = athena_client.get_query_execution(
                        QueryExecutionId=response_query_execution_id['QueryExecutionId'])
                status = response_get_query_details['QueryExecution']['Status']['State']
                execution_time = response_get_query_details['QueryExecution']['Statistics'][
                    'TotalExecutionTimeInMillis']
                try:
                    data_scanned = response_get_query_details['QueryExecution']['Statistics'][
                        'DataScannedInBytes']
                except KeyError:
                    data_scanned = 0
                cost = data_scanned * 1e-12 * self.athena_price_per_tb


                if (status == 'FAILED') or (status == 'CANCELLED'):
                    failure_reason = response_get_query_details['QueryExecution']['Status'][
                        'StateChangeReason']
                    try:
                        self.total_cost += cost
                    except:
                        pass
                    print(failure_reason)
                    sys.exit(0)

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration'][
                        'OutputLocation']
                    try:
                        self.total_cost += cost
                    except:
                        pass
                    print('')
                    # get size of output file
                    try:
                        output_size = self.s3_client.head_object(Bucket=params['bucket'],
                                            Key=location.split(params['bucket'] +'/')[1])['ContentLength']
                        print(f'Query successful! Results are available at {location}. Total size: {convert_file_size(output_size)}')
                    except:
                        print(f'Query successful! Results are available at {location}.')

                    ## Function to get output results
                    if return_results:
                        response_query_result = client.get_query_results(
                                QueryExecutionId=response_query_execution_id['QueryExecutionId'])
                        result_data = response_query_result['ResultSet']

                        if len(result_data['Rows']) > 1:
                            print('Results are available at ' + location)
                            if return_results:
                                header = result_data['Rows'][0]
                                rows = result_data['Rows'][1:]
                                header = get_var_char_values(header)
                                result = pd.DataFrame(
                                        [dict(zip(header, get_var_char_values(row))) for row in rows])
                                return location, result

                        else:
                            print('Result has zero length.')
                            return location, None

                    else:
                        return location, None

                else:
                    print(
                        f'Time elapsed: {execution_time / 1000}s. Data scanned: '
                        f'{convert_file_size(data_scanned)}. Total cost: {self.total_cost + cost:.2f}$.',
                        end='\r')
                    time.sleep(1)

            print('Timed out after 1 hour.')
            return None, None

        except KeyboardInterrupt:
            self.athena_client.stop_query_execution(QueryExecutionId=response_query_execution_id['QueryExecutionId'])
            print('Keyboard interrupt: Query cancelled.')
            sys.exit(0)

    def execute_query(self, query):
        self.aws_params['query'] = query
        res = self.query_results()

        return res

    def drop_all_tables(self):
        query = f"""DROP TABLE IF EXISTS url_list;"""
        self.execute_query(query)
        if not self.keep_ccindex:
            query = f"""DROP TABLE IF EXISTS ccindex;"""
            self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_all_langs;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_to_download_unsorted;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_to_download_sorted;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_to_download_deduplicated;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_to_download_unpartitioned;"""
        self.execute_query(query)
        query = f"""DROP TABLE IF EXISTS urls_merged_cc_to_download;"""
        self.execute_query(query)

    def create_url_list_table(self):
        # to-do: make unspecific to bvdid
        query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS url_list (
        websiteaddress               STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        LOCATION '{self.s3path_url_list}'
        ;"""
        self.execute_query(query)

    def create_ccindex_table(self):
        query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
        url_surtkey                   STRING,
        url                           STRING,
        url_host_name                 STRING,
        url_host_tld                  STRING,
        url_host_2nd_last_part        STRING,
        url_host_3rd_last_part        STRING,
        url_host_4th_last_part        STRING,
        url_host_5th_last_part        STRING,
        url_host_registry_suffix      STRING,
        url_host_registered_domain    STRING,
        url_host_private_suffix       STRING,
        url_host_private_domain       STRING,
        url_protocol                  STRING,
        url_port                      INT,
        url_path                      STRING,
        url_query                     STRING,
        fetch_time                    TIMESTAMP,
        fetch_status                  SMALLINT,
        content_digest                STRING,
        content_mime_type             STRING,
        content_mime_detected         STRING,
        content_charset               STRING,
        content_languages             STRING,
        warc_filename                 STRING,
        warc_record_offset            INT,
        warc_record_length            INT,
        warc_segment                  STRING)
        PARTITIONED BY (
        crawl                         STRING,
        subset                        STRING)
        STORED AS parquet
        LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';"""
        self.execute_query(query)

    def repair_ccindex_table(self):
        query = f"""MSCK REPAIR TABLE {self.ccindex_table_name};"""
        self.execute_query(query)

    def inner_join(self):
        # limit (for debugging)
        if self.limit_cc_table:
            limit = 'LIMIT ' + str(self.limit_cc_table)
        else:
            limit = ''

        # if list of crawls specified, join them with OR: (crawl = 'CC-MAIN-2020-24' OR crawl = 'CC-MAIN-2020-52')
        crawls = '(' + ' OR '.join(f'crawl = \'{w}\'' for w in self.crawls) + ')'

        query = f"""CREATE TABLE urls_merged_cc_all_langs AS
        SELECT url,
               url_host_name,
               url_host_registered_domain,
               url_host_tld,
               fetch_time,
               warc_filename,
               warc_record_offset,
               warc_record_offset + warc_record_length - 1 as warc_record_end,
               crawl,
               subset,
               content_languages
        FROM {self.ccindex_table_name}, url_list
        WHERE {crawls}
          AND subset = 'warc'
          AND {self.ccindex_table_name}.url_host_registered_domain = url_list.websiteaddress
        {limit}"""
        self.execute_query(query)
        self.output_columns = """url, url_host_name, url_host_registered_domain, url_host_tld, fetch_time, 
        warc_filename, warc_record_offset, warc_record_end, crawl, content_languages"""

    def select_lang(self):
        if self.filter_lang:
            query = f"""CREATE TABLE urls_merged_cc AS
            SELECT *
            FROM urls_merged_cc_all_langs
            WHERE content_languages = '{self.filter_lang}' OR url like '%/en/%'""" # use LIKE '%{self.filter_lang}%' for pages in filter_lang and other languages
            self.execute_query(query)
        else:
            query = f"""CREATE TABLE urls_merged_cc AS
            SELECT *
            FROM urls_merged_cc_all_langs"""
            self.execute_query(query)

    def select_subpages(self):
        # shortest subpages
        if self.n_subpages:
            shortest_subpages_query = f"""select {self.output_columns}
                            from (SELECT urls_merged_cc.*, 
                                row_number() over(partition by url_host_registered_domain order by length(url) asc) as subpage_rank
                                from urls_merged_cc)
                            where subpage_rank <= {self.n_subpages}"""

        # subpages with URLs containing keywords
        if self.url_keywords:
            keyword_subpages_query = f"""select {self.output_columns} from (SELECT {self.output_columns},
                                row_number() over(partition by url_host_registered_domain order by length(url) asc) as subpage_rank
                                from (SELECT {self.output_columns}
                                from urls_merged_cc
                        WHERE """ + ' OR '.join([f"url LIKE '%{keyword}%'" for keyword in self.url_keywords]) + ')) as u'
            if self.limit_pages_url_keywords:
                keyword_subpages_query +=  f' WHERE subpage_rank <= {self.limit_pages_url_keywords}'

        # form query
        if self.n_subpages and self.url_keywords:
            query = f"""CREATE TABLE urls_merged_cc_to_download_unsorted AS
            ({shortest_subpages_query})
            UNION 
            ({keyword_subpages_query})"""
        elif self.n_subpages:
            query = f"""CREATE TABLE urls_merged_cc_to_download_unsorted AS
            SELECT {self.output_columns}
            FROM ({shortest_subpages_query})"""
        elif self.url_keywords:
            query = f"""CREATE TABLE urls_merged_cc_to_download_unsorted AS
            SELECT * FROM ({keyword_subpages_query})"""
        else:
            query = f"""CREATE TABLE urls_merged_cc_to_download_unsorted AS
            SELECT * FROM urls_merged_cc"""

        self.execute_query(query)

    # def sort_download_table_by_tld(self):
    #     """ Sorts the the download table by Top Level Domain (TLD) to make it more likely that all subpages
    #     in a batch are using the same language (hence need to download fewer language models). Keeps the
    #     crawl order intact."""
    #     query = f"""CREATE TABLE urls_merged_cc_to_download_unpartitioned AS
    #     SELECT * FROM urls_merged_cc_to_download_unsorted
    #     ORDER BY crawl, url_host_tld, fetch_time"""
    #
    #     self.execute_query(query)

    def deduplicate_download_table(self):
        """CommonCrawl sometimes crawls the same URL multiple times because of redirects. This removes
        duplicate URLs from the download table, keeping only the one with the largest payload size,
        i.e. the most content. If one_snapshot_per_url=True and a URL is present in multiple crawls,
        keeps only the version with the biggest payload (if tied, use the first crawl). """
        partition_by = 'crawl' if self.one_snapshot_per_url else '(url, crawl)'
        query = f"""create table urls_merged_cc_to_download_deduplicated as
                    select {self.output_columns}
                    from
                    (select urls_merged_cc_to_download_unsorted.*, 
                    row_number() over(partition by {partition_by} order by ((warc_record_offset - warc_record_end), fetch_time) desc) as rn
                    from urls_merged_cc_to_download_unsorted) as u
                    where rn=1"""

        self.execute_query(query)

    def partition_download_table(self):
        """Partitions the download table into 100 partitions (this is the maximum and makes querying the table faster)"""
        query = f"""CREATE TABLE urls_merged_cc_to_download
        WITH (partitioned_by = ARRAY['partition']) AS
        SELECT urls_merged_cc_to_download_deduplicated.*, ntile(100) over (order by null) as partition
        from urls_merged_cc_to_download_deduplicated;"""

        self.execute_query(query)


    def get_length_download_table(self):
        query = f"""SELECT COUNT(*) FROM urls_merged_cc_to_download"""

        download_table_length_location, _ = self.execute_query(query)

        self.download_table_length = pd.read_csv(download_table_length_location).values[0][0]

        # find number of unique hostnames
        query = f"""SELECT COUNT(DISTINCT url_host_registered_domain) FROM urls_merged_cc_to_download"""

        download_table_n_unique_host_location, _ = self.execute_query(query)

        self.n_unique_hosts = pd.read_csv(download_table_n_unique_host_location).values[0][0]

        query = f"""SELECT COUNT(DISTINCT websiteaddress) FROM url_list"""

        input_table_length_location, _ = self.execute_query(query)

        self.input_table_length = pd.read_csv(input_table_length_location).values[0][0]

        # find number of rows per partition
        query = f"""SELECT COUNT(*) FROM urls_merged_cc_to_download GROUP BY partition"""

        partition_length_location, _ = self.execute_query(query)

        self.partition_length = pd.read_csv(partition_length_location).values[0][0]

    # def save_table_as_csv(self):
    #     query = f"""SELECT * FROM cc_merged_to_download"""
    #
    #     self.download_table_location,_  = self.execute_query(query)


    def run_lookup(self):
        self.drop_all_tables()
        self.create_url_list_table()
        if not self.keep_ccindex:
            self.create_ccindex_table()
            self.repair_ccindex_table()
        self.inner_join()
        self.select_lang()
        self.select_subpages()
        # self.sort_download_table_by_tld()
        self.deduplicate_download_table()
        self.partition_download_table()
        self.get_length_download_table()
        # self.save_table_as_csv()
        print(f'The results contain {self.download_table_length:,} subpages from {self.n_unique_hosts:,}'
              f' unique hostnames.')
        print(f'Matched {self.n_unique_hosts/self.input_table_length*100:.1f}% of the input domains to at least one subpage.')
