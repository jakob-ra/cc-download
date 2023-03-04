import pandas as pd
import yaml
import awswrangler as wr

with open("config.yml", "r", encoding='utf8') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
result_output_path = cfg['result_output_path'] + '/' + '_'.join(cfg['crawls']) # path in output_bucket to store the downloads in batches

res_path = f's3://{cfg["output_bucket"]}/{result_output_path}/'
res_sample = pd.read_parquet(res_path + 'batch_n_0.parquet')
# res_cols = res_sample.columns.tolist()
# res_cols = ' string, '.join(res_cols) + ' string'
# query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
#         ({res_cols})
#         ROW FORMAT DELIMITED
#         FIELDS TERMINATED BY ','
#         LINES TERMINATED BY '\n'
#         LOCATION '{res_path}'
#         TBLPROPERTIES ('skip.header.line.count'='1')"""
# athena_lookup.execute_query(query)

# read full result
res = wr.s3.read_parquet(path=res_path)
res[res.keyword_paragraphs.str.len() > 0].explode('keyword_paragraphs').url_host_registered_domain.nunique()
res[(res.keyword_paragraphs.str.len() > 0) & (res.content_languages == 'eng')].keyword_paragraphs.sample(
    10).values

# create hyperlink network
res['outgoing_links'] = res.apply(
        lambda row: [link for link in row.links if not row.url_host_registered_domain in link], axis=1)

res = res[res.outgoing_links.str.len()!=0].copy(deep=True)

def remove_url_prefix(url):
    url = url.replace('https://', '')
    url = url.replace('http://', '')
    url = url.replace('www.', '')
    url = url.split('/')[0]

    return url

res['outgoing_links_clean'] = res.outgoing_links.apply(lambda l: [remove_url_prefix(str(e)) for e in l])
res.url_host_name = res.url_host_name.apply(remove_url_prefix)

unique_urls = set(res.url_host_name.unique())

link_network = res[['url_host_name', 'outgoing_links_clean']]
link_network.columns = ['from', 'to']

link_network = link_network.explode('to')
link_network = link_network[link_network.to.isin(unique_urls)]

link_network.drop_duplicates(inplace=True)

link_network.value_counts().head(30)