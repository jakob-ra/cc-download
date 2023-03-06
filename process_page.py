import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
import requests
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
import awswrangler as wr
import re
from utils import exponential_backoff

class PageProcessor:
    """Class for processing HTML pages"""
    def __init__(self, page: str, root_url, keywords: list = None):
        self.page = page
        self.keywords = keywords
        self.root_url = root_url
        self.soup = BeautifulSoup(self.page, 'lxml', parse_only=SoupStrainer(['p', 'a']))

    def extract_texts(self):
        """Extract texts from HTML page (takes only texts between <p> tags)"""
        # get all text
        texts = [text for text in self.soup.stripped_strings]

        # cut off very long texts
        texts = [text[:10000] for text in texts]

        self.texts = texts
        return texts

    def get_keyword_mentioning_texts(self):
        """Return texts that mention any of the keywords, respecting word boundaries"""
        keyword_mentioning_texts = [text for text in self.texts if
                                    re.search(r'\b' + r'\b|\b'.join(self.keywords) + r'\b', text,
                                              flags=re.IGNORECASE)]

        self.keyword_mentioning_texts = keyword_mentioning_texts
        return keyword_mentioning_texts

    def extract_links(self):
        """Extract all links from HTML page"""
        # get all text
        links = [link.get('href') for link in self.soup.find_all('a') if link.get('href') is not None]
        links = [link.strip() for link in links if link.startswith('http')]
        links = [link for link in links if not self.root_url in link] # to get non-local links

        self.links = links
        return links

    def process_page(self):
        """Process HTML page, return relevant paragraphs and non-local links"""
        self.extract_texts()
        self.get_keyword_mentioning_texts()

        return {'keyword_paragraphs': self.keyword_mentioning_texts}

def process_page(page: str, root_url, keywords: list = None):
    """Process HTML page, return relevant paragraphs and non-local links"""
    page_processor = PageProcessor(page, root_url, keywords=keywords)

    return page_processor.process_page()

def test_processing():
    keywords = ['affordable eyeglasses']
    page = requests.get('https://www.siemens.com/global/en.html').content

    return process_page(page, 'siemens.com', keywords=keywords)

class CCDownloader:
    """Class for downloading and processing Common Crawl data. """
    def __init__(self, download_table, s3client, output_path, keywords=None):
        self.df = download_table
        self.s3client = s3client
        self.keywords = keywords
        self.output_path = output_path

    @staticmethod
    def fetch_process_warc_records(row, s3client, keywords=None):
        """Fetch all WARC records defined by filenames and offsets in download table,
        parse the records and the contained HTML pages using the provided function page_process_func,
        and return the results. """

        warc_path, offset, end = row.warc_filename, str(row.warc_record_offset), str(row.warc_record_end)

        rangereq = 'bytes={}-{}'.format(offset, end)

        response = exponential_backoff(s3client.get_object, Bucket='commoncrawl', Key=warc_path, Range=rangereq)

        record_stream = BytesIO(response["Body"].read())

        for record in ArchiveIterator(record_stream): # there is only one record in the byte stream
            if record.rec_type == 'response':
                page = record.content_stream().read()
                return process_page(page, row.url_host_registered_domain, keywords=keywords)

    def download_process_input_table(self):
        # download paragraphs and fill into new column
        print('Starting download...')
        start = time.process_time()
        self.df['result'] = self.df.apply(
                lambda row: self.fetch_process_warc_records(row, self.s3client, keywords=self.keywords), axis=1)
        print(f'Success! Finished downloading and processing in {time.process_time() - start} seconds.')

    def split_results(self):
        """ If the result is a dict, split it into multiple columns. """
        if type(self.df['result'].iloc[0]) is dict:
            self.df = pd.concat([self.df.drop(['result'], axis=1), self.df['result'].apply(pd.Series)], axis=1)

    def clean_results(self):
        """ Drop unnecessary columns and rows. """
        self.df.drop(columns=['warc_filename', 'warc_record_offset', 'warc_record_end'], inplace=True)

        # drop rows with empty result
        self.df = self.df[self.df['keyword_paragraphs'].str.len() > 0]

    def save_results(self):
        if len(self.df) > 0:
            wr.s3.to_parquet(df=self.df, path=self.output_path, index=False, compression='gzip')
            print(f'Results saved to: {self.output_path}')

    def run(self):
        self.download_process_input_table()
        self.split_results()
        self.clean_results()
        self.save_results()