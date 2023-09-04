import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
import requests
from warcio.archiveiterator import ArchiveIterator
import time
from io import BytesIO
import awswrangler as wr
import re
import numpy as np
from nltk.tokenize import sent_tokenize
import time
from botocore.exceptions import ClientError

def exponential_backoff(func, *args, **kwargs):
    """Exponential backoff to deal with request limits"""
    delay = 1  # initial delay
    delay_incr = 1  # additional delay in each loop
    max_delay = 20  # max delay of one loop. Total delay is (max_delay**2)/2

    while delay < max_delay:
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            print(f"ClientError: {e}")
            time.sleep(delay)
            delay += delay_incr
    else:
        raise Exception("Exponential backoff timeout.")


class PassageExtractor:
    """ Extracts passages around keyword mentions.
    Parameters
    ----------
    text : str
        The text to extract passages from.
    keywords : list
        The keywords to extract passages around.
    return_paragraphs : bool
        Whether to return the entire paragraph containing a keyword mention.
    n_sent_backward : int
        The number of sentences to extract before the keyword mention. Does not apply if return_paragraphs
        is True.
    n_sent_forward : int
        The number of sentences to extract after the keyword mention. Does not apply if return_paragraphs
        is True.
    merge_passages : bool
        Whether to merge overlapping passages. Does not apply if return_paragraphs is True.
    char_limit : int
        The maximum number of characters to extract.
    Examples
    --------
    >>> text = 'This is a sentence This is second sentence! This is a third sentence mentioning a
    keyword. This is a fourth sentence. This is a fifth sentence. This is a sixth sentence.'
    >>> keywords = ['keyword']
    >>> extractor = PassageExtractor(text, keywords, n_sent_forward=2, n_sent_backward=1)
    >>> extractor.extract_relevant_passages()
    ['This is a second sentence! This is a third sentence mentioning a keyword. This is a fourth sentence.
    This is a fifth sentence.']
    """

    def __init__(self, text, keywords, return_paragraphs=False, n_sent_backward=1, n_sent_forward=7,
                 char_limit=3000, merge_passages=True):
        self.text = self.replace_all(text)
        self.keywords = keywords
        self.return_paragraphs = return_paragraphs
        self.n_sent_backward = n_sent_backward
        self.n_sent_forward = n_sent_forward
        self.merge_passages = merge_passages
        self.char_limit = char_limit
        if self.char_limit == None:
            self.char_limit = np.inf

    @staticmethod
    def replace_all(text):
        text = re.sub('\n+', '\n', text) # replace multiple newlines with one
        text = re.sub('\r+', '\r', text) # replace multiple carriage returns with one
        text = re.sub('\.+', '.', text) # replace multiple periods with one
        text = text.replace(u'\xa0', u' ') # replace non-breaking space with space
        text = ' '.join(text.split()) # replace multiple spaces with single space

        return text

    @staticmethod
    def mergeIntervals(arr):
        """ Merge overlapping intervals. """
        arr.sort(key=lambda x: x[0])
        index = 0
        for i in range(1, len(arr)):
            if (arr[index][1] >= arr[i][0]):
                arr[index][1] = max(arr[index][1], arr[i][1])
            else:
                index = index + 1
                arr[index] = arr[i]

        return arr[:index + 1]

    def extract_sentences_around_keyword_mention(self) -> list:
        sentence_boundary = '(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|!)\s'
        # sentences = re.split(sentence_boundary, self.text)
        sentences = sent_tokenize(self.text)
        intervals = []
        for index, sentence in enumerate(sentences):
            if re.search(r'\b' + r'\b|\b'.join(self.keywords) + r'\b', sentence, flags=re.IGNORECASE):
                keyword_mention_index = index
                start_index = max(0, keyword_mention_index - self.n_sent_backward)
                end_index = min(len(sentences), keyword_mention_index + self.n_sent_forward + 1)
                intervals.append([start_index, end_index])
        if self.merge_passages:
            intervals = self.mergeIntervals(intervals)
        relevant_passages = [' '.join(sentences[start_index:end_index]) for start_index, end_index in
                             intervals]

        # enforce character limit
        relevant_passages = [passage[:self.char_limit] for passage in relevant_passages]

        return relevant_passages

    def extract_relevant_passages(self) -> list:
        relevant_passages = []

        if self.return_paragraphs == True:
            paragraphs = self.text.split('\n')
            relevant_passages += [paragraph for paragraph in paragraphs if any(keyword.casefold()
                                      in paragraph.casefold() for keyword in self.keywords)]
        else:
            relevant_passages += self.extract_sentences_around_keyword_mention()

        return list(set(relevant_passages))  # remove duplicates

class PageProcessor:
    """Class for processing HTML pages"""
    def __init__(self, page: str, root_url, keywords: list = None):
        self.page = page
        self.keywords = keywords
        self.root_url = root_url
        self.soup = BeautifulSoup(self.page, 'lxml', parse_only=SoupStrainer(['p', 'a']))

    def get_text(self):
        """Get all text from HTML page"""

        text = self.soup.get_text(". ", strip=True)

        self.text = text
        return text

    def get_keyword_mentioning_texts(self):
        extractor = PassageExtractor(self.text, self.keywords)
        keyword_mentioning_texts = extractor.extract_relevant_passages()

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
        self.get_text()
        self.get_keyword_mentioning_texts()

        return {'keyword_paragraphs': self.keyword_mentioning_texts}

def process_page(page: str, root_url, keywords: list = None):
    """Process HTML page, return relevant paragraphs and non-local links"""
    page_processor = PageProcessor(page, root_url, keywords=keywords)

    return page_processor.process_page()

def test_processing():
    keywords = ['xcelerator']
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
        # self.df = self.df[self.df['keyword_paragraphs'].str.len() > 0]

    def save_results(self):
        if len(self.df) > 0:
            wr.s3.to_parquet(df=self.df, path=self.output_path, index=False, compression='gzip')
            print(f'Results saved to: {self.output_path}')

    def run(self):
        self.download_process_input_table()
        self.split_results()
        self.clean_results()
        self.save_results()