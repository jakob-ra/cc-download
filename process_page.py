from bs4 import BeautifulSoup, SoupStrainer

keywords = ['corona', 'covid', 'pandemic', 'epidemic', 'virus', 'hygiene', 'lockdown', 'quarantine', 'outbreak', 'vaccine', 'health', 'office', 'remote']

class PageProcessor:
    """Class for processing HTML pages"""
    def __init__(self, page, keywords=None):
        self.page = page
        self.keywords = ['corona', 'covid', 'pandemic', 'epidemic', 'virus', 'hygiene', 'lockdown', 'quarantine', 'outbreak', 'vaccine', 'health', 'office', 'remote']
        self.soup = BeautifulSoup(self.page, 'html.parser', parse_only=SoupStrainer(['p', 'a']))

    def extract_texts(self):
        """Extract texts from HTML page (takes only texts between <p> tags)"""
        # get all text
        texts = [text for text in self.soup.stripped_strings]

        # cut off very long texts
        texts = [text[:10000] for text in texts]

        self.texts = texts
        return texts

    def get_keyword_mentioning_texts(self):
        """Return texts that mention any of the keywords"""
        keyword_mentioning_texts = [text for text in self.texts if
                                    any(keyword.casefold() in text.casefold() for keyword in self.keywords)]

        self.keyword_mentioning_texts = keyword_mentioning_texts
        return keyword_mentioning_texts

    def extract_links(self):
        """Extract all links from HTML page"""
        # get all text
        links = [link.get('href') for link in self.soup.find_all('a') if link.get('href') is not None]
        links = [link.strip() for link in links if link.startswith('http')]
        # links = [link for link in links if not link.startswith(root_url)] # to get non-local links

        self.links = links
        return links

def process_page(page, keywords=keywords):
    """Process HTML page, return relevant paragraphs and non-local links"""
    processor = PageProcessor(page, keywords=keywords)
    processor.extract_texts()
    processor.get_keyword_mentioning_texts()
    # processor.extract_links()

    return processor.keyword_mentioning_texts #, processor.links

# # test on https://www.siemens.com/global/en.html
import requests
keywords = ['corona', 'covid', 'pandemic', 'epidemic', 'virus', 'hygiene', 'lockdown', 'quarantine', 'outbreak', 'vaccine', 'health', 'office', 'remote']
page = requests.get('https://www.siemens.com/global/en.html').content
process_page(page, keywords=keywords)