from bs4 import BeautifulSoup, SoupStrainer

def extract_text(page):
    """Extract text from HTML page"""
    soup = BeautifulSoup(page, 'html.parser', parse_only=SoupStrainer(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']))

    # get all text
    text = soup.get_text()

    return text

def process_page(page):
    """Process page, return text"""
    text = extract_text(page)

    return text

# def extract_non_local_links(page, root_url):
#     """Extract text non-local links from HTML page"""
#     soup = BeautifulSoup(page, 'html.parser', parse_only=SoupStrainer('a'))
#
#     # get all text
#     links = [link.get('href') for link in soup.find_all('a') if link.get('href') is not None]
#     links = [link for link in links if not link.startswith(root_url)]
#
#     return links