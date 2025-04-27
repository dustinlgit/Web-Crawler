from urllib.parse import urlparse
from utils import normalize
from scraper_utils.tokenizer import tokenize

# Global variables for tracking data
unique_count = 0
longest_page_url = None
longest_page_word_count = 0
top50words = {}
subdomain_count = {}

def track_unique_urls(url, visited_urls):
    """Track unique URLs."""
    clean_url = urlparse(url)._replace(fragment="").geturl()
    visited_urls.add(clean_url)
    global unique_count
    unique_count = len(visited_urls)
    
def track_subdomains(url, subdomain_count):
    """Track subdomains count."""
    subdomain = urlparse(url).netloc.split('.')[0]
    if subdomain in subdomain_count:
        subdomain_count[subdomain] += 1
    else:
        subdomain_count[subdomain] = 1
        
def track_longest_page(url, text):
    """Track the longest page in terms of word count."""
    words = text.split()
    word_count = len(words)
    global longest_page_word_count, longest_page_url
    if word_count > longest_page_word_count:
        longest_page_url = url
        longest_page_word_count = word_count
        
def update_top50_words(text, top50words):
    """Update the top 50 words."""
    words = tokenize(text)
    for word in words:
        word = word.lower()
        if word in top50words:
            top50words[word] += 1
        else:
            top50words[word] = 1
