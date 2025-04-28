import re
from urllib.parse import urlparse, urljoin
import time
from bs4 import BeautifulSoup
import shelve

import io
import PyPDF2
import PyPDF2.errors

from utils import get_logger, normalize
from scraper_utils.fingerprint import get_fp
from scraper_utils.similarity import is_similar_to_visited
from scraper_utils.tokenizer import tokenize

TIMEOUT_LIMIT = 60 * 5
start_time = time.time()

scrap_logger = get_logger("SCRAPPER")
visited_urls = set()
visited_sites_fingerprint = set()
THRESHOLD = 0.8
unique_count = 0
longest_page_url = None
longest_page_word_count = 0
top50words = {}
subdomain_count = {}
top50words = {}

def scraper(url, resp):
    global longest_page_word_count

    if resp.status != 200 or resp.raw_response is None:
        if resp.status >= 300 and resp.status < 400:
            redirect_url = resp.raw_response.headers.get("Location")

            scrap_logger.warning(f"Status {resp.status}: Redirecting {url} -> {redirect_url}")
            return  [redirect_url] if is_valid(redirect_url) else []
        else:
            scrap_logger.warning(f"Skipping URL {url}: Invalid response or status {resp.status}")
            return []

    if is_pdf_resp(url, resp):
        scrap_logger.warning(f"Skipping {url}: pdf file")
        return []
    
    if is_zip_resp(url, resp):
        scrap_logger.warning(f"Skipping {url}: zip file")
        return []

    if is_attachment_resp(url, resp):
        scrap_logger.warning(f"Skipping {url}: downloads attachment")
        return []

    # Add the is_large_resp check here
    if is_large_resp(url, resp, threshold=10 * 1024 * 1024):  # 10 MB threshold
        scrap_logger.warning(f"Skipping {url}: response too large")
        return []
    
    # parse as html document
    try:
        # Get the text from the html response
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')

        # Remove the text of CSS, JS, metadata, alter for JS, embeded websites
        for markup in soup.find_all(["style", "script", "meta", "noscript", "iframe"]):  
            markup.decompose()  # remove all markups stated above
        
        # soup contains only human-readable texts now to be compared near-duplicate
        text = soup.get_text(separator=" ", strip=True)
    except Exception as e:
        scrap_logger.fatal(f"Error parsing {url}: {e}")
    
    track_unique_urls(url, visited_urls)
    track_subdomains(url, subdomain_count)
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
    text = soup.get_text()
    track_longest_page(url, text)
    update_top50_words(text, top50words)

    ''' FINGERPRINT CODE STARTS HERE '''
    #create fingerprint
    #decode content to string because its a byte
    page_content = resp.raw_response.content.decode('utf-8', errors='ignore')
    fingerprint = tuple(get_fp(page_content))

    # then check if the curr page is a near dupe of ANY prev page
        #if it is a dupe, then we dont add it, so SKIP scrawling the page
    if is_similar_to_visited(fingerprint, visited_sites_fingerprint, THRESHOLD):
        print(f"Skipping duplicate page: {url}")
        return [] # So that we skip crawling when its a dupe/near-dupe
    
    # If it is not a dupe then add it to the visted set
    visited_sites_fingerprint.add(fingerprint)
    ''' FINGERPRINT CODE ENDS HERE '''
    
    # We extract the links, all of them from the page
    links = extract_next_links(url, resp)
    # Then just return the links that need to be crawled
    # return [link for link in links if is_valid(link)]
    unique_links = set()
    for link in links:
        if not link:
            scrap_logger.info("Filtered out an empty or none URL")
        elif link in unique_links:
            scrap_logger.info(f"Filtered out duplicate URL: {link}")
        elif not is_valid(link):
            scrap_logger.info(f"Filtered out invalid URL: {link}")
        else:
            unique_links.add(link)

    # Save scraping results after the crawler finishes
    # save_to_shelve("scraper_results")

    return list(unique_links)

''' IMPLEMENT THIS PART => scraper is important for the worker class'''

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # simple check to see if the status code is OK => if it is NOT 200 (ok) then we need to say why, so we call out reps.error
    if resp.status != 200:
        print(f'Error code: {resp.error}')
        return []
    
    # BeautifulSoup will turn the web page into html content so i can find a specific element by tag and more...
    # resp.raw_response.content: the content of the page!

    # use the html.parse of beautifulsoup
    # print(f"response: {resp.raw_response.url}") 
    # print(f"response: {resp.raw_response.content}") 

    # i will store all the links in a list
    links = []
    '''    
    legacy version 1:
    for hyperlink in soup.find_all('a', href=True): # i want to find all a tags where href has something
        link = hyperlink['href']
        print(f"Found link: {link}") 
        links.append(link)
    '''
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        for anchor in soup.find_all('a', href=True):
            link = anchor.get('href')
            
            # convert relative url to absolute url
            abs_url = urljoin(url, link)
            parsed = urlparse(abs_url)

            # remove anyhting that starts iwth # sincei  keep seeing this and it means nothing
            clean_url = normalize(parsed._replace(query="", fragment="").geturl())
            
            # gitlab and github are bad
            if "gitlab.ics.uci.edu" in clean_url or "github.com" in clean_url:
                scrap_logger.info(f"Skipping GitLab/GitHub URL: {clean_url}")
                continue

            if not clean_url:
                scrap_logger.info("Skipping empty or malformed URL")
                continue

            # print(f"Found link: {clean_url}") 
            links.append(clean_url)

    except Exception as e:
        scrap_logger.fatal(f"Error parsing {url}: {e}")

    return list(links)

def is_valid(url):
# Decide whether to crawl this url or not. 
# If you decide to crawl it, return True; otherwise return False.
    try:
        # Break the URL into parts: scheme, netloc, path, query, etc.
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Check if it is within the allowed domains
        domain = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()

        # Exclude GitLab and GitHub links
        if "gitlab.ics.uci.edu" in domain or "github.com" in domain:
            return False

        # Exclude calendar and event-related URLs
        if "/events_calendar" in path or "/calendar" in path or "/events" in path or "date=" in query or "year=" in query or "month=" in query:
            scrap_logger.info(f"Skipping calendar/event URL: {url}")
            return False

        # Rule for links
        valid_domains = [
            "ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"
        ]
        today_uci_path = "/department/information_computer_sciences/"    
        # Allow any subdomain of the 4 domains
        if any(domain.endswith(d) for d in valid_domains):
            return not re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                + r"|png|tiff?|mid|mp2|mp3|mp4"
                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                + r"|epub|dll|cnf|tgz|sha1"
                + r"|thmx|mso|arff|rtf|jar|csv"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz|img)$", parsed.path.lower())
        
        # Case for today.uci.edu since we only accept this specific path
        if domain == "today.uci.edu" and path.startswith(today_uci_path):
            return True

        # True conditions weren't triggered so we exit
        return False
    
    except TypeError:
        print("TypeError for ", parsed)
        raise

# for report
def save_to_shelve(filename="scraper_results"):
    """
    Save the scraping results to a shelve database.
    """
    try:
        with shelve.open(filename) as db:
            db['unique_count'] = unique_count
            db['longest_page_url'] = longest_page_url
            db['longest_page_word_count'] = longest_page_word_count
            db['top50words'] = sorted(top50words.items(), key=lambda x: x[1], reverse=True)[:50]
            db['subdomain_count'] = subdomain_count
        scrap_logger.info(f"Scraping results saved to {filename}.")
    except Exception as e:
        scrap_logger.error(f"Error saving to shelve: {e}")

def is_pdf_resp(url, resp):
    content_type = resp.raw_response.headers.get("Content-Type", "").lower()
    
    # First, check Content-Type header
    if "application/pdf" in content_type:
        return True
    
    try:
        with io.BytesIO(resp.raw_response.content) as pdf_stream:
            start = pdf_stream.read(5)
            if start != b"%PDF-":
                return False  # Definitely not a PDF
            
            # Reset pointer to beginning
            pdf_stream.seek(0)
            
            reader = PyPDF2.PdfReader(pdf_stream)
            return bool(reader.pages)
    except Exception:
        return False

def is_zip_resp(url, resp):
    content_type = resp.raw_response.headers.get("Content-Type", "").lower()

    if "application/zip" == content_type: 
        return True
    
    return False

def is_html_resp(url, resp):
    content_type = resp.raw_response.headers.get("Content-Type", "").lower()

    if content_type.startswith("text/html"):
        return True
    
    return False

def is_attachment_resp(url, resp): 
    content_disposition = resp.raw_response.headers.get("Content-Disposition", "").lower()

    if "attachment" in content_disposition:
        return True

    return False

def is_large_resp(url, resp, threshold): 
    content_length = resp.raw_response.headers.get("Content-Length", "")

    try:
        content_length = int(content_length)
        if content_length > threshold: 
            return True
    except: 
        return False
    
    return False