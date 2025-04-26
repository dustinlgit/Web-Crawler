import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from utils import get_logger, normalize
scrap_logger = get_logger("SCRAPPER")

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

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
    soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
    # print(f"response: {resp.raw_response.url}") 
    # print(f"response: {resp.raw_response.content}") 

    # # i will store all the links in a list
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

            print(f"Found link: {clean_url}") 
            links.append(clean_url)

    except Exception as e:
        scrap_logger.fatal(f"Error parsing {url}: {e}")

    return links


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
