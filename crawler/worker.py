from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

'''
    each worker represents a single thread in the crawler
    they are responsible for:
        1) fetching the url from the frontier
        2) downloading the page
        3) scraping the page for new links
        4) adding new links to the forntier
'''
class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            # download function is form utils/download.py to fetch content of a URL
            resp = download(tbd_url, self.config, self.logger) 
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            # passes the downloaded information to the scraper function to get new links
            # this is what we are implementing
            scraped_urls = scraper.scraper(tbd_url, resp)
            # utilizes the fronter from here to:
                # retireve URLS with get_tbd_url(), add new URLS to the fornter add_url(), then mark them as completed mark_url_complete()
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            # this is for politeness so it avoids overloading servers with too many requests
            time.sleep(self.config.time_delay)
