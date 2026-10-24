from threading import Thread, Event

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    # Shared by every Worker thread/instance. launch.py sets this from a
    # SIGINT/SIGTERM handler to request a clean stop: killing the process
    # outright (no handler registered) can land mid-write to frontier.shelve
    # or analytics.shelve and corrupt them beyond repair, since neither file
    # format is crash-safe against a write interrupted partway through. This
    # event is only ever checked between iterations - i.e. after a download/
    # scrape/save cycle has fully finished - so it never interrupts a write
    # in progress.
    shutdown_event = Event()

    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)

    def run(self):
        while not Worker.shutdown_event.is_set():
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
        else:
            self.logger.info("Shutdown requested. Stopping Crawler.")

        # record_page_visit() only writes analytics.pkl to disk every
        # ANALYTICS_SAVE_INTERVAL pages (see scraper.py), not every single
        # one - force one last unconditional save here so neither a normal
        # completion nor a graceful stop ever loses the last few pages'
        # worth to that batching.
        scraper.flush_analytics_db()
