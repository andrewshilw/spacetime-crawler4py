import sys
import signal
import multiprocessing
from configparser import ConfigParser
from argparse import ArgumentParser

if sys.platform == "darwin":
    # macOS defaults to the "spawn" multiprocessing start method (Python
    # 3.8+), which requires pickling the target process - but spacetime's
    # cache-server node is a locally-defined class that can't be pickled.
    # "fork" (Linux's default, which is why this isn't an issue on openlab)
    # avoids pickling entirely.
    multiprocessing.set_start_method("fork")

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler
from crawler.worker import Worker
import scraper


def _request_shutdown(signum, frame):
    # Without this handler, SIGTERM (a plain `kill`) uses the OS's default
    # disposition: the process is terminated immediately, with no Python
    # code - including atexit handlers - running at all. If that lands
    # mid-write to frontier.shelve or analytics.shelve, the file is
    # corrupted beyond repair (confirmed by hand: a bare `kill` during a
    # live run broke both files). Setting shutdown_event instead asks every
    # Worker thread to stop at the top of its next loop iteration, i.e.
    # only once its current download/scrape/save cycle has fully finished.
    print("\nShutdown requested - finishing the current download/save cycle "
          "on each worker, then exiting...")
    Worker.shutdown_event.set()


def main(config_file, restart):
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    # So is_valid()/scraper() can fetch robots.txt/sitemap.xml through the
    # cache server (see scraper.set_crawler_config's docstring).
    scraper.set_crawler_config(config, restart=restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
