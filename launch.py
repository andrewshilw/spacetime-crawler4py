import sys
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


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    crawler.start()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
