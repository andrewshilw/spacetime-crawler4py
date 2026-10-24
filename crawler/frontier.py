import os
import glob
import shelve
import dbm.dumb

from collections import deque
from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        # deque used as a FIFO queue (popleft), not a stack. The original
        # list + .pop() (no index) is LIFO: since all 4 seed URLs are
        # queued before anything else, and every newly-discovered link gets
        # appended and is immediately the next one popped, that order dives
        # depth-first into whichever domain happens to come up first - in
        # practice, meaning the crawl can get stuck going deep into one
        # domain and never meaningfully reach the other three. FIFO instead
        # explores roughly breadth-first, which keeps progress spread
        # across all four seed domains as they're discovered.
        self.to_be_downloaded = deque()

        # save_file is a base name - the actual on-disk file(s) always
        # carry a backend-specific suffix (e.g. "frontier.shelve.db" for
        # dbm.ndbm, or ".dir"/".dat"/".bak" for dbm.dumb below), so
        # os.path.exists(self.config.save_file) never matches anything.
        # Glob for whatever the backend actually created instead.
        existing_files = glob.glob(self.config.save_file + "*")

        if not existing_files and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif existing_files and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            for path in existing_files:
                os.remove(path)
        # Load existing save file, or create one if it does not exist.
        #
        # NOT shelve.open()'s default backend: that auto-selects whichever
        # dbm module is available (dbm.gnu, else dbm.ndbm, else dbm.dumb).
        # On macOS (no dbm.gnu), that's dbm.ndbm - a fixed-capacity
        # Berkeley DB hash table. Confirmed by hand mid-crawl, on this
        # exact file's sibling (analytics.shelve): once it held enough
        # entries, writes started failing with "_dbm.error: cannot add
        # item to database" / "HASH: Out of overflow pages" - an uncaught
        # exception that kills every worker thread. frontier.shelve holds
        # far MORE entries (every discovered URL, not just visited ones),
        # so it's at equal or greater risk over a real multi-hour crawl.
        # dbm.dumb is pure Python with no such fixed-capacity structure.
        self.save = shelve.Shelf(dbm.dumb.open(self.config.save_file, "c"))
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.popleft()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
