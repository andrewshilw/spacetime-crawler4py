import re
import os
import glob
import shutil
import pickle
import urllib.robotparser
from datetime import datetime
from collections import Counter, defaultdict, deque
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl
from xml.etree import ElementTree

from bs4 import BeautifulSoup, Comment

from utils.download import download as _cache_download

# Extra credit (+1): robots.txt / sitemap checking.
# launch.py calls set_crawler_config() once at startup so is_valid() and
# scraper() can fetch robots.txt/sitemap.xml through utils.download.download
# - the SAME path Worker uses for every page - never a direct request. The
# architecture is explicit that bypassing the cache is not allowed, and
# that applies here just as much as to page fetches.
_crawler_config = None
_robots_parsers = {}    # netloc -> urllib.robotparser.RobotFileParser, or None (unavailable - fail open)
_sitemap_fetched = set()  # netlocs whose sitemap we've already pulled, so we only do it once each

# Module 5: persisted analytics for the report, so a restart during
# deployment (up to 3 allowed) doesn't throw away report progress.
#
# NOT backed by shelve/dbm - macOS only has dbm.ndbm available (no
# dbm.gnu), whose underlying Berkeley DB hash format has a fixed-capacity
# hash table. Confirmed by hand mid-crawl: once word_freq grew large
# enough, writing it back crashed the whole process with
# "_dbm.error: cannot add item to database" / "HASH: Out of overflow
# pages" - an uncaught exception that killed every worker thread, losing
# all progress. A single pickled file has no such limit, and writing it
# via a temp file + os.replace (atomic on POSIX) is also inherently
# crash-safe against a kill mid-write, unlike dbm.
ANALYTICS_SAVE_FILE = "analytics.pkl"
_analytics_db = None  # lazily loaded - see _get_analytics_db

# Save to disk every N pages instead of every single one. record_page_visit
# always updates the in-memory dict immediately; only the disk write is
# batched. Without this, _save_analytics_db() re-pickles the ENTIRE
# word_freq dict on every page - fine at the ~6000-page scale tested so
# far, but over a multi-day deployment crawl reaching tens of thousands of
# distinct words, that write only gets bigger and slower over time,
# especially on a networked filesystem (e.g. an NFS-mounted home directory
# on openlab) where it could end up slower than the politeness delay
# itself and throttle the whole crawl. flush_analytics_db() (called once a
# worker's loop exits, win or shutdown) always does one final unbatched
# save, so a normal stop never loses the last few pages to this batching -
# only a hard crash (SIGKILL) can lose up to ANALYTICS_SAVE_INTERVAL - 1
# pages' worth, same bounded-loss tradeoff as POLITENESS already accepts
# for timing.
ANALYTICS_SAVE_INTERVAL = 20
_pages_since_last_save = 0

# Where backups of analytics.pkl go before --restart wipes the live file,
# and where regenerate_report_from_backup() looks for one to recover from.
ANALYTICS_BACKUP_DIR = "reports"
ANALYTICS_BACKUP_PATTERN = "analytics_backup_*.pkl"

# Standard English stopword list (per the assignment spec's linked list),
# used to filter the word-frequency table for the report.
STOPWORDS = frozenset({
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can't",
    "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from",
    "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
    "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it",
    "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only",
    "or", "other", "ought", "our", "ours", "ourselves", "out", "over",
    "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should",
    "shouldn't", "so", "some", "such", "than", "that", "that's", "the",
    "their", "theirs", "them", "themselves", "then", "there", "there's",
    "these", "they", "they'd", "they'll", "they're", "they've", "this",
    "those", "through", "to", "too", "under", "until", "up", "very",
    "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's",
    "which", "while", "who", "who's", "whom", "why", "why's", "with",
    "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
    "you've", "your", "yours", "yourself", "yourselves",
})

ALLOWED_DOMAINS = (
    "ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu",
)

MAX_CONTENT_SIZE = 5 * 1024 * 1024  # skip pages whose body is bigger than this

MAX_PATH_PREFIX_VISITS = 100   # module 3: see is_repeated_pattern_trap
MIN_WORDS_FOR_REAL_PAGE = 50   # module 3: see is_dead_or_low_value_page

# Extra credit (+2): near-duplicate detection. See compute_simhash /
# is_near_duplicate_of_recent_pages.
SIMHASH_BITS = 64
NEAR_DUP_WINDOW = 50             # compare only against the last N pages,
                                  # not the entire crawl history (Note 4)
NEAR_DUP_HAMMING_THRESHOLD = 3    # out of 64 bits; a common SimHash cutoff
_recent_simhashes = deque(maxlen=NEAR_DUP_WINDOW)

# Module 3 state: lives only for this crawl process. Does NOT persist across
# restarts the way Frontier's shelve file does.
# Maps (netloc, first_path_segment) -> set of distinct normalized URLs seen
# under that bucket. A set, not a counter, so that the SAME popular page
# being linked from hundreds of other pages doesn't look like hundreds of
# distinct trap URLs.
_path_prefix_seen_urls = defaultdict(set)

WORD_RE = re.compile(r"[a-zA-Z0-9']+")  # module 3: see get_page_word_count
NON_VISIBLE_TAGS = ("script", "style", "head", "noscript", "template")

MAX_URL_LENGTH = 200
MAX_PATH_SEGMENTS = 10
MAX_SEGMENT_REPEATS = 3
MAX_QUERY_PARAMS = 10
TRAP_QUERY_KEYS = {
    "sessionid", "sid", "phpsessid", "jsessionid",
    "year", "month", "day", "date", "ical",
}
# Calendar/event-listing traps often encode the date in the PATH instead of
# a query string (e.g. /events/2026-05-13, /events/week/2026-05-18), which
# TRAP_QUERY_KEYS above can't see - each day/week is a distinct, real-
# looking URL, so is_repeated_pattern_trap only catches it after wasting
# MAX_PATH_PREFIX_VISITS requests. Catch it directly instead.
DATE_SEGMENT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{4}-\d{2}$")

DISALLOWED_EXTENSIONS = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico"
    + r"|png|tiff?|mid|mp2|mp3|mp4"
    + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
    + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
    + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
    + r"|epub|dll|cnf|tgz|sha1"
    + r"|thmx|mso|arff|rtf|jar|csv"
    + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", re.IGNORECASE)


def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = []
    for link in links:
        if not is_valid(link):
            continue
        # Deliberately NOT folded into is_valid(): Frontier._parse_save_file
        # also calls is_valid() on every already-seen URL when resuming from
        # frontier.shelve, which would double count visits on every restart.
        if is_repeated_pattern_trap(urlparse(link)):
            continue
        valid_links.append(link)

    # Extra credit (+1): the first time we see a domain, pull its sitemap
    # and add any URLs it lists - sitemaps often list pages nothing we've
    # already crawled links to.
    parsed_current = urlparse(url)
    if parsed_current.netloc not in _sitemap_fetched:
        _sitemap_fetched.add(parsed_current.netloc)
        for sitemap_url in get_sitemap_urls(parsed_current):
            if is_valid(sitemap_url):
                valid_links.append(sitemap_url)

    return valid_links


def extract_next_links(url, resp):
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200 or resp.raw_response is None:
        return list()

    content = resp.raw_response.content
    if not content:
        # 200 with an empty body - a dead URL, not a page with links on it.
        return list()

    if len(content) > MAX_CONTENT_SIZE:
        return list()

    # Fail CLOSED, not open: a missing Content-Type must NOT be treated as
    # "assume HTML". Confirmed live: a binary kernel image (zImage_paapi,
    # no file extension to catch it on) slipped through here with an
    # empty content_type, got fed to BeautifulSoup as if it were text, and
    # flooded the word-frequency table with single-character garbage from
    # misparsed binary bytes - corrupting the whole report's word stats.
    content_type = resp.raw_response.headers.get("Content-Type", "")
    if "html" not in content_type.lower():
        return list()

    try:
        soup = BeautifulSoup(content, "lxml")
    except Exception:
        return list()

    base_url = resolve_base_url(url, resp)

    # Record analytics for every page we actually got real HTML back for,
    # BEFORE the dead-page check below - a thin/low-value page still counts
    # as a page you found, even though you won't crawl further from it.
    record_page_visit(base_url, soup)

    if is_dead_or_low_value_page(soup):
        return list()

    if is_near_duplicate_of_recent_pages(soup):
        return list()

    next_links = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href or href.lower().startswith(("javascript:", "mailto:", "tel:")):
            continue
        next_links.append(normalize_url(href, base_url=base_url))

    return next_links


def defragment_url(url):
    """
    Strip the fragment (#...) from a URL. Two URLs that differ only
    by fragment must become the SAME string after this.
    Also used as the canonical form for the uniqueness count in the report.
    """
    parsed = urlparse(url)
    return urlunparse(parsed._replace(fragment=""))


def normalize_url(url, base_url=None):
    """
    Resolve `url` to an absolute URL (it may be relative, e.g. "/page.html"
    or "../x"), using `base_url` (the page it was found on) when needed.
    Then defragment it. Return the canonical absolute URL string.
    Think about: trailing slashes, default ports (http:80, https:443),
    and lowercasing scheme/netloc (but NOT the path - paths are case-sensitive).
    """
    if base_url:
        url = urljoin(base_url, url)

    url = defragment_url(url)
    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-len(":80")]
    elif scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-len(":443")]

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def is_in_allowed_domain(parsed_url):
    """
    True iff parsed_url.netloc is, or is a subdomain of, one of
    ALLOWED_DOMAINS (e.g. "vision.ics.uci.edu" -> True, "ics.uci.edu.evil.com" -> False).
    Be careful with naive `.endswith()` checks - they're exploitable
    by attacker-controlled substrings; match on the registrable domain boundary.
    """
    netloc = parsed_url.netloc.lower()
    host = netloc.rsplit("@", 1)[-1]  # drop userinfo, if any (user:pass@host)
    host = host.rsplit(":", 1)[0]     # drop port, if any

    return any(
        host == domain or host.endswith("." + domain)
        for domain in ALLOWED_DOMAINS)


def has_disallowed_extension(parsed_url):
    """
    True iff the URL's path ends in a file extension you don't want to crawl.
    DISALLOWED_EXTENSIONS above is the starting list (same as the original
    is_valid) - extend it if you find other large/low-value binary types
    while crawling.
    """
    return DISALLOWED_EXTENSIONS.match(parsed_url.path.lower()) is not None


def is_likely_trap(parsed_url):
    """
    Cheap, URL-string-only trap heuristics that don't require fetching the page:
    - repeated path segments (e.g. /a/a/a/a/...)
    - excessive path depth or URL length
    - query strings that look like calendar/session/sort permutations
      (e.g. ?year=..&month=.., ?sessionid=.., ?page=9999)
    - calendar/event dates encoded directly in the path (e.g.
      /events/2026-05-13, /events/week/2026-05-18) - caught live while
      crawling ics.uci.edu/events, where every day/week is a distinct,
      real-looking URL that would otherwise take MAX_PATH_PREFIX_VISITS
      requests to notice (see is_repeated_pattern_trap)
    This is a FIRST line of defense; a later module adds stateful/content-based
    trap detection on top of this.
    """
    if len(urlunparse(parsed_url)) > MAX_URL_LENGTH:
        return True

    segments = [s for s in parsed_url.path.split("/") if s]
    if len(segments) > MAX_PATH_SEGMENTS:
        return True

    if segments and max(Counter(segments).values()) > MAX_SEGMENT_REPEATS:
        return True

    if any(DATE_SEGMENT_RE.match(segment) for segment in segments):
        return True

    query_pairs = parse_qsl(parsed_url.query)
    if len(query_pairs) > MAX_QUERY_PARAMS:
        return True

    # Substring match, not exact - catches "eventDate"/"startDate" etc, not
    # just a bare "date" key (seen live: ?eventDate=2026-05-25 on a
    # Tribe-Events-style calendar page).
    if any(trap_key in key.lower() for key, _ in query_pairs for trap_key in TRAP_QUERY_KEYS):
        return True

    return False


def set_crawler_config(config, restart=False):
    """
    Called once at startup (from launch.py) so is_allowed_by_robots() and
    get_sitemap_urls() can fetch robots.txt/sitemap.xml through the cache
    server. Tests that never call this still work: every function below
    fails open (treats robots.txt as "allow everything", sitemap as
    empty) when _crawler_config is None, instead of raising.

    restart=True also wipes ANALYTICS_SAVE_FILE, mirroring what Frontier
    already does for frontier.shelve. Without this, _get_analytics_db()
    would silently keep loading old analytics on every run regardless of
    --restart (it only ever checks "does the file exist," never looks at
    restart at all) - meaning a "fresh start" would quietly keep adding
    to a stale, possibly-corrupted word_freq/unique_pages from a previous
    run instead of actually starting over. Backs the file up first (see
    backup_analytics_db) so a --restart never destroys the only copy of
    a crawl's analytics without a recovery path.
    """
    global _crawler_config, _analytics_db
    _crawler_config = config

    if restart and os.path.exists(ANALYTICS_SAVE_FILE):
        backup_analytics_db()
        os.remove(ANALYTICS_SAVE_FILE)
    _analytics_db = None


def backup_analytics_db():
    """
    Copy the current ANALYTICS_SAVE_FILE to a timestamped backup under
    ANALYTICS_BACKUP_DIR, if it exists. Returns the backup path, or None
    if there was no live analytics.pkl to back up. Safe to call any time,
    not just from set_crawler_config - e.g. before manually deleting
    analytics.pkl yourself.
    """
    if not os.path.exists(ANALYTICS_SAVE_FILE):
        return None

    os.makedirs(ANALYTICS_BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(
        ANALYTICS_BACKUP_DIR, f"analytics_backup_{timestamp}.pkl")
    shutil.copy2(ANALYTICS_SAVE_FILE, backup_path)
    return backup_path


def find_latest_analytics_backup():
    """
    Return the path of the most recently created analytics backup under
    ANALYTICS_BACKUP_DIR (by file modification time), or None if there
    isn't one.
    """
    backups = glob.glob(os.path.join(ANALYTICS_BACKUP_DIR, ANALYTICS_BACKUP_PATTERN))
    if not backups:
        return None
    return max(backups, key=os.path.getmtime)


def _fetch_text_via_cache(url):
    """
    Download `url` through the cache server - utils.download.download,
    the exact function Worker uses for every page - and return its
    decoded text, or None if config isn't set, the fetch failed, or the
    status wasn't 200. Never a direct request: robots.txt/sitemap fetches
    go through the same architecture as everything else.
    """
    if _crawler_config is None:
        return None

    resp = _cache_download(url, _crawler_config)
    if resp.status != 200 or resp.raw_response is None or not resp.raw_response.content:
        return None

    try:
        return resp.raw_response.content.decode("utf-8", errors="replace")
    except Exception:
        return None


def get_robots_parser(parsed_url):
    """
    Return a urllib.robotparser.RobotFileParser for parsed_url's domain,
    fetching <scheme>://<netloc>/robots.txt through the cache the first
    time this domain is seen (cached per-netloc afterwards so it's only
    ever fetched once per domain). Feeds the fetched text to .parse() -
    never RobotFileParser.read(), which would fetch directly with
    urllib.request and bypass the cache.
    Returns None if robots.txt couldn't be fetched - fail open (treat as
    allowed), the same way a browser does when robots.txt is missing.
    """
    netloc = parsed_url.netloc
    if netloc in _robots_parsers:
        return _robots_parsers[netloc]

    robots_url = f"{parsed_url.scheme}://{netloc}/robots.txt"
    text = _fetch_text_via_cache(robots_url)

    parser = None
    if text is not None:
        parser = urllib.robotparser.RobotFileParser()
        parser.parse(text.splitlines())

    _robots_parsers[netloc] = parser
    return parser


def is_allowed_by_robots(url):
    """
    True iff robots.txt for url's domain allows our user agent to fetch
    it, or robots.txt couldn't be fetched at all (fail open). The cache
    server already enforces robots.txt on its own (status 608, "Denied by
    domain robot rules") - checking it ourselves here just means we never
    waste a request on a URL it would have refused anyway.
    """
    parsed = urlparse(url)
    parser = get_robots_parser(parsed)
    if parser is None:
        return True

    user_agent = _crawler_config.user_agent if _crawler_config else "*"
    return parser.can_fetch(user_agent, url)


def _extract_sitemap_locs(xml_text):
    """
    Return every <loc> URL in a sitemap or sitemap-index XML document.
    Namespace-agnostic (sitemap XML almost always declares the
    http://www.sitemaps.org/schemas/sitemap/0.9 namespace, which turns
    ElementTree tag names into "{<namespace>}loc" - match on the local
    name instead of requiring an exact namespace string).
    """
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    return [
        el.text.strip()
        for el in root.iter()
        if el.tag.rsplit("}", 1)[-1] == "loc" and el.text and el.text.strip()
    ]


def get_sitemap_urls(parsed_url):
    """
    Fetch and parse this domain's sitemap, ONCE per domain: use the
    Sitemap: directive(s) from robots.txt if present, else fall back to
    <scheme>://<netloc>/sitemap.xml. Handles one level of sitemap-index
    nesting (a sitemap whose <loc> entries point to further *.xml
    sitemaps, rather than pages). Returns the list of page URLs found, or
    [] if no sitemap exists or config isn't set - these are meant to be
    added to the frontier as an extra discovery path, since sitemaps
    often list pages nothing else links to.
    """
    netloc = parsed_url.netloc
    scheme = parsed_url.scheme

    parser = get_robots_parser(parsed_url)
    sitemap_urls = list(parser.site_maps()) if parser and parser.site_maps() else [
        f"{scheme}://{netloc}/sitemap.xml"]

    page_urls = []
    for sitemap_url in sitemap_urls:
        text = _fetch_text_via_cache(sitemap_url)
        if text is None:
            continue

        locs = _extract_sitemap_locs(text)
        # A sitemap INDEX lists further *.xml sitemaps, not pages - fetch
        # one level of those too rather than treating them as page URLs.
        sub_sitemaps = [loc for loc in locs if loc.lower().endswith(".xml")]
        page_urls.extend(loc for loc in locs if loc not in sub_sitemaps)
        for sub_sitemap_url in sub_sitemaps:
            sub_text = _fetch_text_via_cache(sub_sitemap_url)
            if sub_text is not None:
                page_urls.extend(_extract_sitemap_locs(sub_text))

    return page_urls


def is_valid(url):
    """
    Decide whether to crawl this url or not.
    Combine the checks above: scheme check, domain check, extension check,
    trap heuristics, robots.txt. Return True only if url should be added
    to the frontier.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        if not is_in_allowed_domain(parsed):
            return False

        if has_disallowed_extension(parsed):
            return False

        if is_likely_trap(parsed):
            return False

        if not is_allowed_by_robots(url):
            return False

        return True

    except TypeError:
        print ("TypeError for ", parsed)
        raise


def is_repeated_pattern_trap(parsed_url):
    """
    Catches traps no single URL reveals on its own (is_likely_trap above only
    looks at one URL in isolation). Buckets every candidate link by
    (netloc, first path segment) and tracks the set of DISTINCT URLs seen in
    that bucket, returning True once it exceeds MAX_PATH_PREFIX_VISITS - e.g.
    a calendar or faceted-search section generating thousands of distinct-
    looking but low-value URLs under the same section. Counting distinct
    URLs (not raw hits) means a single popular page linked from hundreds of
    other pages doesn't falsely look like a trap.
    Side effect: adds to _path_prefix_seen_urls. Only called from scraper(),
    once per candidate link - see the comment in scraper().
    """
    segments = [s for s in parsed_url.path.split("/") if s]
    first_segment = segments[0] if segments else ""
    bucket_key = (parsed_url.netloc.lower(), first_segment)

    bucket = _path_prefix_seen_urls[bucket_key]
    bucket.add(urlunparse(parsed_url))

    return len(bucket) > MAX_PATH_PREFIX_VISITS


def _get_visible_text(soup):
    """
    Walk text nodes directly rather than calling soup.get_text() on the
    whole tree, so we don't have to mutate/copy `soup` -
    extract_next_links() still needs the original tree intact for
    find_all("a") afterwards. Shared by get_page_word_count (which just
    counts tokens) and extract_content_words (which keeps the actual
    lowercased, stopword-filtered words) so both stay consistent.
    """
    visible_text_parts = []
    for node in soup.find_all(string=True):
        if isinstance(node, Comment):
            continue
        # find_parent checks ALL ancestors, not just the immediate parent -
        # e.g. text inside <head><title>...</title></head> has an immediate
        # parent of "title", but "head" is still an ancestor.
        if node.find_parent(NON_VISIBLE_TAGS) is not None:
            continue
        visible_text_parts.append(str(node))

    return " ".join(visible_text_parts)


def get_page_word_count(soup):
    """
    Given a BeautifulSoup-parsed page, return the number of actual words in
    the visible text. HTML markup must NOT count as words: drop
    <script>/<style> (and anything else non-visible) before calling
    .get_text(), then split on whitespace and count. Module 5's "longest
    page" analytic will reuse this exact function.
    """
    return len(WORD_RE.findall(_get_visible_text(soup)))


def is_dead_or_low_value_page(soup):
    """
    True iff this 200-status page has essentially no real content - i.e.
    get_page_word_count(soup) is below MIN_WORDS_FOR_REAL_PAGE. This is the
    function you'll point to in the TA interview when asked to define "low
    information value." (If you do the near-duplicate-detection extra
    credit, boilerplate/template detection belongs here too.)
    """
    return get_page_word_count(soup) < MIN_WORDS_FOR_REAL_PAGE


def compute_simhash(soup):
    """
    Extra credit (+2): from-scratch 64-bit SimHash fingerprint of this
    page's visible text, built from scratch (no similarity/hashing
    libraries - Python's built-in hash() is used only as a cheap integer-
    hashing primitive, the same role hashlib plays for unrelated URL
    hashing elsewhere in this project, not as "the" similarity algorithm).

    For each distinct content word (via extract_content_words), weighted
    by how many times it occurs on the page: hash the word to a 64-bit
    value, then for every one of the 64 bit positions, add the word's
    weight to a running total if that bit is 1, subtract it if that bit
    is 0. The final fingerprint's bit i is 1 if that total ended up
    positive, 0 otherwise.

    Two pages that share most of their words end up with fingerprints
    differing in only a handful of bits; two unrelated pages differ in
    roughly half of the 64 bits (by the properties of a good hash
    function) - that's what makes Hamming distance a useful similarity
    measure between two fingerprints.
    """
    word_counts = Counter(extract_content_words(soup))
    bit_totals = [0] * SIMHASH_BITS

    for word, weight in word_counts.items():
        word_hash = hash(word) & ((1 << SIMHASH_BITS) - 1)  # fold to 64 unsigned bits
        for bit in range(SIMHASH_BITS):
            if (word_hash >> bit) & 1:
                bit_totals[bit] += weight
            else:
                bit_totals[bit] -= weight

    fingerprint = 0
    for bit in range(SIMHASH_BITS):
        if bit_totals[bit] > 0:
            fingerprint |= (1 << bit)
    return fingerprint


def hamming_distance(a, b):
    """Number of bit positions where two SimHash fingerprints differ."""
    return bin(a ^ b).count("1")


def is_near_duplicate_of_recent_pages(soup):
    """
    Extra credit (+2): True iff this page's SimHash fingerprint is within
    NEAR_DUP_HAMMING_THRESHOLD bits of ANY fingerprint among the last
    NEAR_DUP_WINDOW pages seen - i.e. near-identical content to something
    just crawled (a chain of boilerplate-heavy or templated pages adding
    no new information). Deliberately compares against a bounded RECENT
    window, not the entire crawl history, per the assignment's own Note 4
    hint ("content similarity repetition over a certain amount of chained
    pages") - full O(n) comparison against every page ever seen would get
    slower as the crawl grows; a bounded window stays cheap throughout.

    Always records this page's own fingerprint afterwards regardless of
    the result, so a long chain of near-duplicates is bounded by the
    comparison window rather than only ever compared against the first
    page in the chain.
    """
    fingerprint = compute_simhash(soup)
    is_duplicate = any(
        hamming_distance(fingerprint, seen) <= NEAR_DUP_HAMMING_THRESHOLD
        for seen in _recent_simhashes)
    _recent_simhashes.append(fingerprint)
    return is_duplicate


def resolve_base_url(url, resp):
    """
    Return the URL that relative links on the page should be resolved
    against (and, in Module 5, the canonical URL to record stats against):
    if resp.raw_response.url differs from the requested `url`, the server
    redirected you mid-fetch, and the page you actually got lives at
    raw_response.url - that's what relative hrefs on it are relative to,
    not the URL you originally requested.
    """
    if resp.raw_response is not None and getattr(resp.raw_response, "url", None):
        return resp.raw_response.url
    return resp.url or url


def _get_analytics_db():
    """
    Lazily load (or create) the dict that persists report analytics across
    restarts. Returns a plain dict with keys:
      "unique_pages"     -> set of canonical (defragmented) URLs
      "longest_page"     -> (url, word_count) tuple, the current max
      "word_freq"        -> Counter of lowercased content word -> count
      "subdomain_counts" -> Counter of ics.uci.edu subdomain host -> count
    Call _save_analytics_db() (or the throttled/flush variants below)
    after any mutation to persist it - this dict is plain in-memory
    otherwise, nothing autosaves it.
    """
    global _analytics_db
    if _analytics_db is None:
        if os.path.exists(ANALYTICS_SAVE_FILE):
            with open(ANALYTICS_SAVE_FILE, "rb") as f:
                _analytics_db = pickle.load(f)
        else:
            _analytics_db = {
                "unique_pages": set(),
                "longest_page": (None, 0),
                "word_freq": Counter(),
                "subdomain_counts": Counter(),
            }

    return _analytics_db


def _save_analytics_db():
    """
    Atomically persist the analytics dict to ANALYTICS_SAVE_FILE: write to
    a temp file, then os.replace() it into place. os.replace is atomic on
    POSIX, so a kill mid-write leaves the previous, still-valid file in
    place rather than a half-written one.
    """
    tmp_path = ANALYTICS_SAVE_FILE + ".tmp"
    with open(tmp_path, "wb") as f:
        pickle.dump(_get_analytics_db(), f)
    os.replace(tmp_path, ANALYTICS_SAVE_FILE)


def _save_analytics_db_throttled():
    """
    Save at most once every ANALYTICS_SAVE_INTERVAL calls, instead of
    every single one - see the comment above ANALYTICS_SAVE_INTERVAL for
    why. Called from record_page_visit; use _save_analytics_db() directly
    (or flush_analytics_db()) when you need an unconditional save.
    """
    global _pages_since_last_save
    _pages_since_last_save += 1
    if _pages_since_last_save >= ANALYTICS_SAVE_INTERVAL:
        _save_analytics_db()
        _pages_since_last_save = 0


def flush_analytics_db():
    """
    Force an unconditional save regardless of ANALYTICS_SAVE_INTERVAL
    batching. Called once from Worker.run(), right after its loop exits
    (frontier empty OR shutdown requested) - so neither a normal
    completion nor a graceful Ctrl+C/kill ever loses the last few pages'
    analytics to the save-interval batching above; only a hard crash
    (SIGKILL) can still lose up to ANALYTICS_SAVE_INTERVAL - 1 pages.
    """
    global _pages_since_last_save
    if _analytics_db is not None:
        _save_analytics_db()
    _pages_since_last_save = 0


def get_ics_subdomain(parsed_url):
    """
    Return the ics.uci.edu subdomain host for parsed_url (e.g.
    "vision.ics.uci.edu"), or None if parsed_url isn't under ics.uci.edu at
    all. Used to build the per-subdomain unique-page counts the report
    asks for - only ics.uci.edu subdomains count, not cs/informatics/stat.
    """
    host = parsed_url.netloc.lower()
    host = host.rsplit("@", 1)[-1]  # drop userinfo, if any
    host = host.rsplit(":", 1)[0]   # drop port, if any

    if host == "ics.uci.edu" or host.endswith(".ics.uci.edu"):
        return host
    return None


def extract_content_words(soup):
    """
    Like get_page_word_count, but returns the actual lowercased word list
    (with STOPWORDS filtered out) for the word-frequency table, instead of
    just a count. Reuse the same text-extraction approach (skip
    script/style/head/comments) so the two stay consistent.
    """
    words = WORD_RE.findall(_get_visible_text(soup).lower())
    return [word for word in words if word not in STOPWORDS]


def record_page_visit(url, soup):
    """
    Update the persisted analytics for one newly-downloaded page:
    - add `url` to the unique-page set
    - update the longest-page record if get_page_word_count(soup) beats
      the current max
    - add this page's content words (via extract_content_words) to the
      global word-frequency table
    - if get_ics_subdomain(urlparse(url)) is not None, count this page
      under that subdomain
    Called once per successfully-parsed HTML page from extract_next_links,
    BEFORE the dead-page check - a thin page still counts as a page you
    found, even if you don't crawl further from it.
    """
    db = _get_analytics_db()

    # Uniqueness for the report is defined by URL with the fragment
    # discarded, same rule as defragment_url/normalize_url use everywhere
    # else - a page already passed through normalize_url is already
    # fragment-free, but defragment again here so this stays correct even
    # if called with a raw url.
    canonical_url = defragment_url(url)
    db["unique_pages"].add(canonical_url)

    word_count = get_page_word_count(soup)
    _, longest_count = db["longest_page"]
    if word_count > longest_count:
        db["longest_page"] = (canonical_url, word_count)

    for word in extract_content_words(soup):
        db["word_freq"][word] += 1

    subdomain = get_ics_subdomain(urlparse(canonical_url))
    if subdomain is not None:
        db["subdomain_counts"][subdomain] += 1

    _save_analytics_db_throttled()


def generate_report(output_path="report.txt", analytics_path=None):
    """
    NOT called by the crawler itself - run this once (or periodically, to
    check progress) to produce the plain-text report answering all four
    assignment questions from the persisted analytics: unique page count,
    the longest page (URL + word count), the 50 most common words by
    frequency, and the alphabetically-sorted list of ics.uci.edu
    subdomains with their unique-page counts.
    e.g. python3 -c "from scraper import generate_report; generate_report()"

    analytics_path: if given, load analytics from this file instead of
    the live ANALYTICS_SAVE_FILE - used by regenerate_report_from_backup
    to rebuild a lost report.txt from a backup snapshot without touching
    the live crawl's in-memory analytics state.
    """
    if analytics_path is not None:
        with open(analytics_path, "rb") as f:
            db = pickle.load(f)
    else:
        db = _get_analytics_db()
    longest_url, longest_count = db["longest_page"]

    lines = []
    lines.append("1. Unique pages found: {}".format(len(db["unique_pages"])))
    lines.append("")
    lines.append("2. Longest page: {} ({} words)".format(longest_url, longest_count))
    lines.append("")
    lines.append("3. 50 most common words:")
    for word, count in db["word_freq"].most_common(50):
        lines.append("{} - {}".format(word, count))
    lines.append("")
    lines.append("4. Subdomains found in ics.uci.edu:")
    for subdomain in sorted(db["subdomain_counts"]):
        lines.append("http://{}, {}".format(subdomain, db["subdomain_counts"][subdomain]))

    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    return output_path


def regenerate_report_from_backup(backup_path=None, output_path="report.txt"):
    """
    Rebuild report.txt (or another output_path) from a backed-up
    analytics.pkl snapshot - for when report.txt (and/or the live
    analytics.pkl) has been lost or deleted, but a backup made by
    backup_analytics_db() still exists.

    If backup_path isn't given, uses the most recently created backup
    found by find_latest_analytics_backup(). Raises FileNotFoundError if
    no backup path is given and none can be found.

    e.g. python3 -c "from scraper import regenerate_report_from_backup; regenerate_report_from_backup()"
    """
    if backup_path is None:
        backup_path = find_latest_analytics_backup()
    if backup_path is None:
        raise FileNotFoundError(
            "No analytics backup found under {} to regenerate a report "
            "from.".format(ANALYTICS_BACKUP_DIR))

    return generate_report(output_path=output_path, analytics_path=backup_path)
