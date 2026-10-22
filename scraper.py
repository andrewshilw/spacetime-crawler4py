import re
import shelve
import atexit
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl

from bs4 import BeautifulSoup, Comment

# Module 5: persisted analytics for the report. A shelve file, like
# Frontier's own save file, so a restart during deployment (up to 3 allowed)
# doesn't throw away report progress.
ANALYTICS_SAVE_FILE = "analytics.shelve"
_analytics_db = None  # lazily opened - see _get_analytics_db

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

    content_type = resp.raw_response.headers.get("Content-Type", "")
    if content_type and "html" not in content_type.lower():
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

    query_pairs = parse_qsl(parsed_url.query)
    if len(query_pairs) > MAX_QUERY_PARAMS:
        return True

    if any(key.lower() in TRAP_QUERY_KEYS for key, _ in query_pairs):
        return True

    return False


def is_valid(url):
    """
    Decide whether to crawl this url or not.
    Combine the checks above: scheme check, domain check, extension check,
    trap heuristics. Return True only if url should be added to the frontier.
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
    Lazily open (or create) the shelve file that persists report analytics
    across restarts, the same way Frontier persists frontier.shelve.
    Returns a dict-like object with keys:
      "unique_pages"     -> set of canonical (defragmented) URLs
      "longest_page"     -> (url, word_count) tuple, the current max
      "word_freq"        -> Counter of lowercased content word -> count
      "subdomain_counts" -> Counter of ics.uci.edu subdomain host -> count
    Initialize these four keys with setdefault the first time the shelve
    file is created. Remember to call .sync() after any mutation (shelve
    does not autosave), the same way Frontier.add_url does.
    """
    global _analytics_db
    if _analytics_db is None:
        # writeback=True so in-place mutations to the cached set/Counter
        # values (e.g. db["unique_pages"].add(url)) get persisted on
        # sync()/close() without having to reassign db[key] every time.
        _analytics_db = shelve.open(ANALYTICS_SAVE_FILE, writeback=True)
        _analytics_db.setdefault("unique_pages", set())
        _analytics_db.setdefault("longest_page", (None, 0))
        _analytics_db.setdefault("word_freq", Counter())
        _analytics_db.setdefault("subdomain_counts", Counter())
        _analytics_db.sync()
        # Close explicitly at normal interpreter exit. Without this,
        # Shelf.__del__ tries to flush-and-close during interpreter
        # shutdown, by which point some builtin modules it needs for
        # pickling may already be torn down - harmless (record_page_visit
        # already .sync()s after every write) but it prints an ugly
        # traceback on every run.
        atexit.register(_analytics_db.close)

    return _analytics_db


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

    db.sync()


def generate_report(output_path="report.txt"):
    """
    NOT called by the crawler itself - run this once (or periodically, to
    check progress) to produce the plain-text report answering all four
    assignment questions from the persisted analytics: unique page count,
    the longest page (URL + word count), the 50 most common words by
    frequency, and the alphabetically-sorted list of ics.uci.edu
    subdomains with their unique-page counts.
    e.g. python3 -c "from scraper import generate_report; generate_report()"
    """
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
