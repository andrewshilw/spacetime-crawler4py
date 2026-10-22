import re
from collections import Counter
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl

from bs4 import BeautifulSoup

ALLOWED_DOMAINS = (
    "ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu",
)

MAX_CONTENT_SIZE = 5 * 1024 * 1024  # skip pages whose body is bigger than this

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
    return [link for link in links if is_valid(link)]


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

    base_url = resp.raw_response.url or resp.url or url

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
