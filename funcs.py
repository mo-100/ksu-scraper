import os
import csv
from typing import Set, Deque, Optional
from urllib import parse
from collections import deque
import pandas as pd
from bs4 import BeautifulSoup

# --- Constants ---
INDEX_FILE = "index.csv"
FILES_DIR = "files"
QUEUE_FILE = "crawl_queue.txt"

# --- Pure Functions (or close approximations) ---

def get_host_from_url(url: str) -> str:
    """
    Extracts the network location (hostname) from a URL. Pure function.

    Args:
        url: The URL string.

    Returns:
        The hostname part (e.g., 'www.example.com'). Returns an empty string
        if the URL is malformed or has no network location.
    """
    try:
        parsed_url = parse.urlparse(url)
        return parsed_url.netloc
    except ValueError:
        # Handle potential errors if url is severely malformed, although urlparse is robust
        return ""

def is_likely_html_page(url: str) -> bool:
    """
    Checks if a URL likely points to an HTML page based on its path extension.
    Handles common non-HTML file types. Pure function.

    Args:
        url: The URL string.

    Returns:
        True if it seems like an HTML link, False otherwise (e.g., PDF, JPG).
    """
    try:
        path = parse.urlparse(url).path
        if not path or path == '/':  # Root path is usually HTML
            return True

        # Get the part after the last dot in the last segment of the path
        last_segment = path.split('/')[-1]
        if '.' in last_segment:
            extension = last_segment.split('.')[-1].lower()
            # List of common non-HTML extensions
            non_html_extensions = {
                'jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'webp',  # Images
                'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'ppts', 'psd', 'rdf', 'm4v',  # Documents
                'zip', 'rar', 'gz', 'tar', '7z',  # Archives
                'css', 'js',  # Web resources (often linked, but not crawled for content)
                'xml', 'json', 'csv', 'txt',  # Data formats
                'mp3', 'mp4', 'avi', 'mov', 'wav',  # Media
                'exe', 'dmg', 'iso' # Executables/Images
            }
            if extension in non_html_extensions:
                return False
        # No extension or unrecognized extension - assume it *might* be HTML
        return True
    except ValueError:
        return False # Malformed URL is unlikely to be HTML

def normalize_and_clean_url(href: str, base_url: str) -> Optional[str]:
    """
    Takes a potentially relative href and joins it with the base URL,
    cleans it (removes fragment, optionally query/params), and validates the scheme.
    Pure function.

    Args:
        href: The href string from an anchor tag.
        base_url: The absolute URL of the page where the href was found.

    Returns:
        A cleaned, absolute URL string, or None if invalid/unusable.
    """
    try:
        # Create absolute URL (handles relative paths, // links etc.)
        absolute_url = parse.urljoin(base_url, href.strip())

        # Parse the absolute URL
        url_parts = parse.urlparse(absolute_url)

        # Basic validation: scheme and network location must exist
        if not url_parts.scheme in ['http', 'https'] or not url_parts.netloc:
            return None

        # Clean the URL: remove fragment, keep path, params, query
        # Modify here if you want to remove params or query: ['', '', ...]
        clean_url = parse.urlunparse((
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            '', # Remove params
            '', # Remove query
            '',  # Remove fragment
        ))

        # Optional: remove trailing slash for consistency, unless it's just the domain
        if len(clean_url) > len(f"{url_parts.scheme}://{url_parts.netloc}") + 1 and clean_url.endswith('/'):
             clean_url = clean_url.rstrip('/')

        return clean_url

    except ValueError:
        # urljoin or urlparse might fail on severely malformed hrefs/base_urls
        return None

def is_url_valid_for_host(url: str, required_host_substring: str) -> bool:
    """
    Checks if a URL is likely HTML and belongs to the specified host domain.
    Pure function.

    Args:
        url: The absolute, cleaned URL to check.
        required_host_substring: The substring that must be present in the URL's host.

    Returns:
        True if the URL is valid for crawling, False otherwise.
    """
    if not url: # Handle None case from normalization
        return False

    host = get_host_from_url(url)
    if not host: # Should not happen if normalize_and_clean_url worked, but safer
        return False

    # Case-insensitive check for host inclusion and HTML likelihood
    return (required_host_substring.lower() in host.lower() and
            is_likely_html_page(url))


def extract_html_text(soup: BeautifulSoup) -> str:
    """
    Extracts all human-readable text content from a BeautifulSoup object.
    Removes script/style tags and normalizes whitespace.
    Effectively pure for its purpose (input soup -> output string).

    Args:
        soup: A BeautifulSoup object representing the parsed HTML.

    Returns:
        A string containing the extracted text.
    """
    # Remove script and style elements
    for script_or_style in soup(["script", "style"]):
        script_or_style.extract()

    # Get text and normalize whitespace
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  ")) # Split by double spaces too
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text

def extract_valid_urls(soup: BeautifulSoup, base_url: str, required_host_substring: str) -> Set[str]:
    """
    Finds all valid, absolute URLs within the desired host from a BeautifulSoup object.

    Args:
        soup: The BeautifulSoup object for the page.
        base_url: The absolute URL of the page being parsed.
        required_host_substring: The substring the host of found URLs must contain.

    Returns:
        A set of valid, absolute URL strings.
    """
    valid_urls: Set[str] = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
            continue

        cleaned_url = normalize_and_clean_url(href, base_url)

        if cleaned_url and is_url_valid_for_host(cleaned_url, required_host_substring):
            valid_urls.add(cleaned_url)

    return valid_urls


# --- Functions with Side Effects (File I/O, State) ---

def get_max_index(filename: str = INDEX_FILE) -> int:
    """
    Gets the maximum index value from the 'index' column of the index CSV.

    Returns:
        The maximum index found, or 0 if the file doesn't exist, is empty,
        or has no valid integer indices.
    """
    if not os.path.exists(filename):
        return 0
    try:
        df = pd.read_csv(filename)
        if df.empty or 'index' not in df.columns:
            return 0
        # Attempt conversion to numeric, coercing errors to NaN, then drop NaNs and find max
        numeric_indices = pd.to_numeric(df['index'], errors='coerce').dropna()
        if numeric_indices.empty:
            return 0
        return int(numeric_indices.max()) # Convert float max back to int
    except pd.errors.EmptyDataError:
        return 0 # File exists but is empty
    except Exception as e:
        print(f"Error reading max index from '{filename}': {e}")
        return 0 # Return safe default on unexpected errors

def url_exists_in_index(url: str, filename: str = INDEX_FILE) -> bool:
    """Checks if a URL already exists in the 'url' column of the index CSV."""
    if not os.path.exists(filename):
        return False
    try:
        df = pd.read_csv(filename)
        if df.empty or 'url' not in df.columns:
            return False
        return url in df['url'].values
    except pd.errors.EmptyDataError:
        return False # File exists but is empty
    except Exception as e:
        print(f"Error checking URL existence in '{filename}': {e}")
        return False # Be cautious on error, assume it doesn't exist to allow potential processing

def write_content_file(content: str, index: int, url: str,
                       index_filename: str = INDEX_FILE,
                       content_dir: str = FILES_DIR) -> bool:
    """
    Writes content to a text file named '{index}.txt' inside 'content_dir'
    and adds an entry to the index CSV *only if* the URL is not already in the index.

    Args:
        content: The text content to write.
        index: The integer index to use for the filename and index entry.
        url: The source URL of the content.
        index_filename: Path to the index CSV file.
        content_dir: Directory to save the content files.

    Returns:
        True if the file was newly written and index updated, False if the URL
        already existed in the index or an error occurred.
    """
    # Check if URL already processed before doing any writing
    if url_exists_in_index(url, index_filename):
        # print(f"Debug: URL {url} already in index, skipping write.") # Optional debug
        return False

    try:
        # Ensure content directory exists
        os.makedirs(content_dir, exist_ok=True)

        # Create the full path for the content file
        file_path = os.path.join(content_dir, f"{index}.txt")

        # Write the content to the file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        # Append the new entry to the index CSV
        with open(index_filename, "a", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([index, file_path, url])

        return True # Success

    except OSError as e:
        print(f"Error writing file/index for URL '{url}': {e}")
        # Clean up potentially created file if index write fails? Maybe not necessary.
        return False
    except Exception as e:
        print(f"Unexpected error writing file/index for URL '{url}': {e}")
        return False

def load_queue_from_file(filename: str = QUEUE_FILE) -> Deque[str]:
    """Loads the crawl queue from a text file (one URL per line)."""
    if not os.path.exists(filename):
        return deque()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
            return deque(urls)
    except Exception as e:
        print(f"Error loading queue file '{filename}': {e}. Starting fresh.")
        return deque()

def save_queue_to_file(queue: Deque[str], filename: str = QUEUE_FILE):
    """Saves the crawl queue to a text file (one URL per line)."""
    # Ensure parent directory exists if filename includes path separators
    try:
        os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            for url in queue:
                f.write(url + '\n')
    except OSError as e:
        print(f"Error saving queue to '{filename}': {e}")
    except Exception as e:
        print(f"Unexpected error saving queue to '{filename}': {e}")