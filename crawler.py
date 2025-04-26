import threading
import time
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from typing import List, Set, Optional, Deque

# Import the separated utility functions
from funcs import (
    FILES_DIR, QUEUE_FILE, get_max_index, url_exists_in_index,
    write_content_file, load_queue_from_file, save_queue_to_file,
    extract_html_text, extract_valid_urls, is_url_valid_for_host,
)


class Crawler:
    """
    Orchestrates the web crawling process, managing the queue and interacting
    with helper functions for processing and persistence. Uses parallel execution
    to improve performance.
    """
    
    def __init__(self, host_includes: str, initial_urls: List[str], max_workers: int = 10):
        """
        Initializes the Crawler.

        Args:
            host_includes: Substring that crawled URLs' hosts must contain.
            initial_urls: A list of starting URLs for the crawl.
            max_workers: Maximum number of parallel workers for crawling.
        """

        self.host_includes: str = host_includes
        self.current_index: int = get_max_index() + 1
        self.max_workers: int = max_workers

        # Load the queue from persistent storage
        self.queue: Deque[str] = load_queue_from_file()

        # Keep track of URLs currently in the queue or being processed
        # to avoid adding duplicates during *this* run.
        self.urls_in_session: Set[str] = set(self.queue)

        # Lock for thread-safe operations on shared resources
        self.lock = threading.RLock()

        # Initialize queue if it was empty
        self._initialize_queue(initial_urls)

    def _initialize_queue(self, initial_urls: List[str]):
        """Populates the queue with valid initial URLs if it's empty."""
        if not self.queue:
            print("Queue is empty. Initializing with provided URLs.")
            for url in initial_urls:
                # Check if the initial URL itself is valid and not already processed
                if (is_url_valid_for_host(url, self.host_includes) and
                        not url_exists_in_index(url)):
                    if url not in self.urls_in_session:
                        self.queue.append(url)
                        self.urls_in_session.add(url)
                        print(f"Added initial URL to queue: {url}")
                else:
                     print(f"Skipping invalid or already processed initial URL: {url}")

    def _fetch_page(self, url: str, timeout: int = 10) -> Optional[BeautifulSoup]:
        """
        Fetches the content of a URL and returns a BeautifulSoup object.
        Handles network errors. Returns None on failure.
        """
        print(f"Fetching: {url}")
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            # Use 'html.parser' for built-in, lxml is faster if installed
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except requests.exceptions.Timeout:
            print(f"Timeout error fetching {url}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch {url}. Error: {e}")
            return None
        except Exception as e:
            # Catch other potential errors during request/parsing (less common)
            print(f"Unexpected error fetching/parsing {url}: {e}")
            return None

    def _process_url(self, url: str, delay_seconds: float = 0.1) -> None:
        """
        Processes a single URL by fetching, extracting content and links,
        and updating the queue.
        
        Args:
            url: The URL to process
            delay_seconds: Optional delay between requests to be polite.
        """
        # --- Check if already processed ---
        if url_exists_in_index(url):
            print(f"Skipping already processed URL (found in index): {url}")
            return

        # --- Fetch Page ---
        soup = self._fetch_page(url)
        if soup is None:
            # Error message already printed by _fetch_page
            return

        # --- Process Content ---
        text_content = extract_html_text(soup)

        # --- Persist Content and Index ---
        with self.lock:
            # Thread-safe access to current_index
            current_index = self.current_index
            self.current_index += 1
        
        # write_content_file also checks url_exists_in_index internally
        file_written = write_content_file(
            content=text_content,
            index=current_index,
            url=url
        )

        if file_written:
            print(f" -> Saved content to '{FILES_DIR}/{current_index}.txt'")

            # --- Extract and Enqueue New URLs ---
            new_urls = extract_valid_urls(soup, url, self.host_includes)
            print(f" -> Found {len(new_urls)} potentially new valid URLs.")

            added_count = 0
            with self.lock:
                for new_url in new_urls:
                    # Add if not already processed (check index) AND not already in queue/session
                    if new_url not in self.urls_in_session and not url_exists_in_index(new_url):
                        self.urls_in_session.add(new_url)
                        self.queue.append(new_url)
                        added_count += 1
            if added_count > 0:
                print(f" -> Added {added_count} new URLs to the queue.")

        # --- Polite Delay ---
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    def run(self, delay_seconds: float = 0.1):
        """
        Starts and manages the iterative crawling process with parallel execution.

        Args:
            delay_seconds: Optional delay between requests to be polite.
        """
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                while True:
                    # Get a batch of URLs to process
                    urls_to_process = []
                    with self.lock:
                        # Get up to max_workers URLs from queue
                        for _ in range(min(self.max_workers - len(futures), len(self.queue))):
                            if self.queue:
                                urls_to_process.append(self.queue.popleft())
                    
                    # If no URLs to process and no pending futures, we're done
                    if not urls_to_process and not futures:
                        break
                    
                    # Submit new tasks for the URLs
                    for url in urls_to_process:
                        future = executor.submit(self._process_url, url, delay_seconds)
                        futures.append(future)
                    
                    # Check for completed futures
                    completed = []
                    for future in futures:
                        if future.done():
                            completed.append(future)
                    
                    # Remove completed futures from the tracking list
                    for future in completed:
                        futures.remove(future)
                    
                    # If we have no URLs to process but have pending futures,
                    # wait a bit for more work
                    if not urls_to_process and futures:
                        time.sleep(0.1)
                    
                    # Periodically save queue state
                    with self.lock:
                        save_queue_to_file(self.queue)

        except KeyboardInterrupt:
            print("\nInterrupt received. Gracefully shutting down...")
        finally:
            # --- Save Final Queue State ---
            print("\nExiting crawl loop (finished or interrupted). Saving queue...")
            with self.lock:
                save_queue_to_file(self.queue)
            print(f"Queue state saved to '{QUEUE_FILE}'. Processed up to index {self.current_index -1}.")