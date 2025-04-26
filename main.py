from crawler import Crawler

def main():
    # --- Configuration ---
    START_URLS = ['https://ksu.edu.sa']
    TARGET_HOST_SUBSTRING = 'ksu.edu.sa' # Substring to keep crawling within
    REQUEST_DELAY = 0 # Seconds between requests

    print("--- Starting Crawler ---")
    print(f"Initial URLs: {START_URLS}")
    print(f"Target Host Substring: {TARGET_HOST_SUBSTRING}")
    print(f"Delay: {REQUEST_DELAY}")

    # --- Initialization & Run ---
    try:
        crawler = Crawler(host_includes=TARGET_HOST_SUBSTRING, initial_urls=START_URLS)
        crawler.run(delay_seconds=REQUEST_DELAY)
        print("--- Crawler finished normally ---")
    except ValueError as ve:
         print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred during crawler execution: {e}")
    
if __name__ == "__main__":
    main()