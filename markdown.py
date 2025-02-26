# markdown.py

import asyncio
from typing import List,Optional,Set
import random
from api_management import get_supabase_client
from utils import generate_unique_name
from apply import URLCrawler
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.async_configs import BrowserConfig

supabase = get_supabase_client()



async def get_fit_markdown_async(url: str, depth: int, max_url: int, nextButton: Optional[str] = None, visited_urls: Optional[Set[str]] = None) -> str:
    """
    Async function using crawl4ai's AsyncWebCrawler to produce raw markdown.
    Ensures it follows internal links at depth=1 and beyond.
    """
    if visited_urls is None:
        visited_urls = set()
    
    print(f"Starting crawl at {url} with depth={depth}, max_url={max_url}, visited={len(visited_urls)}")

    if depth < 0 or len(visited_urls) >= max_url:
        print(f"Stopping crawl: Depth={depth} too low or visited={len(visited_urls)} >= max_url={max_url}")
        return ""

    def get_random_user_agent():
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/92.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.1.2 Safari/537.36",
        ]
        return random.choice(user_agents)

    whole_data = ""
    
    try:
        browser_config = BrowserConfig()
        config = CrawlerRunConfig(
            wait_for_images=True,
            scan_full_page=True,
            cache_mode=CacheMode.BYPASS,
            user_agent=get_random_user_agent(),
            js_code=f"""
                async function clickShowMore() {{
                    const btn = document.querySelector('{nextButton}');
                    if (btn) {{
                        btn.click();
                        await new Promise(r => setTimeout(r, 2000));
                    }}
                }}
                clickShowMore();
            """ if nextButton else "",
            wait_for="js:() => document.readyState === 'complete'",
            delay_before_return_html=2 if nextButton else 0,
            verbose=True
        )

        async with AsyncWebCrawler(config=browser_config) as async_crawler:
            # Fetch the current page
            print(f"Going to: {url}")
            result = await async_crawler.arun(url, config=config)
            if not result.success:
                print(f"Failed to fetch {url}")
                return whole_data
            
            whole_data += result.html
            visited_urls.add(url)
            print(f"Fetched: {url} (Depth: {depth}, URLs processed: {len(visited_urls)}/{max_url})")

            # Stop if limits are hit
            remaining_slots = max_url - len(visited_urls)
            if remaining_slots <= 0 or depth <= 0:
                print(f"Stopping: No slots left ({remaining_slots}) or depth={depth} exhausted")
                return whole_data

            # Get internal links directly from the page
            internal_links = [link["href"] for link in result.links.get("internal", []) if link["href"]]
            print(f"Found {len(internal_links)} internal links: {internal_links}")
            
            # Filter out visited URLs and limit to remaining slots
            discovered_urls = set(internal_links) - visited_urls
            discovered_urls = list(discovered_urls)[:remaining_slots]
            print(f"Filtered to {len(discovered_urls)} URLs to crawl at depth {depth}: {discovered_urls}")

            # Process each discovered URL
            for next_url in sorted(discovered_urls):
                if next_url in visited_urls or len(visited_urls) >= max_url:
                    print(f"Skipping {next_url}: Already visited or max_url={max_url} reached")
                    continue

                print(f"Going to: {next_url}")
                try:
                    next_result = await async_crawler.arun(next_url, config=config)
                    if next_result.success:
                        whole_data += next_result.html
                        visited_urls.add(next_url)
                        print(f"Fetched: {next_url} (Depth: {depth}, URLs processed: {len(visited_urls)}/{max_url})")
                        
                        # Recurse if depth allows
                        if depth > 1 and len(visited_urls) < max_url:
                            print(f"Recursing into {next_url} at depth {depth-1}")
                            recursive_result = await get_fit_markdown_async(
                                next_url,
                                depth - 1,
                                max_url,
                                nextButton,
                                visited_urls
                            )
                            whole_data += recursive_result
                            print(f"Back from recursion at {next_url}, total data length: {len(whole_data)}")
                    else:
                        print(f"Failed to fetch {next_url}")
                except Exception as e:
                    print(f"Error fetching {next_url}: {e}")
                    continue

        print(f"Finished crawl from {url}, total URLs: {len(visited_urls)}, data length: {len(whole_data)}")
        return whole_data

    except Exception as e:
        print(f"Crawl error at {url}: {e}")
        return whole_data



def fetch_fit_markdown(url: str, depth, max_url, nextButton) -> str:
    """
    Synchronous wrapper around get_fit_markdown_async().
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            get_fit_markdown_async(url, depth, max_url, nextButton)
        )
    finally:
        loop.close()


def read_raw_data(unique_name: str) -> str:
    """
    Query the 'scraped_data' table for the row with this unique_name,
    and return the 'raw_data' field.
    """
    response = (
        supabase.table("scraped_data")
        .select("raw_data")
        .eq("unique_name", unique_name)
        .execute()
    )
    data = response.data
    if data and len(data) > 0:
        return data[0]["raw_data"]
    return ""


def save_raw_data(unique_name: str, url: str, raw_data: str) -> None:
    """
    Save or update the row in supabase with unique_name, url, and raw_data.
    If a row with unique_name doesn't exist, it inserts; otherwise it might upsert.
    """
    supabase.table("scraped_data").upsert(
        {"unique_name": unique_name, "url": url, "raw_data": raw_data}, on_conflict="id"
    ).execute()
    BLUE = "\033[34m"
    RESET = "\033[0m"
    print(f"{BLUE}INFO:Raw data stored for {unique_name}{RESET}")


def fetch_and_store_markdowns(urls: List[str], depth, max_url, nextButton) -> List[str]:
    """
    For each URL:
      1) Generate unique_name
      2) Check if there's already a row in supabase with that unique_name
      3) If not found or if raw_data is empty, fetch fit_markdown
      4) Save to supabase
    Return a list of unique_names (one per URL).
    """
    unique_names = []

    for url in urls:
        unique_name = generate_unique_name(url)
        MAGENTA = "\033[35m"
        RESET = "\033[0m"
        # check if we already have raw_data in supabase
        raw_data = read_raw_data(unique_name)
        if raw_data:
            print(
                f"{MAGENTA}Found existing data in supabase for {url} => {unique_name}{RESET}"
            )
        else:
            # fetch fit markdown
            fit_md = fetch_fit_markdown(url, depth, max_url, nextButton)
            save_raw_data(unique_name, url, fit_md)
        unique_names.append(unique_name)

    return unique_names
