import asyncio
from typing import List, Set, Dict, Optional
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from collections import deque

class URLCrawler:
    def __init__(self, config: Dict = None):
        self.config = {
            "max_depth": 1,
            "include_external": False,
            "crawl_sitemap": True,
            "handle_pagination": False,
            "handle_lazy_load": False,
            "concurrent_requests": 10,  # Number of concurrent requests
            "batch_size": 50,          # Process URLs in batches
            **(config or {}),
        }
        self.visited_urls: Set[str] = set()
        self.semaphore = asyncio.Semaphore(self.config["concurrent_requests"])

    def _is_same_domain(self, base_url: str, url: str) -> bool:
        base_domain = urlparse(base_url).netloc
        url_domain = urlparse(url).netloc
        return base_domain == url_domain

    async def _get_sitemap_urls(self, base_url: str) -> Set[str]:
        sitemap_urls = set()
        try:
            async with AsyncWebCrawler() as crawler:
                sitemap_url = urljoin(base_url, "/sitemap.xml")
                result = await crawler.arun(sitemap_url)

                if result.success:
                    root = ET.fromstring(result.html)
                    sitemap_urls.update(
                        url.text for url in root.findall(
                            ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                        )
                    )
        except Exception as e:
            print(f"Error fetching sitemap: {e}")
        return sitemap_urls

    async def _handle_pagination(self, crawler: AsyncWebCrawler, url: str) -> Set[str]:
        config = CrawlerRunConfig(
            wait_for="js:() => document.readyState === 'complete'",
        )
        result = await crawler.arun(url, config=config)
        return {link["href"] for link in result.links.get("internal", [])} if result.success else set()

    async def _handle_lazy_load(self, crawler: AsyncWebCrawler, url: str) -> Set[str]:
        config = CrawlerRunConfig(
            js_code=[
                """
                async function scrollToBottom() {
                    for (let i = 0; i < 3; i++) {
                        window.scrollTo(0, document.body.scrollHeight);
                        await new Promise(r => setTimeout(r, 1000));
                    }
                }
                scrollToBottom();
                """
            ],
            wait_for="js:() => document.readyState === 'complete'",
            delay_before_return_html=3,
        )
        result = await crawler.arun(url, config=config)
        return {link["href"] for link in result.links.get("internal", []) if link["href"]} if result.success else set()

    async def _process_url(self, crawler: AsyncWebCrawler, url: str, start_url: str) -> Set[str]:
        """Process a single URL and return discovered URLs"""
        async with self.semaphore:  # Limit concurrent requests
            discovered_urls = set()
            result = await crawler.arun(url)
            
            if result.success:
                # Add internal links
                discovered_urls.update(link["href"] for link in result.links.get("internal", []))
                
                # Add external links if configured
                if self.config["include_external"]:
                    discovered_urls.update(link["href"] for link in result.links.get("external", []))
                
                # Handle pagination if enabled
                if self.config["handle_pagination"]:
                    pagination_urls = await self._handle_pagination(crawler, url)
                    discovered_urls.update(pagination_urls)
                
                # Handle lazy loading if enabled
                if self.config["handle_lazy_load"]:
                    lazy_urls = await self._handle_lazy_load(crawler, url)
                    discovered_urls.update(lazy_urls)
            
            return discovered_urls

    async def _process_url_batch(self, urls: List[str], start_url: str) -> Set[str]:
        """Process a batch of URLs concurrently"""
        async with AsyncWebCrawler() as crawler:
            tasks = [
                self._process_url(crawler, url, start_url)
                for url in urls
                if url not in self.visited_urls
            ]
            results = await asyncio.gather(*tasks)
            return {url for urls in results for url in urls}  # Flatten results

    async def get_urls(self, start_url: str, depth: int = 0, max_urls: int = 1) -> Set[str]:
        """
        Main method to get a limited number of URLs starting from a given URL
        Returns a set of discovered URLs (up to max_urls)
        """
        if depth > self.config["max_depth"] or start_url in self.visited_urls or len(self.visited_urls) >= max_urls:
            return set()

        self.visited_urls.add(start_url)
        discovered_urls = {start_url}
        urls_to_process = deque([start_url])

        async with AsyncWebCrawler() as crawler:
            # 1. Try sitemap if enabled
            if self.config["crawl_sitemap"] and depth == 0:
                sitemap_urls = await self._get_sitemap_urls(start_url)
                discovered_urls.update(sitemap_urls)
                urls_to_process.extend(sitemap_urls)

            while urls_to_process and len(discovered_urls) < max_urls:
                # Process URLs in batches
                batch = []
                while urls_to_process and len(batch) < self.config["batch_size"]:
                    url = urls_to_process.popleft()
                    if url not in self.visited_urls:
                        batch.append(url)
                        self.visited_urls.add(url)

                if not batch:
                    break

                # Process batch concurrently
                new_urls = await self._process_url_batch(batch, start_url)
                
                # Filter and add new URLs
                for url in new_urls:
                    if url not in self.visited_urls and len(discovered_urls) < max_urls:
                        if not self.config["include_external"] and not self._is_same_domain(start_url, url):
                            continue
                        discovered_urls.add(url)
                        urls_to_process.append(url)

            # Trim to max_urls before returning
            return set(list(discovered_urls)[:max_urls])

    async def fetch_data(self, url: str, config: Dict = {}) -> Set[str]:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url, config=config)
            if result.success:
                return result
        return ""

# Example usage
# async def main():
#     config = {
#         "max_depth": 2,
#         "include_external": True,
#         "crawl_sitemap": True,
#         "handle_pagination": True,
#         "handle_lazy_load": True,
#         "concurrent_requests": 10,
#         "batch_size": 50
#     }

#     crawler = URLCrawler(config)
#     urls = await crawler.get_urls("https://example.com", max_urls=10)
#     print(f"Discovered {len(urls)} URLs")

# if __name__ == "__main__":
#     asyncio.run(main())