import asyncio
from typing import List, Set, Dict, Optional
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from apply import get_urls, _handle_lazy_load, _handle_pagination, URLCrawler


class URLCrawler:
    def __init__(self, config: Dict = None):
        """
        Initialize crawler with configuration

        config options:
        - max_depth: int - Maximum depth for recursive crawling (default: 1)
        - include_external: bool - Include external links (default: False)
        - crawl_sitemap: bool - Attempt to crawl sitemap.xml (default: True)
        - handle_pagination: bool - Handle pagination links (default: False)
        - handle_lazy_load: bool - Handle lazy loaded content (default: False)
        """
        self.config = {
            "max_depth": 1,
            "include_external": False,
            "crawl_sitemap": True,
            "handle_pagination": False,
            "handle_lazy_load": False,
            **(config or {}),
        }
        self.visited_urls: Set[str] = set()

    def _is_same_domain(self, base_url: str, url: str) -> bool:
        """Check if URL belongs to the same domain as base_url"""
        base_domain = urlparse(base_url).netloc
        url_domain = urlparse(url).netloc
        return base_domain == url_domain

    async def _get_sitemap_urls(self, base_url: str) -> Set[str]:
        """Attempt to fetch URLs from sitemap.xml"""
        sitemap_urls = set()
        try:
            async with AsyncWebCrawler() as crawler:
                sitemap_url = urljoin(base_url, "/sitemap.xml")
                result = await crawler.arun(sitemap_url)

                if result.success:
                    root = ET.fromstring(result.html)
                    # Handle both standard sitemap and sitemap index
                    for url in root.findall(
                        ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                    ):
                        sitemap_urls.add(url.text)
        except Exception as e:
            print(f"Error fetching sitemap: {e}")
        return sitemap_urls

    async def _handle_pagination(self, crawler: AsyncWebCrawler, url: str) -> Set[str]:
        """Handle pagination by looking for common pagination patterns"""
        pagination_urls = set()
        config = CrawlerRunConfig(
            js_code=[
                # Scroll to bottom to trigger lazy loading
                "window.scrollTo(0, document.body.scrollHeight);",
                # Look for and click "Load More" or pagination buttons
                """
                const nextButton = document.querySelector('a.next, [aria-label="Next page"], .pagination-next');
                if (nextButton) nextButton.click();
                """,
            ],
            wait_for="js:() => document.readyState === 'complete'",
        )

        result = await crawler.arun(url, config=config)
        if result.success:
            pagination_urls.update(
                link["href"] for link in result.links.get("internal", [])
            )

        return pagination_urls

    async def _handle_lazy_load(self, crawler: AsyncWebCrawler, url: str) -> Set[str]:
        """Handle lazy loaded content by scrolling and waiting"""
        config = CrawlerRunConfig(
            js_code=[
                # Scroll multiple times with delays
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
        return set(
            link["href"] for link in result.links.get("internal", []) if link["href"]
        )


async def get_urls(self, start_url: str, depth: int = 0) -> Set[str]:
    """
    Main method to get URLs starting from a given URL
    Returns a set of discovered URLs
    """
    if depth > self.config["max_depth"] or start_url in self.visited_urls:
        return set()

    self.visited_urls.add(start_url)
    discovered_urls = {start_url}

    async with AsyncWebCrawler() as crawler:
        # 1. First try sitemap if enabled
        if self.config["crawl_sitemap"] and depth == 0:
            sitemap_urls = await self._get_sitemap_urls(start_url)
            discovered_urls.update(sitemap_urls)

        # 2. Crawl the page
        result = await crawler.arun(start_url)
        if not result.success:
            return discovered_urls

        # Add internal links
        internal_links = {link["href"] for link in result.links.get("internal", [])}
        discovered_urls.update(internal_links)

        # Add external links if configured
        if self.config["include_external"]:
            external_links = {link["href"] for link in result.links.get("external", [])}
            discovered_urls.update(external_links)

        # 3. Handle pagination if enabled
        if self.config["handle_pagination"]:
            pagination_urls = await self._handle_pagination(crawler, start_url)
            discovered_urls.update(pagination_urls)

        # 4. Handle lazy loading if enabled
        if self.config["handle_lazy_load"]:
            lazy_urls = await self._handle_lazy_load(crawler, start_url)
            discovered_urls.update(lazy_urls)

        # 5. Recursive crawling for internal links
        if depth < self.config["max_depth"]:
            for url in internal_links:
                if url not in self.visited_urls and self._is_same_domain(
                    start_url, url
                ):
                    sub_urls = await self.get_urls(url, depth + 1)
                    discovered_urls.update(sub_urls)

    return discovered_urls


# Example usage
async def main():
    config = {
        "max_depth": 2,
        "include_external": True,
        "crawl_sitemap": True,
        "handle_pagination": True,
        "handle_lazy_load": True,
    }

    crawler = URLCrawler(config)
    urls = await crawler.get_urls("https://example.com")

    print("Discovered URLs:")
    for url in sorted(urls):
        print(f"- {url}")


if name == "__main__":
    asyncio.run(main())
