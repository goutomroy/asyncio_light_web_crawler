import asyncio
from collections import namedtuple
import aiohttp
from urllib.parse import urljoin, urlparse
from lxml import html as lh

ResContent = namedtuple('ResContent', ['url', 'content', 'found_urls'])


class LightCrawler:

    def __init__(self, start_url, depth=3, max_concurrency=200):
        self.start_url = start_url
        self.base_url = '{}://{}'.format(urlparse(self.start_url).scheme, urlparse(self.start_url).netloc)
        self.depth = depth
        self.checked_urls = []
        self.results = []
        self.bounded_sempahore = asyncio.BoundedSemaphore(max_concurrency)

    async def pull_page(self, session, url):
        async with self.bounded_sempahore:
            async with session.get(url, timeout=30) as response:
                return await response.read()

    async def find_urls(self, html):
        found_urls = []
        dom = lh.fromstring(html)
        for href in dom.xpath('//a/@href'):
            url = urljoin(self.base_url, href)
            if url.startswith(self.base_url) and url not in self.checked_urls:
                found_urls.append(url)

        return found_urls

    async def single_fetch(self, session, url):
        try:
            content = await self.pull_page(session, url)
            found_urls = await self.find_urls(content)
        except Exception as e:
            return e

        return ResContent(url=url, content=content, found_urls=found_urls)

    async def multi_fetch(self, fetch_urls):
        async with aiohttp.ClientSession() as session:
            coros = [self.single_fetch(session, each) for each in fetch_urls]
            self.checked_urls.extend(fetch_urls)
            fan_in_results = await asyncio.gather(*coros)
            res_contents = []
            for each in fan_in_results:
                if not isinstance(each, Exception):
                    res_contents.append(each)

        return res_contents

    async def start_crawl(self):
        to_fetch = [self.start_url]
        for i in range(self.depth):
            res_contents = await self.multi_fetch(to_fetch)
            self.results.extend(res_contents)
            to_fetch = []
            for each_res_content in res_contents:
                to_fetch.extend(each_res_content.found_urls)

        print(len(self.results))
        print(len(self.checked_urls))


if __name__ == '__main__':
    target_url = 'https://pymotw.com/3/'
    # target_url = 'https://www.prothomalo.com/'
    asyncio.run(LightCrawler(target_url, 3).start_crawl())