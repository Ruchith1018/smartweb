import pandas as pd
from scrapy.crawler import CrawlerProcess
import scrapy
from text_extractor import TextExtractor  # Import the class

class LinkExtractorSpider(scrapy.Spider):
    name = "link_extractor"
    
    def __init__(self, start_url, all_data, excluded_keywords, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.allowed_domains = [start_url.split('//')[1].split('/')[0]]
        self.base_url = start_url
        self.depth1_urls = set()
        self.depth2_urls = {}
        self.all_data = all_data
        self.excluded_keywords = excluded_keywords
        self.page_texts = {}

    def should_exclude(self, url):
        return any(keyword in url.lower() for keyword in self.excluded_keywords)

    def parse(self, response):
        extractor = TextExtractor(response,"AIzaSyCCRbSDrJQM9uVT_7KlNgHlUcPKOcGka98")  # Instantiate the TextExtractor
        self.page_texts[response.url] = extractor.extract_and_summarize()
        
        for link in response.css("a::attr(href)").getall():
            absolute_url = response.urljoin(link)
            if absolute_url.startswith(self.start_urls[0]) and absolute_url != self.base_url:
                if not self.should_exclude(absolute_url):
                    self.depth1_urls.add(absolute_url)
        
        for url in self.depth1_urls:
            yield scrapy.Request(url, callback=self.parse_depth2, meta={'depth1_url': url})
    
    def parse_depth2(self, response):
        depth1_url = response.meta['depth1_url']
        extractor = TextExtractor(response,"AIzaSyCCRbSDrJQM9uVT_7KlNgHlUcPKOcGka98")  # Instantiate the TextExtractor
        self.page_texts[response.url] = extractor.extract_and_summarize()
        
        if depth1_url not in self.depth2_urls:
            self.depth2_urls[depth1_url] = set()
        
        for link in response.css("a::attr(href)").getall():
            absolute_url = response.urljoin(link)
            if absolute_url.startswith(depth1_url) and absolute_url != depth1_url:
                if not self.should_exclude(absolute_url):
                    self.depth2_urls[depth1_url].add(absolute_url)

    def closed(self, reason):
        for depth1_url in self.depth1_urls:
            depth2_list = list(self.depth2_urls.get(depth1_url, []))
            text_to_store = self.page_texts.get(depth1_url, "")

            self.all_data.append([self.base_url, depth1_url, "", "1", text_to_store])
            for depth2_url in depth2_list:
                text_to_store = self.page_texts.get(depth2_url, "")
                self.all_data.append([self.base_url, depth1_url, depth2_url, "2", text_to_store])


def run_spider_from_csv(input_csv, output_path, excluded_keywords):
    urls = pd.read_csv(input_csv)["URL"].tolist()
    all_data = []

    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
    
    for url in urls:
        process.crawl(LinkExtractorSpider, start_url=url, all_data=all_data, excluded_keywords=excluded_keywords)
    
    process.start()
    
    df = pd.DataFrame(all_data, columns=["Base URL", "Depth - 1 URL", "Depth - 2 URL", "Depth Level", "Extracted Text"])
    df.to_excel(output_path, index=False)
    return df

def run_spider_from_url(single_url, output_path, excluded_keywords):
    all_data = []
    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
    
    process.crawl(LinkExtractorSpider, start_url=single_url, all_data=all_data, excluded_keywords=excluded_keywords)
    process.start()
    
    df = pd.DataFrame(all_data, columns=["Base URL", "Depth - 1 URL", "Depth - 2 URL", "Depth Level", "Extracted Text"])
    df.to_excel(output_path, index=False)
    return df
