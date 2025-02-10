import scrapy
from scrapy.crawler import CrawlerProcess
from threading import Thread

class TextExtractorSpider(scrapy.Spider):
    name = "text_extractor"

    def __init__(self, url, extracted_text, char_limit=5000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [url]
        self.extracted_text = extracted_text
        self.char_limit = char_limit

    def parse(self, response):
        text = ' '.join(response.css("p::text").getall()).strip()
        cleaned_text = ' '.join(text.split())
        self.extracted_text["text"] = cleaned_text[:self.char_limit]

def extract_text_with_scrapy(url, char_limit=5000):
    """Extracts up to 5000 characters of text from a given URL using Scrapy in a separate thread."""
    extracted_text = {"text": ""}
    
    def run_spider():
        process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
        process.crawl(TextExtractorSpider, url=url, extracted_text=extracted_text, char_limit=char_limit)
        process.start()
    
    thread = Thread(target=run_spider)
    thread.start()
    thread.join()  # Wait for the thread to complete

    return extracted_text["text"]  # Return the extracted text
