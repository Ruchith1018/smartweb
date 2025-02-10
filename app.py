import streamlit as st
import pandas as pd
from scrapy.crawler import CrawlerProcess
import scrapy
from io import BytesIO
from multiprocessing import Process
import os

class LinkExtractorSpider(scrapy.Spider):
    name = "link_extractor"
    
    excluded_keywords = ["login", "signup", "register", "account", "password"]
    
    def __init__(self, start_url, all_data, *args, **kwargs):
        super(LinkExtractorSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.allowed_domains = [start_url.split('//')[1].split('/')[0]]
        self.base_url = start_url
        self.depth1_urls = set()
        self.depth2_urls = {}
        self.all_data = all_data  # List to store all results

    def should_exclude(self, url):
        return any(keyword in url.lower() for keyword in self.excluded_keywords)

    def parse(self, response):
        # Extract depth-1 URLs
        for link in response.css("a::attr(href)").getall():
            absolute_url = response.urljoin(link)
            if absolute_url.startswith(self.start_urls[0]) and absolute_url != self.base_url:
                if not self.should_exclude(absolute_url):
                    self.depth1_urls.add(absolute_url)
        
        # Follow each depth-1 URL to extract depth-2 URLs
        for url in self.depth1_urls:
            yield scrapy.Request(url, callback=self.parse_depth2, meta={'depth1_url': url})
    
    def parse_depth2(self, response):
        depth1_url = response.meta['depth1_url']
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
            self.all_data.append([self.base_url, depth1_url, "", "1"])
            for depth2_url in depth2_list:
                self.all_data.append([self.base_url, depth1_url, depth2_url, "2"])

# Function to run the spider for each URL from CSV
def run_spider_from_csv(input_csv, output_path):
    urls = pd.read_csv(input_csv)["URL"].tolist()  # Assuming CSV has a column 'URL'
    
    all_data = []  # List to store data from all URLs
    
    process = CrawlerProcess(settings={
        "LOG_LEVEL": "ERROR",
    })
    
    # Create a job for each URL
    for url in urls:
        process.crawl(LinkExtractorSpider, start_url=url, all_data=all_data)
    
    process.start()
    
    # After all crawlers finish, save the combined data to a Pandas DataFrame
    df = pd.DataFrame(all_data, columns=["Base URL", "Depth - 1 URL", "Depth - 2 URL", "Depth Level"])
    df.to_excel(output_path, index=False)
    return df  # Return DataFrame for preview

def main():
    st.title("Scrapy Link Extractor")

    st.markdown("### Upload CSV file containing URLs")
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
    
    if uploaded_file:
        # Save uploaded file to a temporary path
        input_csv = "input.csv"
        with open(input_csv, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Show a progress bar or spinner while the spider runs
        with st.spinner('Running the scraper... Please wait'):
            output_path = "combined_extracted_urls.xlsx"
            process = Process(target=run_spider_from_csv, args=(input_csv, output_path))
            process.start()
            process.join()  # Wait for the process to finish
            
            # Read the Excel file into a DataFrame to preview it
            df = pd.read_excel(output_path)
            
            # Show a preview of the first few rows of the data
            st.subheader("Preview of Extracted Data")
            st.dataframe(df)  # Show the first few rows of the DataFrame
            
            # Provide a download link for the Excel file after it has been created
            with open(output_path, "rb") as file:
                st.success("Scraping complete!")
                st.download_button(
                    label="Download Extracted Data as Excel",
                    data=file,
                    file_name="combined_extracted_urls.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

if __name__ == "__main__":
    main()