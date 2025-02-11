# text_extractor.py

def extract_meaningful_text(response):
    """Extracts meaningful text from <p>, <div>, and header tags, ignoring unnecessary elements."""
    texts = response.xpath("//p//text() | //div[not(@class) and not(@id)]//text() | //h1//text() | //h2//text() | //h3//text()").getall()
    
    # Clean text and filter out short fragments
    cleaned_text = " ".join(text.strip() for text in texts if len(text.strip()) > 30)
    
    return cleaned_text[:2000]  # Limit to 2000 characters for readability
