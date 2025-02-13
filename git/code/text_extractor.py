import google.generativeai as genai
from bs4 import BeautifulSoup

class TextExtractor:
    """Class to extract meaningful text from HTML responses using BeautifulSoup and summarize using Google Gemini API."""

    def __init__(self, response, api_key):
        """Initialize with Scrapy response and Google Gemini API key."""
        self.soup = BeautifulSoup(response.text, 'html.parser')  # Extract HTML text
        self.api_key = api_key
        genai.configure(api_key=self.api_key)  # Configure Gemini API

    def extract_text(self):
        """Extracts meaningful text from <p>, <div> (without classes/IDs), and header tags."""
        texts = []

        # Extract text from <p>, <h1>, <h2>, <h3>
        for tag in self.soup.find_all(['p', 'h1', 'h2', 'h3']):
            if tag.get_text(strip=True):
                texts.append(tag.get_text(strip=True))

        # Extract text from <div> that has no class or id
        for tag in self.soup.find_all('div', class_=False, id=False):
            if tag.get_text(strip=True):
                texts.append(tag.get_text(strip=True))

        # Clean text and filter out short fragments
        cleaned_text = " ".join(text for text in texts if len(text) > 30)
        return cleaned_text[:2000]  # Limit to 2000 characters for readability

    def summarize_text(self, text):
        """Summarizes the extracted text using Google Gemini API. If quota is exceeded, returns original text."""
        if not text:
            return "No meaningful text extracted."

        try:
            model = genai.GenerativeModel("gemini-pro")  # Using Gemini-Pro model
            response = model.generate_content(f"Summarize the following text:\n{text}")
            
            if response and response.text:
                return response.text  # Return summarized text if successful

        except Exception as e:
            # If there's an error (e.g., quota exceeded), return the original text
            print(f"Gemini API Error: {e}")  # Log the error for debugging

        return text  # Fallback: return original text if summarization fails

    def extract_and_summarize(self):
        """Extracts meaningful text and summarizes it using Gemini API. If quota is exceeded, returns original text."""
        extracted_text = self.extract_text()
        return self.summarize_text(extracted_text)
