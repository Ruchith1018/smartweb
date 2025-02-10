import streamlit as st
import pandas as pd
from multiprocessing import Process
from crawler import run_spider_from_csv, run_spider_from_url
from utils import load_excluded_keywords

def main():
    st.title("Scrapy Link Extractor")

    # Step 1: User chooses an option (Single URL or Bulk CSV Upload)
    st.subheader("Choose Input Type:")
    col1, col2 = st.columns(2)

    # Button-based selection
    if "option" not in st.session_state:
        st.session_state.option = None

    if col1.button("Single URL"):
        st.session_state.option = "Single URL"
    if col2.button("Bulk CSV Upload"):
        st.session_state.option = "Bulk CSV Upload"

    # Step 2: Show input field based on selection
    single_url = None
    uploaded_file = None

    if st.session_state.option == "Single URL":
        single_url = st.text_input("Enter a single URL:")
    
    elif st.session_state.option == "Bulk CSV Upload":
        uploaded_file = st.file_uploader("Upload a CSV file containing URLs", type=["csv"])

    # Step 3: Start scraping when input is provided
    if (st.session_state.option == "Single URL" and single_url) or (st.session_state.option == "Bulk CSV Upload" and uploaded_file):
        if st.button("Start Scraping"):
            excluded_keywords = load_excluded_keywords()
            output_path = "data/output.xlsx"

            with st.spinner('Running the scraper... Please wait'):
                if st.session_state.option == "Bulk CSV Upload":
                    # Save the uploaded CSV
                    input_csv = "data/input.csv"
                    with open(input_csv, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    process = Process(target=run_spider_from_csv, args=(input_csv, output_path, excluded_keywords))
                else:
                    process = Process(target=run_spider_from_url, args=(single_url, output_path, excluded_keywords))

                process.start()
                process.join()

                df = pd.read_excel(output_path)
                st.subheader("Preview of Extracted Data")
                st.dataframe(df)

                with open(output_path, "rb") as file:
                    st.success("Scraping complete!")
                    st.download_button(
                        label="Download Extracted Data as Excel",
                        data=file,
                        file_name="extracted_urls.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

if __name__ == "__main__":
    main()
