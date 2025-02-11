from transformers import pipeline

summarizer = pipeline("summarization", model="Falconsai/text_summarization")

def summarize_text(text):
    return summarizer(text, max_length=150, min_length=30, do_sample=False)[0]

if __name__=="__main__":
    text = "This is a long text that needs to be summarized. It is very important to understand that this text is very long and needs to be shortened. The text is so long that it is difficult to read. I hope this text can be summarized."
    summary = summarize_text(text)
    print(summary)