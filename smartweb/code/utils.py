import os

def load_excluded_keywords(file_path="excluded_keywords.txt"):
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            return [line.strip().lower() for line in file.readlines() if line.strip()]
    return []
