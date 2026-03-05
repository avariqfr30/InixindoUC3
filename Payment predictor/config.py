import os

# Base directory resolving
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Data paths
DB_PATH = os.path.join(BASE_DIR, 'data', 'db.csv')

# LLM & API settings
LLM_MODEL = 'gpt-oss:120b-cloud'
OLLAMA_HOST = 'http://127.0.0.1:11434'