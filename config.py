"""
Updated config.py for Streamlit deployment (uses st.secrets instead of .env/config.toml)
"""
import os
import streamlit as st

class AppConfig:
    """A centralized class to hold all application settings for Streamlit Cloud."""
    def __init__(self):
        # --- Streamlit Secrets ---
        self.DEBUG = True  # Optional: Set False in production

        # --- API Keys ---
        self.PLACES_API_KEY = st.secrets["GOOGLE_PLACES_API_KEY"]
        self.GCP_API_KEY = st.secrets["GCP_API_KEY"]
        self.GCP_CX = st.secrets["GOOGLE_PROGRAMMABLE_SEARCH_CX"]
        self.HUNTER_KEY = st.secrets["HUNTER_IO_API_KEY"]

        # --- Ollama ---
        self.OLLAMA_BASE_URL = st.secrets.get("OLLAMA_BASE_URL", "http://localhost:11434")
        self.OLLAMA_REASONING_MODEL = "llama3"  # Hardcoded or later exposed via UI
        self.OLLAMA_EMBEDDING_MODEL = "mxbai-embed-large"

        # --- OpenStreetMap ---
        self.NOMINATIM_USER_AGENT = "LeadGenPro/1.0"

        # --- DB Paths and Logs (within Streamlit's allowed folders) ---
        self.DB_FILE = "leads.db"
        self.API_USAGE_LOG_FILE = "api_usage.csv"
        self.LLM_INTERACTIONS_LOG_FILE = "llm_interactions.csv"
        self.DOWNLOAD_DIR = "downloads"

        os.makedirs(self.DOWNLOAD_DIR, exist_ok=True)

        # --- Company Profile ---
        try:
            with open("company_profile.txt", 'r', encoding='utf-8') as f:
                self.TMC_MEDIA_PROFILE = f.read()
        except FileNotFoundError:
            self.TMC_MEDIA_PROFILE = "A digital marketing agency specializing in web development and SEO."

    def save(self):
        # Saving logic omitted for Streamlit Cloud use case
        print("Save method is disabled in deployed Streamlit Cloud apps.")
        return False
