# config.py
"""
Handles loading and saving all application configuration from .env and config.toml.
"""
import os
import toml
from dotenv import load_dotenv

def _get_project_root():
    """Finds the project root by looking for the .env file."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != os.path.dirname(current_dir):
        if ".env" in os.listdir(current_dir):
            return current_dir
        current_dir = os.path.dirname(current_dir)
    raise FileNotFoundError("Could not find project root containing the .env file.")

class AppConfig:
    """A centralized class to hold all application settings."""
    def __init__(self):
        self.project_root = _get_project_root()
        load_dotenv(os.path.join(self.project_root, ".env"))

        self.config_path = os.path.join(self.project_root, "config.toml")
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"config.toml not found at {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            cfg = toml.load(f)

        # --- Set All Config Attributes ---
        self.DEBUG = cfg.get("streamlit", {}).get("debug", False)
        
        # Database
        db_path = cfg.get("database", {}).get("path", "user_data/leads.db")
        self.DB_FILE = os.path.join(self.project_root, db_path)
        
        # ADDED: Directory for downloads/exports
        self.DOWNLOAD_DIR = os.path.join(self.project_root, "user_data", "downloads")
        os.makedirs(self.DOWNLOAD_DIR, exist_ok=True) # Ensure directory exists
        
        # API Keys & Endpoints (from .env file)
        self.PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
        self.GCP_API_KEY = os.getenv("GOOGLE_CLOUD_PLATFORM_API_KEY", "")
        self.GCP_CX = os.getenv("GOOGLE_PROGRAMMABLE_SEARCH_CX", "")
        self.HUNTER_KEY = os.getenv("HUNTER_IO_API_KEY", "")
        
        # Ollama
        self.OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.OLLAMA_REASONING_MODEL = cfg.get("ollama", {}).get("reasoning_model", "llama3")
        self.OLLAMA_EMBEDDING_MODEL = cfg.get("ollama", {}).get("embedding_model", "mxbai-embed-large")

        # Nominatim (for OpenStreetMap)
        self.NOMINATIM_USER_AGENT = cfg.get("nominatim", {}).get("user_agent", "LeadGenPro/1.0")

        # Log Files
        log_dir = os.path.join(self.project_root, "user_data")
        os.makedirs(log_dir, exist_ok=True)
        self.API_USAGE_LOG_FILE = os.path.join(log_dir, "api_usage.csv")
        self.LLM_INTERACTIONS_LOG_FILE = os.path.join(log_dir, "llm_interactions.csv")

        # Company Profile (for prompts)
        self.PROFILE_PATH_RELATIVE = cfg.get("company", {}).get("profile_path", "company_profile.txt")
        profile_path = os.path.join(self.project_root, self.PROFILE_PATH_RELATIVE)
        try:
            with open(profile_path, 'r', encoding='utf-8') as f:
                self.TMC_MEDIA_PROFILE = f.read()
        except FileNotFoundError:
            self.TMC_MEDIA_PROFILE = "A digital marketing agency specializing in web development and SEO."

    def save(self):
        """Saves the current configuration back to the config.toml file."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                cfg_data = toml.load(f)

            # Update the values from the current config object
            cfg_data["streamlit"]["debug"] = self.DEBUG
            # Save the relative path for portability
            db_relative_path = os.path.relpath(self.DB_FILE, self.project_root)
            cfg_data["database"]["path"] = db_relative_path.replace("\\", "/") # Ensure forward slashes
            cfg_data["ollama"]["reasoning_model"] = self.OLLAMA_REASONING_MODEL
            cfg_data["company"]["profile_path"] = self.PROFILE_PATH_RELATIVE

            # Write the updated data back to the file
            with open(self.config_path, 'w', encoding='utf-8') as f:
                toml.dump(cfg_data, f)
            
            print("Configuration saved successfully to config.toml")
            return True
        except Exception as e:
            print(f"Failed to save configuration: {e}")
            return False