# core/utils.py
"""
Contains utility and helper functions for logging, data cleaning, and API interaction.
"""
import csv
import os
import re
import datetime as dt
import math
import pandas as pd
from urllib.parse import urlparse

# UPDATED: Import the logger from the central logging module.
from .logging import dbg

# NOTE: The import from external_apis is moved into the clean_name function below.

# ─── API Usage Logging ───────────────────────────────────────────────
def log_api_call(log_file_path, service_name, cost, query_info=""):
    """Logs an API call to a CSV file."""
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    log_entry = {
        "timestamp": timestamp,
        "api_service": service_name,
        "cost": cost,
        "query_info": query_info[:200]
    }
    try:
        header = not os.path.exists(log_file_path)
        with open(log_file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=log_entry.keys())
            if header:
                writer.writeheader()
            writer.writerow(log_entry)
    except IOError as e:
        dbg(f"API Log ERR: Could not write to API usage log: {e}")

def load_api_usage_df(log_file_path):
    """Loads the API usage log CSV into a DataFrame."""
    if os.path.exists(log_file_path):
        try:
            df = pd.read_csv(log_file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as e:
            dbg(f"API Log ERR: Could not read API usage log: {e}")
    return pd.DataFrame(columns=["timestamp", "api_service", "cost", "query_info"])

# ─── LLM Interaction Logging ─────────────────────────────────
def log_llm_interaction(log_file_path, task_type, model_name, prompt, raw_response, parsed_output="", success=True):
    """Logs details of an LLM interaction to a CSV file."""
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    log_entry = {
        "timestamp": timestamp,
        "task_type": task_type,
        "model_name": model_name,
        "prompt_hash": hash(prompt),
        "raw_response_snippet": str(raw_response)[:500] + "...",
        "parsed_output_snippet": str(parsed_output)[:500] + "...",
        "success_flag": success
    }
    try:
        header = not os.path.exists(log_file_path)
        with open(log_file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=log_entry.keys())
            if header:
                writer.writeheader()
            writer.writerow(log_entry)
    except IOError as e:
        dbg(f"LLM Log ERR: Could not write to LLM interactions log: {e}")

# ─── Data Cleaning & Formatting ────────────────────────────────────────
def format_url(url_str):
    """Ensures a URL string has a scheme (https://)."""
    if not url_str or pd.isna(url_str):
        return None
    parsed = urlparse(str(url_str))
    if not parsed.scheme:
        return f"https://{url_str}"
    return url_str

def haversine(p1, p2):
    """Calculates the distance between two lat/lng points in kilometers."""
    R = 6371  # Earth radius in kilometers
    if not all(isinstance(c, (int, float)) for p in [p1, p2] for c in p if c is not None):
        dbg("Haversine: Invalid input coordinates.")
        return float('inf')
    try:
        lat1, lon1, lat2, lon2 = map(math.radians, [p1[0], p1[1], p2[0], p2[1]])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        return R * 2 * math.asin(math.sqrt(a))
    except (TypeError, IndexError, ValueError) as e:
        dbg(f"Haversine ERR: Failed calculation for {p1}, {p2}: {e}")
        return float('inf')

def clean_name(raw, ollama_config):
    """Intelligently extracts a person's name from raw text using an LLM, with a regex fallback."""
    # UPDATED: Import is moved inside the function to break the circular dependency.
    from .external_apis import call_ollama_model

    if not raw: return None
    
    # Fallback function used if LLM fails or is unavailable
    def fallback_clean(text):
        name = str(text).split('–')[0].split('|')[0].strip()
        name = re.sub(r'\s*-\s*LinkedIn.*', '', name, flags=re.I)
        name = re.sub(r'\s+\(.*', '', name)
        dbg(f"Clean Name Fallback: Raw: '{text}' -> Cleaned: '{name}'")
        return name if name else None

    # Try LLM first
    prompt = f"From the following text, extract only the full human name. If no clear human name is present, respond with only the word 'None'. Text: '{raw}'"
    try:
        name_extracted = call_ollama_model(
            ollama_config['base_url'],
            ollama_config['reasoning_model'],
            prompt,
            expect_json=False
        )
    except Exception as e:
        dbg(f"clean_name ERR: LLM call failed: {e}. Using fallback.")
        return fallback_clean(raw)
    
    if name_extracted and isinstance(name_extracted, str):
        name = name_extracted.strip()
        # Basic validation of the LLM's output
        if name.lower() == 'none' or len(name.split()) < 2 or len(name) > 70 or any(c.isdigit() for c in name):
            dbg(f"Ollama clean_name: Invalid or 'None' from LLM. Raw: '{raw}', LLM Output: '{name}'. Using fallback.")
            return fallback_clean(raw)
        
        dbg(f"Ollama clean_name: Raw: '{raw}' -> Cleaned: '{name}'")
        return name
    else:
        dbg(f"Ollama clean_name WARN: LLM returned non-string or empty. Using fallback. Response: {name_extracted}")
        return fallback_clean(raw)