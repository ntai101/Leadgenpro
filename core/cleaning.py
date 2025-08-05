# core/cleaning.py
"""
Contains the core logic for the Database Cleaning tool.
This module finds potentially incorrect or "junk" leads and runs maintenance tasks.
"""
import pandas as pd
import re
import time
import streamlit as st
from urllib.parse import urlparse

# Core module imports
from .database import load_db_paginated, delete_leads_from_db, update_lead_in_db, remove_db_duplicates
from .external_apis import call_ollama_model
from .ai_prompts import get_prompt_for_entry_validation
from .utils import dbg

def find_bad_entries_with_rules(db_file: str, limit: int = 1000) -> pd.DataFrame:
    """
    Finds potentially incorrect lead entries based on a set of predefined rules.
    """
    dbg(f"Starting rule-based scan for incorrect entries on the latest {limit} leads.")
    df = load_db_paginated(db_file, page_number=1, page_size=limit)
    if df.empty:
        return pd.DataFrame()

    bad_entries = []
    for _, row in df.iterrows():
        name = str(row.get('name', '')).strip()
        reason = None

        if not name:
            reason = "Name is empty."
        elif len(name) < 3:
            reason = "Name is too short (< 3 chars)."
        elif re.search(r'\d{5,}', name):
            reason = "Name contains a long number, likely a phone number or ID."
        elif name.lower() in ['n/a', 'unknown', 'not available', 'name', 'test']:
            reason = "Name is a generic placeholder."
        elif 'http' in name.lower() or '.com' in name.lower() or '.org' in name.lower():
            reason = "Name appears to be a URL."
        
        if reason:
            entry = row.to_dict()
            entry['reason'] = reason
            bad_entries.append(entry)
            
    dbg(f"Rule-based scan found {len(bad_entries)} potential bad entries.")
    return pd.DataFrame(bad_entries)

def find_bad_entries_with_ai(config, limit: int = 100) -> pd.DataFrame:
    """
    Uses an AI model to analyze and identify potentially incorrect lead entries.
    """
    dbg(f"Starting AI scan for {limit} incorrect entries.")
    df = load_db_paginated(config.DB_FILE, page_number=1, page_size=limit)
    if df.empty:
        return pd.DataFrame()

    bad_entries = []
    progress_bar = st.progress(0, text="Starting AI Analysis...") if 'st' in locals() else None

    total_leads = len(df)
    for i, (_, row) in enumerate(df.iterrows()):
        if progress_bar:
            progress_bar.progress((i + 1) / total_leads, text=f"Analyzing lead {i+1}/{total_leads}: {row['name']}")
        
        prompt = get_prompt_for_entry_validation(row.to_dict())
        
        ai_response = call_ollama_model(
            config.OLLAMA_BASE_URL,
            config.OLLAMA_REASONING_MODEL,
            prompt,
            expect_json=True
        )

        if isinstance(ai_response, dict) and ai_response.get("is_valid") is False:
            entry = row.to_dict()
            entry['reason'] = ai_response.get('reason', 'AI flagged as invalid.')
            bad_entries.append(entry)
        
        time.sleep(0.5)

    if progress_bar:
        progress_bar.empty()
        
    dbg(f"AI scan found {len(bad_entries)} potential bad entries.")
    return pd.DataFrame(bad_entries)

# --- NEW FUNCTION FOR DATABASE MAINTENANCE ---
def run_db_maintenance(db_file: str, actions_to_run: dict) -> str:
    """
    Runs selected database maintenance tasks.
    
    Args:
        db_file (str): The path to the SQLite database.
        actions_to_run (dict): A dictionary with boolean flags for each task.
        
    Returns:
        str: A summary report of the actions performed.
    """
    report_lines = []
    
    # --- Action 1: Clean and Standardize Website URLs ---
    if actions_to_run.get('clean_websites'):
        dbg("Running maintenance: Clean Website URLs")
        updated_count = 0
        # Load all leads that have a website URL
        df_websites = load_db_paginated(db_file, page_number=1, page_size=10000, has_website=True)
        for _, row in df_websites.iterrows():
            original_url = row['website']
            if not original_url or not isinstance(original_url, str):
                continue
            
            # Standardize: add scheme, remove www, remove trailing slash
            parsed = urlparse(original_url, scheme='https')
            netloc = parsed.netloc.replace('www.', '')
            path = parsed.path.rstrip('/')
            
            cleaned_url = f"{parsed.scheme}://{netloc}{path}"
            
            if cleaned_url != original_url:
                if update_lead_in_db(db_file, row['id'], 'website', cleaned_url):
                    updated_count += 1
        report_lines.append(f"Cleaned and standardized {updated_count} website URLs.")

    # --- Action 2: Remove Duplicate Leads ---
    if actions_to_run.get('remove_duplicates'):
        dbg("Running maintenance: Remove Duplicates")
        # This function is already available in the database module
        removed_count = remove_db_duplicates(db_file)
        report_lines.append(f"Removed {removed_count} duplicate leads.")

    if not report_lines:
        return "No maintenance actions were selected."
        
    return " ".join(report_lines)