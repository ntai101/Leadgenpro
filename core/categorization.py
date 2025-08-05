# core/categorization.py
"""
Contains the core logic for the AI-powered Smart Lists feature.
This module is responsible for analyzing and categorizing leads based on user-defined goals.
"""
import json
import time
import streamlit as st # <-- THIS IS THE FIX: Import the Streamlit library

# Core module imports
from .database import load_db_paginated, add_lead_to_smart_list, get_analyzed_lead_ids_for_list
from .external_apis import call_ollama_model
from .ai_prompts import get_prompt_for_smart_list_categorization
from .utils import dbg

def build_smart_list(config, list_name, list_goal, filters, max_leads_to_analyze=100):
    """
    Analyzes a pool of leads against a user-defined goal and builds a "smart list".

    Args:
        config: The main application config object.
        list_name (str): The name for the new smart list.
        list_goal (str): A natural language description of the list's purpose.
        filters (dict): A dictionary of filters to be passed to the database query.
        max_leads_to_analyze (int): The maximum number of leads to process in this run.

    Returns:
        A tuple of (success_count, failure_count).
    """
    if not all([list_name, list_goal]):
        raise ValueError("List Name and List Goal are both required.")

    dbg(f"Starting Smart List build for '{list_name}' with goal: '{list_goal}' and filters: {filters}")

    # 1. Get a pool of candidate leads using the new, more powerful filters.
    candidate_leads_df = load_db_paginated(
        config.DB_FILE, 
        page_number=1, 
        page_size=max_leads_to_analyze * 2,
        **filters
    )
    
    if candidate_leads_df.empty:
        dbg("No candidate leads found for the given filters.")
        return 0, 0

    # 2. Get IDs of leads already in this list to avoid re-analyzing them.
    already_analyzed_ids = get_analyzed_lead_ids_for_list(config.DB_FILE, list_name)
    dbg(f"Found {len(already_analyzed_ids)} leads already analyzed for this list.")

    leads_to_process_df = candidate_leads_df[~candidate_leads_df['id'].isin(already_analyzed_ids)]
    
    if len(leads_to_process_df) > max_leads_to_analyze:
        leads_to_process_df = leads_to_process_df.head(max_leads_to_analyze)

    if leads_to_process_df.empty:
        dbg("All found candidates have already been analyzed for this list.")
        return 0, 0
        
    dbg(f"Found {len(leads_to_process_df)} new leads to analyze.")

    # 3. Loop through each candidate and ask the LLM for its opinion.
    success_count = 0
    failure_count = 0
    
    progress_bar = st.progress(0, text="Starting AI Analysis...") # Use Streamlit's progress bar
    total_leads = len(leads_to_process_df)
    
    for i, (_, lead) in enumerate(leads_to_process_df.iterrows()):
        progress_bar.progress((i + 1) / total_leads, text=f"Analyzing lead {i+1} of {total_leads}: {lead['name']}")
        
        lead_data_for_prompt = lead[['name', 'title', 'source', 'address', 'business_type']].to_json()

        prompt = get_prompt_for_smart_list_categorization(
            lead_data_json=lead_data_for_prompt,
            list_goal=list_goal,
            list_name=list_name
        )

        ai_response = call_ollama_model(
            config.OLLAMA_BASE_URL,
            config.OLLAMA_REASONING_MODEL,
            prompt,
            expect_json=True
        )

        if isinstance(ai_response, dict) and 'match' in ai_response:
            if ai_response.get('match') is True:
                add_lead_to_smart_list(
                    db_file=config.DB_FILE,
                    list_name=list_name,
                    lead_id=lead['id'],
                    ai_category=ai_response.get('category'),
                    ai_justification=ai_response.get('justification')
                )
                success_count += 1
        else:
            dbg(f"AI categorization failed for lead ID {lead['id']}. Invalid response: {ai_response}")
            failure_count += 1
            
        time.sleep(0.5)
    
    progress_bar.empty()
    dbg(f"Smart List build complete. Added {success_count} new leads to '{list_name}'. Failed to analyze: {failure_count}.")
    return success_count, failure_count