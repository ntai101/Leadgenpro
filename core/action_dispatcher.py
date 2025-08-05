# core/action_dispatcher.py
"""
Acts as a central router or dispatcher for running various enrichment agents.
This allows any part of the UI to trigger a backend action in a consistent way.
"""
from .enrichment import (
    enrich_leads_with_ai_agent_batch,
    find_and_fill_with_selenium,
    fill_missing_data_for_leads
)
from .utils import dbg

def run_enrichment_action(action_name: str, lead_ids: list, config):
    """
    Dispatches the appropriate enrichment function based on the action name.

    Args:
        action_name (str): The name of the action to perform.
        lead_ids (list): A list of lead IDs to process.
        config: The main application config object.

    Returns:
        A tuple of (success_message, error_message).
    """
    if not lead_ids:
        return None, "No leads were selected."

    dbg(f"Dispatching action '{action_name}' for {len(lead_ids)} leads.")

    try:
        if action_name == "Deep Analysis Report":
            success, failed = enrich_leads_with_ai_agent_batch(config.DB_FILE, config, lead_ids)
            return f"Deep Analysis complete. Success: {success}, Failed: {failed}.", None

        elif action_name == "Find & Fill (Selenium - Free)":
            updated, failed = find_and_fill_with_selenium(config.DB_FILE, lead_ids, config)
            return f"Selenium Agent finished. Updated: {updated}, Failed/No Data: {failed}.", None
        
        elif action_name == "Find & Fill (Google API - Paid)":
            updated = fill_missing_data_for_leads(config.DB_FILE, lead_ids, config)
            return f"Google API Agent finished. Attempted to update {updated} lead(s).", None

        else:
            return None, f"Unknown action: '{action_name}'"

    except Exception as e:
        dbg(f"Action Dispatcher CRITICAL ERROR for action '{action_name}': {e}")
        return None, f"A critical error occurred: {e}"