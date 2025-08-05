# core/logging.py
"""
Centralized logging configuration for the application.
Initializes and configures a global logger instance to be used across all modules.
"""
import logging
import streamlit as st
from dotenv import load_dotenv
load_dotenv()
# Configure the root logger. This is a robust way to set up logging once.
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s (%(filename)s:%(lineno)d)")

def dbg(msg):
    """Prints a message to the console log and optionally to the Streamlit UI if debug mode is on."""
    # Log to console every time
    logging.info(msg)
    
    # Check if we are in a Streamlit context and if debug mode is enabled
    try:
        if hasattr(st, 'session_state') and st.session_state.get("debug", False):
            display_msg = str(msg)
            # Truncate long messages for better UI display
            max_len = 1000
            if len(display_msg) > max_len:
                display_msg = display_msg[:max_len] + "... (truncated)"
            st.code(display_msg)
    except Exception:
        # This can happen if called from a non-streamlit thread. The console log still works.
        pass