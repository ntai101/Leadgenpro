# main_app.py
"""
The main entry point for the LeadGen Pro Streamlit application.
"""
import sys
import os
import tempfile
import time # Added time for the sidebar import success message

# Add project root to the Python path to ensure modules are found
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(SCRIPT_DIR)
except NameError:
    # Fallback for environments where __file__ is not defined
    sys.path.append(os.getcwd())

import streamlit as st
import pandas as pd

# --- First Party Imports (Core Logic and UI) ---
from config import AppConfig
from core.database import init_db, remove_db_duplicates, upsert_leads
from core.harvesters import harvest_linkedin, harvest_places, harvest_openstreetmap
from core.enrichment import run_basic_enrichment, run_manual_enrichment
from core.utils import dbg
from ui.sidebar import render_sidebar
from ui.tabs.view_harvest import render_harvest_tab
from ui.tabs.view_enrich import render_enrich_tab
from ui.tabs.view_database import render_database_tab
from ui.tabs.view_map import render_map_search_tab, render_full_map_tab
from ui.tabs.view_bulk import render_bulk_places_tab, render_bulk_osm_tab
from ui.tabs.view_smart_lists import render_smart_lists_tab # Assuming you've created this file
from ui.components import display_api_usage_summary
from ui.tabs.view_cleaning import render_cleaning_tab # <-- ADDED IMPORT

# --- 1. Page Configuration & Initial Setup ------------------------------------
st.set_page_config(
    page_title="LeadGen Pro",
    page_icon="🚀",
    layout="wide"
)

# Initialize the AppConfig class to load all settings.
try:
    config = AppConfig()
except Exception as e:
    st.error(f"Fatal Error: Could not load configuration. Please check your config.toml and .env files. Details: {e}")
    st.stop()

# Initialize the database on startup
try:
    init_db(config.DB_FILE)
    # Optional: Run duplicate check on startup. Can be commented out for faster loads.
    # remove_db_duplicates(config.DB_FILE)
except Exception as e:
    st.error(f"Fatal Error: Could not initialize database at '{config.DB_FILE}'")
    st.exception(e)
    st.stop()

# --- 2. Session State Initialization ------------------------------------------

# --- THIS IS THE CRITICAL FIX ---
# Store the config object in the session state so it's globally accessible to all components.
if 'config' not in st.session_state:
    st.session_state.config = config
    # Ensure DOWNLOAD_DIR from config is accessible in session_state as well
    st.session_state.config.DOWNLOAD_DIR = config.DOWNLOAD_DIR 


session_state_defaults = {
    "latest_harvest": pd.DataFrame(),
    "debug": config.DEBUG,
    "map_radius": 5.0,
    "map_center_coords": (43.6532, -79.3832), # Default to Toronto
    "db_limit": 200,
    "db_search_name": "",
    "generated_sql_for_ai_assistant": "",
    "auto_enrich_basic": True,
    "manual_enrich_report": "",
    "bulk_places_df": pd.DataFrame(),
    "bulk_osm_df": pd.DataFrame(),
    "total_db_count": 0
}
for key, default_value in session_state_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = default_value

# --- 3. Render Sidebar and Handle Actions -----------------------------------
harvester_settings, manual_enrich_settings = render_sidebar(config)

# --- Handle Harvester Action ---
if harvester_settings["run"]:
    st.session_state.latest_harvest = pd.DataFrame()
    hits = []
    
    lead_type = harvester_settings["lead_type"]
    keywords = harvester_settings["keywords"]
    location = harvester_settings["location"]
    pages = harvester_settings["pages"]
    
    with st.spinner(f"Harvesting from {lead_type}..."):
        try:
            if lead_type == "Google Places":
                hits = harvest_places(
                    places_api_key=config.PLACES_API_KEY, 
                    keyword=keywords, 
                    location=location, 
                    db_path=config.DB_FILE,
                    api_log_file=config.API_USAGE_LOG_FILE
                )
            elif "LinkedIn" in lead_type:
                gcp_config = {"api_key": config.GCP_API_KEY, "cx_id": config.GCP_CX, "api_log_file": config.API_USAGE_LOG_FILE}
                ollama_config = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
                mode = "linkedin_person" if "person" in lead_type else "linkedin_company"
                hits = harvest_linkedin(gcp_config, config.HUNTER_KEY, ollama_config, keywords, pages, mode)
            elif lead_type == "Open Street Map":
                osm_config = {"user_agent": config.NOMINATIM_USER_AGENT}
                hits = harvest_openstreetmap(
                    osm_config=osm_config, 
                    keywords=keywords.split(','),
                    location=location,
                    db_path=config.DB_FILE
                )
                
            if hits:
                inserted, skipped = upsert_leads(config.DB_FILE, hits)
                st.sidebar.success(f"Harvest complete. Inserted: {inserted}, Skipped: {skipped}")
                st.session_state.latest_harvest = pd.DataFrame(hits)

                if st.session_state.auto_enrich_basic and inserted > 0:
                    st.sidebar.info("Performing basic auto-enrichment...")
                    run_basic_enrichment(config.DB_FILE, config)
            else:
                st.sidebar.warning("Harvesting returned no new results. (Existing leads were skipped).")
        except Exception as e:
            st.sidebar.error(f"Harvesting failed: {e}")
            dbg(f"HARVESTING ERROR: {e}")
            
    st.rerun()

# --- Handle Manual Enrichment Action ---
if manual_enrich_settings["run"]:
    tool = manual_enrich_settings["tool"]
    user_input = manual_enrich_settings["input"]
    
    with st.spinner(f"Running {tool}..."):
        report = ""
        if tool == "OCR from Image" and user_input is not None:
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(user_input.name)[1]) as tmp:
                tmp.write(user_input.getvalue())
                temp_path = tmp.name
            report = run_manual_enrichment(tool, temp_path, config)
            os.remove(temp_path)
        
        elif tool != "OCR from Image" and user_input:
            report = run_manual_enrichment(tool, user_input, config)
        
        else:
            st.warning("Please provide input for the selected tool.")
        
        st.session_state.manual_enrich_report = report

# --- 4. Render Main Content Area with Tabs ------------------------------------
st.title("LeadGen Pro Dashboard")

if st.session_state.manual_enrich_report:
    with st.expander("🔬 Manual Enrichment Report", expanded=True):
        st.markdown(st.session_state.manual_enrich_report)
        if st.button("Clear Report"):
            st.session_state.manual_enrich_report = ""
            st.rerun()

# --- MODIFIED: Added "DB Cleaning" to the tab list ---
tab_titles = ["🆕 Latest Harvest", "✨ Enrich Leads", "📊 Database View", "🧠 Smart Lists", "🧹 DB Cleaning", "🗺️ Map Search", "📍 Full Map", "📦 Bulk Places", "🌍 Bulk OSM"]
tabs = st.tabs(tab_titles)

with tabs[0]:
    render_harvest_tab()
with tabs[1]:
    render_enrich_tab(config)
with tabs[2]:
    render_database_tab(config)
with tabs[3]:
    render_smart_lists_tab(config)
with tabs[4]:
    render_cleaning_tab(config) # <-- RENDER THE NEW TAB
with tabs[5]:
    render_map_search_tab(config)
with tabs[6]:
    render_full_map_tab(config)
with tabs[7]:
    render_bulk_places_tab(config)
with tabs[8]:
    render_bulk_osm_tab(config)





