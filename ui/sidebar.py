# ui/sidebar.py
"""
Defines the function to render the entire Streamlit sidebar.
"""
import streamlit as st
import os
import tempfile
import re
import time # Added for the success message timer

# Core and UI component imports
from ui.components import display_api_usage_summary
from core.database import import_file_to_db, load_db_paginated, get_total_lead_count
from core.external_apis import call_ollama_model, get_ollama_models
from core.ai_prompts import get_prompt_for_sql_generation
from core.utils import dbg


def render_sidebar(config):
    """
    Renders the entire sidebar UI.
    Returns two dictionaries: one for harvester settings and one for manual enrichment settings.
    """
    st.sidebar.title("⚙️ LeadGen Pro Controls")

    # --- API Usage Monitor ---
    display_api_usage_summary(config.API_USAGE_LOG_FILE)
    st.sidebar.divider()

    # --- AI Lead Assistant (DB Query) ---
    st.sidebar.subheader("🤖 AI DB Query")
    ai_query_db = st.sidebar.text_area(
        "Ask your database:", height=100, key="ai_query_db_area",
        placeholder="e.g., 'Find restaurants in Toronto'"
    )

    if st.sidebar.button("💬 Ask Database", key="ask_ai_db_btn"):
        if ai_query_db:
            with st.spinner("AI is generating SQL..."):
                prompt = get_prompt_for_sql_generation(ai_query_db, config.TMC_MEDIA_PROFILE)
                sql_raw = call_ollama_model(config.OLLAMA_BASE_URL, config.OLLAMA_REASONING_MODEL, prompt)
                
                if sql_raw:
                    cleaned_sql = re.sub(r"```sql|```", "", sql_raw, flags=re.IGNORECASE).strip()
                    st.session_state["generated_sql_for_ai_assistant"] = cleaned_sql
                else:
                    st.sidebar.error("AI failed to generate a query.")
                st.rerun()
        else:
            st.sidebar.warning("Please enter a query.")
    
    if st.session_state.get("generated_sql_for_ai_assistant"):
        st.sidebar.code(st.session_state["generated_sql_for_ai_assistant"], language="sql")
        if st.sidebar.button("✅ Execute SQL", key="exec_ai_sql_btn"):
            with st.spinner("Executing SQL..."):
                try:
                    df_results = load_db_paginated(config.DB_FILE, query_override=st.session_state["generated_sql_for_ai_assistant"])
                    st.session_state['latest_harvest'] = df_results
                    st.sidebar.success(f"Query found {len(df_results)} leads. See 'Latest Harvest' tab.")
                    del st.session_state["generated_sql_for_ai_assistant"]
                    st.rerun()
                except Exception as e:
                    st.sidebar.error(f"SQL Error: {e}")

    st.sidebar.divider()
    
    # --- Lead Harvester Controls ---
    st.sidebar.subheader("🔍 Lead Harvester")
    lead_type = st.sidebar.selectbox("Lead source", ["Google Places", "LinkedIn (person)", "LinkedIn (company)", "Open Street Map"], key="sb_lead_type")
    location = st.sidebar.text_input("Location / Region", "Toronto", key="sb_location")
    keywords = st.sidebar.text_input("Keywords / Title / Category", "", key="sb_keywords", placeholder="e.g., 'plumbing'")
    
    pages = 1
    if "LinkedIn" in lead_type:
        pages = st.sidebar.slider("Google pages (for LinkedIn)", 1, 10, 3, key="sb_pages")
    
    run_harvester_clicked = st.sidebar.button("🚀 Harvest Leads", type="primary")

    st.sidebar.divider()

    # --- Manual Enrichment Section ---
    st.sidebar.subheader("🔎 Manual Enrichment")
    tool_choice = st.sidebar.selectbox(
        "Select Enrichment Tool",
        ["Browser Automation Report", "Google Places Search", "OCR from Image"]
    )
    manual_input = None
    if tool_choice == "OCR from Image":
        manual_input = st.sidebar.file_uploader("Upload an image for OCR", type=['png', 'jpg', 'jpeg'])
    else:
        manual_input = st.sidebar.text_input("Enter Company Name, Website, or Query", key="manual_enrich_input")
    run_manual_enrich_clicked = st.sidebar.button("🔬 Run Enrichment", use_container_width=True)

    st.sidebar.divider()

    # --- Import Leads Section ---
    # FIXED: This section is now fully implemented with feedback and error handling.
    st.sidebar.subheader("⬆️ Import Leads")
    uploaded_file = st.sidebar.file_uploader("Upload CSV or Excel file", type=['csv', 'xlsx', 'xls'])
    if uploaded_file:
        if st.sidebar.button("Import File"):
            # Use a temporary directory to handle the file robustly
            temp_dir = os.path.join(config.project_root, "temp_uploads")
            os.makedirs(temp_dir, exist_ok=True)
            temp_filepath = os.path.join(temp_dir, uploaded_file.name)
            
            with open(temp_filepath, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            with st.spinner(f"Processing and importing '{uploaded_file.name}'..."):
                try:
                    # Capture the return values from the fixed import function
                    inserted, skipped = import_file_to_db(config.DB_FILE, temp_filepath)
                    # Display a clear success message
                    st.sidebar.success(f"Import Complete! New Leads Added: {inserted}, Duplicates Skipped: {skipped}")
                    time.sleep(3) # Give user time to read the message
                    st.rerun() # Rerun the app to refresh the database view
                except Exception as e:
                    st.sidebar.error(f"Import failed: {e}")
                    dbg(f"File Import Error: {e}")
                finally:
                    # Always clean up the temporary file
                    if os.path.exists(temp_filepath):
                        os.remove(temp_filepath)

    st.sidebar.divider()
    
    # --- Settings & Configuration Expander ---
    with st.sidebar.expander("🛠️ Tools & Settings", expanded=False):
        st.subheader("Ollama Configuration")
        
        available_models = get_ollama_models(config.OLLAMA_BASE_URL)
        
        if available_models:
            try:
                current_model_index = available_models.index(config.OLLAMA_REASONING_MODEL)
            except ValueError:
                current_model_index = 0
            
            selected_model = st.selectbox(
                "Select AI Model",
                options=available_models,
                index=current_model_index,
                help="Choose from locally installed Ollama models."
            )
            config.OLLAMA_REASONING_MODEL = selected_model
        else:
            st.warning("Could not connect to Ollama server.")
            config.OLLAMA_REASONING_MODEL = st.text_input(
                "Enter Model Name Manually", 
                value=config.OLLAMA_REASONING_MODEL
            )

        st.subheader("General Settings")
        config.DEBUG = st.checkbox("Show debug messages", value=config.DEBUG)

        if st.button("Save Settings", use_container_width=True):
            if hasattr(config, 'save') and config.save():
                st.toast("Settings saved successfully!")
            else:
                st.error("Failed to save settings. Check permissions for config.toml.")

    st.sidebar.divider()
    
    # --- Database Info ---
    total_leads = get_total_lead_count(config.DB_FILE)
    st.sidebar.caption(f"DB: {os.path.basename(config.DB_FILE)} | Total Leads: {total_leads}")

    # --- Return all settings from the sidebar ---
    harvester_settings = {
        "run": run_harvester_clicked,
        "lead_type": lead_type,
        "location": location,
        "keywords": keywords,
        "pages": pages
    }
    manual_enrich_settings = {
        "run": run_manual_enrich_clicked,
        "tool": tool_choice,
        "input": manual_input
    }
    
    return harvester_settings, manual_enrich_settings