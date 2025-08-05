# ui/tabs/view_enrich.py
"""
Renders the UI tab for the AI Enrichment Workbench, allowing users to
review and trigger advanced AI analysis on selected leads.
"""
import streamlit as st
import pandas as pd

# Core module imports
from core.enrichment import enrich_leads_with_ai_agent_batch, fill_missing_data_for_leads, find_and_fill_with_selenium
from core.database import unenriched, get_leads_for_enrichment
from core.utils import dbg

def render_enrich_tab(config):
    """Renders the AI Enrichment Workbench tab with multiple sections."""
    db_path = config.DB_FILE
    
    # --- SECTION 1: DEEP ANALYSIS FOR LEADS WITH WEBSITES ---
    st.subheader("🤖 AI Deep Analysis Agent")
    st.caption("This section shows leads that have a website but have not yet been analyzed by the AI for outreach strategies and deeper insights.")
    
    try:
        df_to_analyze = unenriched(db_path)
    except Exception as e:
        st.error(f"Failed to load unenriched leads from the database: {e}")
        df_to_analyze = pd.DataFrame()

    if df_to_analyze.empty:
        st.info("No new leads with websites are available for deep AI analysis at this time.")
    else:
        st.write(f"Found **{len(df_to_analyze)}** leads ready for deep analysis.")
        
        # --- THIS IS THE FIX: Added 'Select All' checkbox for this section ---
        select_all_analyze = st.checkbox("Select All for Deep Analysis", key="enrich_analyze_select_all")
        df_to_analyze['Select'] = select_all_analyze
        
        column_config_analyze = {
            "Select": st.column_config.CheckboxColumn("Select", required=True),
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "name": st.column_config.TextColumn("Name", disabled=True),
            "website": st.column_config.LinkColumn("Website", disabled=True)
        }
        edited_df_analyze = st.data_editor(
            df_to_analyze[['Select', 'name', 'website', 'id']],
            hide_index=True, column_config=column_config_analyze,
            use_container_width=True, key="analysis_selector"
        )
        selected_rows_analyze = edited_df_analyze[edited_df_analyze['Select']]
        if not selected_rows_analyze.empty:
            st.write(f"You have selected **{len(selected_rows_analyze)}** lead(s) for deep analysis.")
            if st.button(f"🚀 Run AI Agent on {len(selected_rows_analyze)} Lead(s)", type="primary"):
                lead_ids_to_process = selected_rows_analyze['id'].tolist()
                with st.spinner("AI Agent is performing deep analysis... This may take a while."):
                    try:
                        success_count, failure_count = enrich_leads_with_ai_agent_batch(db_path, config, lead_ids_to_process)
                        st.success(f"AI enrichment complete! Successfully analyzed: {success_count}, Failed: {failure_count}.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"An error occurred during the AI enrichment batch process: {e}")
        else:
            st.info("Select one or more leads from the table above to run the deep analysis agent.")

    st.divider()

    # --- Shared column configuration for the next two tables ---
    column_config_fill = {
        "Select": st.column_config.CheckboxColumn("Select", required=True),
        "id": st.column_config.NumberColumn("ID", disabled=True),
        "name": st.column_config.TextColumn("Name", disabled=True),
        "website": st.column_config.LinkColumn("Website", disabled=True),
        "phone": st.column_config.TextColumn("Phone", disabled=True),
        "email": st.column_config.TextColumn("Email", disabled=True),
        "address": st.column_config.TextColumn("Address", disabled=True)
    }

    try:
        df_for_filling = get_leads_for_enrichment(db_path, limit=200)
    except Exception as e:
        st.error(f"Failed to load leads for data recovery: {e}")
        df_for_filling = pd.DataFrame()
        
    # --- SECTION 2: FIND AND FILL MISSING DATA (PAID GOOGLE API) ---
    st.subheader("🤖 AI Data Recovery Agent (Google API)")
    st.caption("This agent uses the paid Google Search API to find missing data. It is faster but will incur costs.")

    if df_for_filling.empty:
        st.info("✅ No leads currently need data recovery.")
    else:
        st.write(f"Found **{len(df_for_filling)}** leads with missing information.")
        
        # --- THIS IS THE FIX: Added 'Select All' checkbox for this section ---
        select_all_google = st.checkbox("Select All for Google API Agent", key="enrich_google_select_all")
        df_to_fill_google = df_for_filling.copy()
        df_to_fill_google.insert(0, 'Select', select_all_google)
        
        edited_df_fill_google = st.data_editor(
            df_to_fill_google, hide_index=True, column_config=column_config_fill,
            use_container_width=True, key="data_recovery_selector_google"
        )
        selected_rows_fill_google = edited_df_fill_google[edited_df_fill_google['Select']]
        if not selected_rows_fill_google.empty:
            st.write(f"You have selected **{len(selected_rows_fill_google)}** lead(s).")
            if st.button(f"🤖 Find & Fill with Google API for {len(selected_rows_fill_google)} Lead(s)", type="primary"):
                lead_ids_to_process = selected_rows_fill_google['id'].tolist()
                with st.spinner("AI Agent is searching for missing data using Google..."):
                    updated_count = fill_missing_data_for_leads(config.DB_FILE, lead_ids_to_process, config)
                    st.success(f"Process complete! Attempted to update {updated_count} lead(s) with new information.")
                    st.rerun()

    st.divider()

    # --- SECTION 3: FIND AND FILL MISSING DATA (FREE SELENIUM) ---
    st.subheader("🤖 AI Data Recovery Agent (Selenium - Free)")
    st.caption("This agent uses a web browser (Selenium) and your local AI to find missing data. It is free but may be slower.")

    if df_for_filling.empty:
        st.info("✅ No leads currently need data recovery.")
    else:
        st.write(f"Found **{len(df_for_filling)}** leads with missing information.")
        
        # --- THIS IS THE FIX: Added 'Select All' checkbox for this section ---
        select_all_selenium = st.checkbox("Select All for Selenium Agent", key="enrich_selenium_select_all")
        df_to_fill_selenium = df_for_filling.copy()
        df_to_fill_selenium.insert(0, 'Select', select_all_selenium)
        
        edited_df_fill_selenium = st.data_editor(
            df_to_fill_selenium,
            hide_index=True,
            column_config=column_config_fill,
            use_container_width=True,
            key="data_recovery_selector_selenium"
        )
        selected_rows_fill_selenium = edited_df_fill_selenium[edited_df_fill_selenium['Select']]
        if not selected_rows_fill_selenium.empty:
            st.write(f"You have selected **{len(selected_rows_fill_selenium)}** lead(s).")
            if st.button(f"🤖 Find & Fill with Selenium for {len(selected_rows_fill_selenium)} Lead(s)", type="primary"):
                lead_ids_to_process = selected_rows_fill_selenium['id'].tolist()
                with st.spinner("AI Agent is starting a browser to find missing data... This may take a while."):
                    updated_count, failed_count = find_and_fill_with_selenium(config.DB_FILE, lead_ids_to_process, config)
                    st.success(f"Process complete! Successfully updated: {updated_count}, Failed or no new data found: {failed_count}.")
                    st.rerun()