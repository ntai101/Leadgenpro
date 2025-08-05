# ui/tabs/view_cleaning.py
"""
Renders the UI for the Database Cleaning tool, allowing users to select
leads from a full database view and scan them for junk entries.
"""
import streamlit as st
import pandas as pd
import time
import math

# Core module imports
from core.cleaning import find_bad_entries_with_rules, find_bad_entries_with_ai, run_db_maintenance
from core.database import delete_leads_from_db, load_db_paginated, get_filtered_lead_count

def render_cleaning_tab(config):
    """Renders the UI for the Database Cleaning tool."""
    st.header("🧹 Database Cleaning Tool")

    # Initialize session state for this tab
    if 'junk_scan_results' not in st.session_state:
        st.session_state.junk_scan_results = pd.DataFrame()

    junk_finder_tab, maintenance_tab = st.tabs(["Junk Entry Finder", "Database Maintenance"])

    with junk_finder_tab:
        st.subheader("Select Leads from the Database to Scan")
        st.caption("Use the filters to find leads, check the 'Select for Scan' box for any you want to analyze, and then click the 'Scan Selected Leads' button that appears below.")

        # --- 1. FILTERS (Now includes 'Search by Website') ---
        with st.expander("Filter and View Options", expanded=True):
            if 'clean_filters' not in st.session_state:
                # Add 'website' to the filter state
                st.session_state.clean_filters = {'name': '', 'address': '', 'biz_type': '', 'website': ''}

            col1, col2 = st.columns(2)
            with col1:
                st.session_state.clean_filters['name'] = st.text_input("Search by Name", st.session_state.clean_filters['name'], key="clean_name")
                st.session_state.clean_filters['address'] = st.text_input("Search by Address", st.session_state.clean_filters['address'], key="clean_addr")
            with col2:
                # Add the new website text input
                st.session_state.clean_filters['website'] = st.text_input("Search by Website", st.session_state.clean_filters['website'], key="clean_website")
                st.session_state.clean_filters['biz_type'] = st.text_input("Search by Business Type", st.session_state.clean_filters['biz_type'], key="clean_biz")


        # Add the new filter to the dictionary passed to the database
        current_filters = {
            "search_name": st.session_state.clean_filters['name'],
            "search_address": st.session_state.clean_filters['address'],
            "search_business_type": st.session_state.clean_filters['biz_type'],
            "search_website": st.session_state.clean_filters['website']
        }

        # --- 2. PAGINATION ---
        total_leads = get_filtered_lead_count(config.DB_FILE, **current_filters)
        page_size = 5000
        total_pages = max(1, math.ceil(total_leads / page_size)) if page_size > 0 else 1
        
        pg_col1, pg_col2 = st.columns([1, 4])
        with pg_col1:
            st.number_input(f"Page (1-{total_pages})", min_value=1, max_value=total_pages, key="clean_current_page")
        with pg_col2:
            st.info(f"Showing page **{st.session_state.clean_current_page}** of **{total_pages}**. (Total matching leads: **{total_leads}**)")
        
        df_leads = load_db_paginated(config.DB_FILE, page_number=st.session_state.clean_current_page, page_size=page_size, **current_filters)
        
        # --- 3. DATA TABLE FOR SELECTION ---
        select_all = st.checkbox("Select All on Current Page for Scanning", key="clean_select_all")
        df_leads.insert(0, "Select for Scan", select_all)
        
        edited_df = st.data_editor(
            df_leads,
            column_config={"Select for Scan": st.column_config.CheckboxColumn(required=True)},
            disabled=df_leads.columns.drop("Select for Scan"),
            column_order=["Select for Scan", "id", "name", "website", "address", "business_type"],
            hide_index=True, use_container_width=True, key="clean_data_editor"
        )
        
        selected_to_scan = edited_df[edited_df['Select for Scan']]

        # --- ACTIONS & RESULTS ---
        st.divider()
        if not selected_to_scan.empty:
            st.subheader(f"Scan {len(selected_to_scan)} Selected Lead(s)")
            scan_type = st.radio("Scan Method", ["Rule-Based (Fast)", "AI-Powered (Slow)"], horizontal=True, key="clean_scan_type")
            
            if st.button("🤖 Scan Selected Leads", type="primary"):
                st.session_state.junk_scan_results = pd.DataFrame()
                if scan_type == "Rule-Based (Fast)":
                    with st.spinner("Scanning..."):
                        st.session_state.junk_scan_results = find_bad_entries_with_rules(selected_to_scan)
                else:
                    with st.spinner("Scanning with AI..."):
                        st.session_state.junk_scan_results = find_bad_entries_with_ai(config, selected_to_scan)
        else:
            st.info("Select leads from the table above to begin a scan.")

        if not st.session_state.junk_scan_results.empty:
            st.subheader("Scan Results: Potential Junk Entries")
            df_results = st.session_state.junk_scan_results
            select_all_junk = st.checkbox("Select All Results for Deletion", key="junk_select_all", value=True)
            df_results.insert(0, 'Select to Delete', select_all_junk)
            results_editor = st.data_editor(df_results[['Select to Delete', 'id', 'name', 'reason']], column_config={"Select to Delete": st.column_config.CheckboxColumn(required=True)}, disabled=['id', 'name', 'reason'], hide_index=True, use_container_width=True, key="junk_results_editor")
            selected_to_delete = results_editor[results_editor['Select to Delete']]
            if not selected_to_delete.empty:
                if st.button(f"🗑️ Delete {len(selected_to_delete)} Junk Entries", type="primary"):
                    ids_to_delete = selected_to_delete['id'].tolist()
                    with st.spinner("Deleting entries..."):
                        deleted_count = delete_leads_from_db(config.DB_FILE, ids_to_delete)
                    st.success(f"Successfully deleted {deleted_count} entries.")
                    st.session_state.junk_scan_results = pd.DataFrame()
                    time.sleep(1)
                    st.rerun()

    # --- Database Maintenance Tab ---
    with maintenance_tab:
        st.subheader("Database Maintenance Utilities")
        st.info("Run common data cleaning and standardization tasks on your entire database.")
        st.write("##### Select Maintenance Actions:")
        action_clean_websites = st.checkbox("Clean and Standardize Website URLs", value=True)
        action_remove_duplicates = st.checkbox("Remove Duplicate Leads")
        st.warning("These actions will modify your entire database and cannot be undone.", icon="⚠️")
        if st.button("Run Selected Maintenance Tasks", type="primary"):
            actions_to_run = {'clean_websites': action_clean_websites, 'remove_duplicates': action_remove_duplicates}
            if not any(actions_to_run.values()):
                st.error("Please select at least one maintenance action to run.")
            else:
                with st.spinner("Running database maintenance..."):
                    report = run_db_maintenance(config.DB_FILE, actions_to_run)
                st.success(f"**Maintenance Complete!**\n\n{report}")