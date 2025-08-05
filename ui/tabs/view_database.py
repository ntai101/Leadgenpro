# ui/tabs/view_database.py
import streamlit as st
import pandas as pd
import os
import math
import time
import json

# Core module imports
from core.database import load_db_paginated, get_filtered_lead_count, delete_leads_from_db, export_leads_to_file
from ui.components import render_enrichment_widget
from core.utils import dbg

def render_database_tab(config):
    """
    Renders the main database view with a details expander and customizable columns.
    """
    st.header("📊 Database View")
    st.caption("Filter, view, select, and run actions on the leads in your database.")

    # --- 1. FILTERS & COLUMN SELECTION ---
    with st.expander("Filter and View Options", expanded=True):
        
        # Define all possible columns the user can choose to see
        ALL_COLUMNS = ['id', 'name', 'title', 'website', 'phone', 'email', 'address', 'linkedin', 'business_type', 'source', 'ts']
        DEFAULT_COLUMNS = ['id', 'name', 'website', 'phone', 'address', 'linkedin']

        # Initialize session state for filters and column visibility
        if 'db_filters' not in st.session_state:
            st.session_state.db_filters = {
                'name': '', 'address': '', 'biz_type': '', 'website': 'Any', 'phone': 'Any',
                'visible_columns': DEFAULT_COLUMNS
            }

        # --- Filter Inputs ---
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.db_filters['name'] = st.text_input("Search by Name", st.session_state.db_filters['name'])
            st.session_state.db_filters['address'] = st.text_input("Search by Address", st.session_state.db_filters['address'])
            st.session_state.db_filters['phone'] = st.selectbox("Has Phone?", ["Any", "Yes", "No"], index=["Any", "Yes", "No"].index(st.session_state.db_filters['phone']))
        with col2:
            st.session_state.db_filters['biz_type'] = st.text_input("Search by Business Type", st.session_state.db_filters['biz_type'])
            st.session_state.db_filters['website'] = st.selectbox("Has Website?", ["Any", "Yes", "No"], index=["Any", "Yes", "No"].index(st.session_state.db_filters['website']))
        
        # --- NEW: Column Selection Widget ---
        st.session_state.db_filters['visible_columns'] = st.multiselect(
            "Select Visible Columns",
            options=ALL_COLUMNS,
            default=st.session_state.db_filters['visible_columns']
        )

    def get_bool_filter(value_str):
        if value_str == "Yes": return True
        if value_str == "No": return False
        return None

    current_filters = {
        "search_name": st.session_state.db_filters['name'],
        "search_address": st.session_state.db_filters['address'],
        "search_business_type": st.session_state.db_filters['biz_type'],
        "has_website": get_bool_filter(st.session_state.db_filters['website']),
        "has_phone": get_bool_filter(st.session_state.db_filters['phone'])
    }

    # --- 2. PAGINATION & DATA LOADING ---
    total_leads = get_filtered_lead_count(config.DB_FILE, **current_filters)
    page_options = [10, 25, 50, 100, 200, 500, 1000, 5000, 10000]
    col_page1, col_page2, col_page3 = st.columns([1, 1, 3])
    with col_page1:
        try:
            default_limit_index = page_options.index(st.session_state.get('db_limit', 50))
        except ValueError:
            default_limit_index = 2
        st.selectbox("Rows per page", page_options, index=default_limit_index, key="db_limit")
    page_size = st.session_state.db_limit
    total_pages = max(1, math.ceil(total_leads / page_size)) if page_size > 0 else 1
    with col_page2:
        st.number_input(f"Page (1-{total_pages})", min_value=1, max_value=total_pages, key="db_current_page")
    with col_page3:
         st.info(f"Showing page **{st.session_state.db_current_page}** of **{total_pages}**. (Total matching leads: **{total_leads}**)")
    df_leads = load_db_paginated(config.DB_FILE, page_number=st.session_state.db_current_page, page_size=page_size, **current_filters)

    # --- 3. DATA TABLE & SELECTION ---
    if not df_leads.empty:
        select_all = st.checkbox("Select All on Current Page", key="db_view_select_all")
        df_leads.insert(0, "Select", select_all)

        # **MODIFIED**: Use the user's column selection to define the table view.
        # Always include the 'Select' column for functionality.
        display_columns = ['Select'] + st.session_state.db_filters.get('visible_columns', DEFAULT_COLUMNS)
        
        edited_df = st.data_editor(
            df_leads,
            # **MODIFIED**: The column_config now contains all possible columns to ensure proper formatting
            # even when columns are hidden and then re-enabled by the user.
            column_config={
                "Select": st.column_config.CheckboxColumn(required=True),
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "name": st.column_config.TextColumn("Name", disabled=True),
                "title": st.column_config.TextColumn("Title", disabled=True),
                "website": st.column_config.LinkColumn("Website", disabled=True),
                "phone": st.column_config.TextColumn("Phone", disabled=True),
                "email": st.column_config.TextColumn("Email", disabled=True),
                "address": st.column_config.TextColumn("Address", width="medium", disabled=True),
                "linkedin": st.column_config.LinkColumn("LinkedIn", disabled=True),
                "business_type": st.column_config.TextColumn("Business Type", disabled=True),
                "source": st.column_config.TextColumn("Source", disabled=True),
                "ts": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm", disabled=True),
            },
            column_order=display_columns,
            hide_index=True,
            use_container_width=True,
            key="leads_data_editor"
        )
        
        selected_leads_df = edited_df[edited_df['Select']]
        st.divider()

        # --- DETAIL VIEW FOR A SINGLE SELECTED LEAD ---
        if len(selected_leads_df) == 1:
            with st.expander("🕵️ Show Full Details for Selected Lead", expanded=False):
                full_lead_data = selected_leads_df.iloc[0]
                st.subheader(full_lead_data.get('name', 'N/A'))
                c1, c2 = st.columns(2)
                with c1:
                    st.write(f"**Business Type:** {full_lead_data.get('business_type', 'N/A')}")
                    st.write(f"**Address:** {full_lead_data.get('address', 'N/A')}")
                    st.write(f"**Phone:** {full_lead_data.get('phone', 'N/A')}")
                    st.write(f"**Email:** {full_lead_data.get('email', 'N/A')}")
                with c2:
                    st.write(f"**Website:** {full_lead_data.get('website', 'N/A')}")
                    st.write(f"**LinkedIn:** {full_lead_data.get('linkedin', 'N/A')}")
                    social_links_str = full_lead_data.get('social_media_links')
                    if social_links_str and isinstance(social_links_str, str):
                        try:
                            social_links = json.loads(social_links_str)
                            st.write(f"**Facebook:** {social_links.get('facebook', 'N/A')}")
                            st.write(f"**Instagram:** {social_links.get('instagram', 'N/A')}")
                        except json.JSONDecodeError:
                            st.write("Could not parse other social media links.")
        
        # --- ACTIONS FOR MULTIPLE SELECTED LEADS ---
        if not selected_leads_df.empty:
            st.subheader(f"Actions for {len(selected_leads_df)} Selected Lead(s)")
            action_col1, action_col2 = st.columns([2, 1])
            with action_col1:
                render_enrichment_widget(selected_leads_df, location="db_view")
            with action_col2:
                if st.button(f"🗑️ Delete Selected Leads", use_container_width=True):
                    lead_ids_to_delete = selected_leads_df['id'].tolist()
                    with st.spinner("Deleting selected leads..."):
                        deleted_count = delete_leads_from_db(config.DB_FILE, lead_ids_to_delete)
                    st.success(f"Successfully deleted {deleted_count} leads.")
                    time.sleep(2)
                    st.rerun()
        else:
            st.info("Select one or more leads from the table above to perform an action.")

    else:
        st.info("No leads found matching the current filters.")

    # --- EXPORT DATA ---
    with st.expander("Export Filtered Leads to File"):
        export_file_name = st.text_input("Export File Name", "leads_export")
        export_format = st.selectbox("Format", ["CSV", "Excel"])
        if st.button("Generate & Download Export File"):
            if total_leads > 0:
                with st.spinner(f"Generating file with {total_leads} leads..."):
                    output_filename = f"{export_file_name}.{export_format.lower()}"
                    output_path = os.path.join(config.DOWNLOAD_DIR, output_filename)
                    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
                    success = export_leads_to_file(config.DB_FILE, output_path, export_format.lower(), **current_filters)
                    if success:
                        st.success(f"File generated: {output_filename}")
                        with open(output_path, "rb") as f:
                            st.download_button(
                                label=f"Download {output_filename}",
                                data=f,
                                file_name=output_filename,
                                mime="text/csv" if export_format == "CSV" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                    else:
                        st.error("Failed to generate the export file. Check logs for details.")
            else:
                st.warning("No leads to export based on current filters.")