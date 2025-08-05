# ui/tabs/view_smart_lists.py
"""
Renders the "Smart Lists" tab in the Streamlit UI.
This feature allows users to create and view AI-categorized lists of leads.
"""
import streamlit as st
import pandas as pd
import time

# Core module imports
from core.database import get_smart_list_names, get_leads_for_smart_list, update_lead_in_db
from core.categorization import build_smart_list
from ui.components import render_enrichment_widget
from core.utils import dbg

def render_smart_lists_tab(config):
    """Renders the entire UI for the Smart Lists feature."""
    st.subheader("🧠 AI-Powered Smart Lists")
    st.caption("Automatically categorize your leads into targeted lists based on a defined goal.")

    # --- Part 1: Create a New Smart List ---
    with st.form(key="smart_list_form"):
        st.write("#### Create a New Smart List")
        list_name = st.text_input("List Name", placeholder="e.g., 'Restaurants without Websites'")
        list_goal = st.text_area("List Goal", placeholder="Describe the ideal lead for this list. For example: 'Restaurants in Toronto that do not have a website, making them good candidates for web development services.'")
        st.write("##### Pre-Filter for AI Analysis")
        c1, c2, c3 = st.columns(3)
        with c1:
            search_name = st.text_input("Filter by Name", placeholder="e.g., 'pizza'")
        with c2:
            search_biz_type = st.text_input("Filter by Business Type", placeholder="e.g., 'restaurant'")
        with c3:
            has_website_filter = st.selectbox("Filter by Website", ["Any", "Yes", "No"], index=2)
        max_leads = st.slider("Max Leads to Analyze per Run", 10, 500, 50)
        submit_button = st.form_submit_button(label="🚀 Build Smart List")

    if submit_button:
        if not all([list_name, list_goal]):
            st.warning("Please fill out 'List Name' and 'List Goal'.")
        else:
            filters = {"search_name": search_name, "search_business_type": search_biz_type}
            if has_website_filter == "Yes": filters['has_website'] = True
            elif has_website_filter == "No": filters['has_website'] = False
            status_placeholder = st.empty()
            with st.spinner("Preparing to analyze leads..."):
                try:
                    added, failed = build_smart_list(config, list_name, list_goal, filters, max_leads)
                    status_placeholder.success(f"Analysis Complete! Added {added} new leads to '{list_name}'. Failed to analyze: {failed}.")
                except Exception as e:
                    status_placeholder.error(f"An error occurred while building the list: {e}")
                    dbg(f"Smart List Build Error: {e}")

    st.divider()

    # --- Part 2: View Existing Smart Lists ---
    st.write("#### View Existing Smart Lists")
    try:
        existing_lists = get_smart_list_names(config.DB_FILE)
    except Exception as e:
        st.error(f"Could not load smart lists from the database: {e}"); existing_lists = []

    if not existing_lists:
        st.info("You haven't created any smart lists yet. Use the form above to build your first one!")
    else:
        selected_list = st.selectbox("Select a list to view", options=existing_lists)
        if selected_list:
            with st.spinner(f"Loading leads for '{selected_list}'..."):
                df_list = get_leads_for_smart_list(config.DB_FILE, selected_list)
            
            if df_list.empty:
                st.warning(f"The list '{selected_list}' is currently empty.")
            else:
                st.write(f"Displaying **{len(df_list)}** leads from the '{selected_list}' list.")
                
                # --- THIS IS THE FIX: Added 'Select All' checkbox ---
                select_all = st.checkbox("Select All Leads in This List", key="smart_list_select_all")
                df_list.insert(0, 'Select', select_all)
                
                column_config = {
                    "Select": st.column_config.CheckboxColumn("Select", required=True),
                    "id": "ID", "name": "Name", "website": st.column_config.LinkColumn("Website"),
                    "phone": "Phone", "email": "Email", "address": "Address",
                    "ai_category": st.column_config.Column("AI Category", help="The category assigned by the AI."),
                    "ai_justification": st.column_config.Column("AI Justification", width="large"),
                }
                
                edited_df = st.data_editor(
                    df_list, column_config=column_config, use_container_width=True,
                    hide_index=True, key="smart_list_editor"
                )
                selected_leads = edited_df[edited_df['Select']]
                st.divider()

                col1, col2 = st.columns([2,1])
                with col1:
                    render_enrichment_widget(selected_leads, location="smart_list_view")
                with col2:
                    if st.button(f"🏷️ Save List to Source", help="This will update the 'source' field for all leads in this list, allowing you to easily find them in the main Database View."):
                        source_name = f"smartlist_{selected_list.lower().replace(' ', '_')}"
                        with st.spinner(f"Updating source for {len(df_list)} leads to '{source_name}'..."):
                            updated_count = 0
                            for lead_id in df_list['id']:
                                if update_lead_in_db(config.DB_FILE, lead_id, 'source', source_name):
                                    updated_count += 1
                        st.success(f"Successfully updated {updated_count} leads. You can now filter for this source in the 'Database View' tab.")
                        time.sleep(3)