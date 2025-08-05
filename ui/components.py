# ui/components.py
"""
Contains reusable Streamlit UI components used across different tabs and the sidebar.
"""
import streamlit as st
import base64
import pandas as pd
import time

# St_AgGrid is an optional dependency for this component
try:
    from st_aggrid import JsCode
    AGGRID_AVAILABLE = True
except ImportError:
    AGGRID_AVAILABLE = False
    
# Import core logic functions required by components
from core.utils import load_api_usage_df, dbg
from core.action_dispatcher import run_enrichment_action


def create_styled_download_button(button_text, data_to_download, download_filename, mime_type, key,
                                 bg_color="#1E88E5", text_color="#FFFFFF", hover_bg_color="#1565C0"):
    """Creates a custom-styled download button using HTML and Base64 encoding."""
    try:
        if isinstance(data_to_download, pd.DataFrame):
            data_b64 = base64.b64encode(data_to_download.to_csv(index=False).encode()).decode()
        elif isinstance(data_to_download, str):
            data_b64 = base64.b64encode(data_to_download.encode()).decode()
        elif isinstance(data_to_download, bytes):
            data_b64 = base64.b64encode(data_to_download).decode()
        else:
            st.error("Unsupported data type for download.")
            return

        href = f'<a href="data:{mime_type};base64,{data_b64}" download="{download_filename}" style="text-decoration: none;">'
        style = f"""
            <style>
                #{key} {{
                    display: inline-block; padding: 0.5em 1em; font-size: 1em;
                    font-weight: bold; color: {text_color} !important; background-color: {bg_color};
                    border: none; border-radius: 0.25rem; text-align: center;
                    cursor: pointer; transition: background-color 0.2s ease-in-out;
                }}
                #{key}:hover {{ background-color: {hover_bg_color}; color: {text_color} !important; }}
            </style>
        """
        button_html = f'{style}{href}<button id="{key}">{button_text}</button></a>'
        st.markdown(button_html, unsafe_allow_html=True)
    except Exception as e:
        dbg(f"[Download Button ERR] for key '{key}': {e}")
        st.warning(f"Could not generate download link for '{download_filename}'.")


def display_api_usage_summary(api_log_file):
    """Displays the API usage monitor in the sidebar."""
    st.sidebar.subheader("📊 API Usage Monitor")
    df_usage = load_api_usage_df(api_log_file)

    if df_usage.empty:
        st.sidebar.info("No API usage logged yet.")
        return

    now = pd.Timestamp.now(tz='UTC')
    today_usage = df_usage[df_usage['timestamp'].dt.date == now.date()]
    this_month_usage = df_usage[
        (df_usage['timestamp'].dt.year == now.year) &
        (df_usage['timestamp'].dt.month == now.month)
    ]

    st.sidebar.metric("Cost Today", f"${today_usage['cost'].sum():.2f}")
    st.sidebar.metric("Cost This Month", f"${this_month_usage['cost'].sum():.2f}")

    if st.sidebar.button("Refresh Usage Stats", key="refresh_api_usage"):
        st.rerun()

# AgGrid JavaScript renderers for clickable links in tables
if AGGRID_AVAILABLE:
    render_link_js = JsCode(r"""
        function(params) {
            if (params.value == null || params.value === '') { return ''; }
            let url = params.value;
            if (!url.startsWith('http://') && !url.startsWith('https://')) { url = 'https://' + url; }
            return '<a href="' + url + '" target="_blank" rel="noopener noreferrer">' + params.value + '</a>';
        }
    """)
    render_phone_js = JsCode(r"""
        function(params) {
            if (params.value == null || params.value === '') { return ''; }
            let phoneNumber = String(params.value).replace(/[-\s\(\)]/g, '');
            return '<a href="tel:' + phoneNumber + '">' + params.value + '</a>';
        }
    """)
else:
    render_link_js = None
    render_phone_js = None

# --- NEW FUNCTION ADDED HERE ---
# This version uses st.expander for compatibility with older Streamlit versions.
def render_enrichment_widget(selected_leads_df, location="main"):
    """
    Renders the "Enrich Selected Leads" button and a pop-up-like expander.
    This function can be called from any page that has a dataframe with a 'Select' column.

    Args:
        selected_leads_df (pd.DataFrame): The dataframe of selected leads.
        location (str): A unique key prefix for Streamlit widgets (e.g., 'db_view', 'smart_list').
    """
    # Use a unique session state key for the expander's visibility
    expander_state_key = f'show_enrich_expander_{location}'
    if expander_state_key not in st.session_state:
        st.session_state[expander_state_key] = False

    if selected_leads_df.empty:
        st.info("Select one or more leads from the table to perform an action.")
        # Ensure the expander is closed if no rows are selected
        st.session_state[expander_state_key] = False
        return

    st.write(f"**{len(selected_leads_df)}** lead(s) selected.")

    # This button toggles the visibility of the expander
    if st.button(f"⚡️ Enrich Selected Leads...", key=f"enrich_button_{location}"):
        st.session_state[expander_state_key] = not st.session_state[expander_state_key]

    # The "Dialog" is now an expander controlled by session state
    with st.expander("Enrichment Agent Widget", expanded=st.session_state[expander_state_key]):
        st.subheader(f"⚡️ Run an Agent on {len(selected_leads_df)} Selected Lead(s)")
        st.caption("The selected agent will process the leads you've chosen from the table.")
        
        available_agents = [
            "Find & Fill (Selenium - Free)",
            "Find & Fill (Google API - Paid)",
            "Deep Analysis Report"
        ]
        
        selected_agent = st.selectbox(
            "Choose an Enrichment Agent:",
            options=available_agents,
            key=f"agent_selector_{location}"
        )
        
        st.warning(f"**Agent:** {selected_agent}\n\nThis will process {len(selected_leads_df)} leads. This action cannot be undone.", icon="⚠️")

        if st.button("🚀 Launch Agent", type="primary", use_container_width=True, key=f"launch_agent_{location}"):
            lead_ids = selected_leads_df['id'].tolist()
            
            if 'config' not in st.session_state:
                 st.error("Configuration object not found in session state. Cannot run agent.")
                 return

            with st.spinner(f"Running '{selected_agent}'... Please wait."):
                success_msg, error_msg = run_enrichment_action(
                    action_name=selected_agent,
                    lead_ids=lead_ids,
                    config=st.session_state.config
                )
            
            if error_msg:
                st.error(error_msg)
            else:
                st.success(success_msg)
                
            time.sleep(3)
            # Close the expander and rerun the app to refresh the data
            st.session_state[expander_state_key] = False
            st.rerun()

        if st.button("Cancel", use_container_width=True, key=f"cancel_agent_{location}"):
            st.session_state[expander_state_key] = False
            st.rerun()