# ui/tabs/view_bulk.py
"""
Renders the bulk data download tabs for Google Places and OpenStreetMap.
These tabs are for gathering large, raw datasets for offline analysis.
"""
import streamlit as st
import pandas as pd
import requests
import time
import datetime as dt

# Core and UI component imports
from ..components import create_styled_download_button
from core.harvesters import harvest_openstreetmap_bulk
from core.utils import dbg
from st_aggrid import AgGrid, GridOptionsBuilder

AGGRID_AVAILABLE = True  # Assuming it's installed

def render_bulk_places_tab(config):
    """Renders the UI for the bulk Google Places data download tab."""
    st.subheader("📦 Bulk Data Download (Google Places)")
    st.warning("⚠️ This feature uses multiple API calls and can be slow and/or costly. It fetches raw data directly from the Text Search API.")
    
    bulk_query = st.text_input("Places Search Query", "business Toronto", key="bulk_query_input")
    max_pages = st.number_input("Max result pages (~20 results/page)", 1, 5, 1, key="bulk_pages_input")
    max_records = st.number_input("Max total records to fetch", 10, 200, 20, key="bulk_cap_input")

    if st.button("Fetch & Prepare Download", key="bulk_fetch_btn"):
        if not config.PLACES_API_KEY:
            st.error("Google Places API Key is not configured.")
            return

        all_places = []
        token = None
        
        with st.spinner(f"Fetching up to {max_records} records..."):
            try:
                for page in range(max_pages):
                    if len(all_places) >= max_records:
                        break
                    
                    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
                    params = {"query": bulk_query, "key": config.PLACES_API_KEY}
                    if token:
                        params["pagetoken"] = token
                        time.sleep(2) # Required delay for next_page_token to become valid

                    resp = requests.get(url, params=params, timeout=25).json()
                    
                    if resp.get("status") == "OK":
                        all_places.extend(resp.get('results', []))
                        st.spinner(f"Page {page + 1}... Found {len(all_places)}/{max_records} total.")
                        token = resp.get('next_page_token')
                        if not token:
                            break # No more results
                    else:
                        st.error(f"API Error on page {page + 1}: {resp.get('status')}")
                        break
            except Exception as e:
                st.error(f"An unexpected error occurred during fetch: {e}")

        if all_places:
            df_bulk = pd.json_normalize(all_places[:max_records])
            st.session_state['bulk_places_df'] = df_bulk
        else:
            st.warning("No records were fetched.")
            st.session_state['bulk_places_df'] = pd.DataFrame()
            
    # Display results and download button if data is in session state
    if 'bulk_places_df' in st.session_state and not st.session_state['bulk_places_df'].empty:
        df_to_show = st.session_state['bulk_places_df']
        st.success(f"Fetched {len(df_to_show)} raw records from Google Places.")
        st.dataframe(df_to_show.head())

        create_styled_download_button(
            f"📥 Download {len(df_to_show)} Records (CSV)", df_to_show,
            f"places_bulk_{dt.datetime.now().strftime('%Y%m%d')}.csv", "text/csv",
            key="bulk_places_dl_btn", bg_color="#FFC107", text_color="#212529", hover_bg_color="#E0A800"
        )

def render_bulk_osm_tab(config):
    """Renders the UI for the bulk OpenStreetMap download tab."""
    st.subheader("🌍 Bulk OpenStreetMap Download")
    st.info("Fetch all features matching keywords within a named administrative area (e.g., a city).")
    st.warning("⚠️ Large areas or broad keywords can be very slow or may time out. Use specific tags like 'amenity=cafe' for best results.")

    osm_bulk_keywords = st.text_input("OSM Keywords (comma-separated)", "amenity=cafe, amenity=restaurant", key="osm_keywords_input")
    osm_bulk_area = st.text_input("Area Name (e.g., 'City of Toronto')", "Toronto", key="osm_area_input")

    if st.button("Fetch & Prepare OSM Download", key="osm_bulk_fetch_btn"):
        if not osm_bulk_keywords or not osm_bulk_area:
            st.warning("Please provide both Keywords and an Area Name.")
        else:
            with st.spinner(f"Querying Overpass API for '{osm_bulk_keywords}' in '{osm_bulk_area}'... This can take minutes."):
                # We create a small config dict to pass to the harvester
                osm_config = { 'user_agent': config.NOMINATIM_USER_AGENT }
                # This function now correctly returns a DataFrame, fixing the crash.
                df_osm_bulk = harvest_openstreetmap_bulk(osm_config, osm_bulk_keywords, osm_bulk_area)
                st.session_state['bulk_osm_df'] = df_osm_bulk
                
    # Display results if available in session state
    if 'bulk_osm_df' in st.session_state:
        df_osm = st.session_state['bulk_osm_df']
        # This check will now work correctly because df_osm is a DataFrame.
        if not df_osm.empty:
            st.success(f"Fetched {len(df_osm)} features from OpenStreetMap.")
            
            # Show a preview using AgGrid if available
            if AGGRID_AVAILABLE:
                gb = GridOptionsBuilder.from_dataframe(df_osm)
                gb.configure_default_column(filter=True, sortable=True, resizable=True, wrapText=True, autoHeight=True)
                gb.configure_pagination(enabled=True, paginationPageSize=15)
                AgGrid(df_osm, gridOptions=gb.build(), height=400, key='osm_bulk_preview', update_mode='NO_UPDATE', fit_columns_on_grid_load=True)
            else:
                st.dataframe(df_osm)

            create_styled_download_button(
                f"📥 Download {len(df_osm)} OSM Records (CSV)", df_osm,
                f"osm_bulk_{osm_bulk_area.replace(' ', '_')}_{dt.datetime.now().strftime('%Y%m%d')}.csv", "text/csv",
                key="osm_bulk_dl_btn", bg_color="#6F42C1", hover_bg_color="#5A32A3"
            )
        else:
            # Handle the case where the fetch ran but found nothing
            st.info("No matching features found in OSM for the given criteria, or the last fetch failed.")