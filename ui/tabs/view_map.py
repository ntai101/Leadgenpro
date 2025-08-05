# ui/tabs/view_map.py
"""
Renders the map-related tabs in the Streamlit UI.
- Tab 1: "Map Search" for finding nearby businesses via Google Places or OpenStreetMap.
- Tab 2: "Full Map" for visualizing all geolocated leads in the database.
"""
import streamlit as st
import pandas as pd
import pydeck as pdk
import math
import time

# Core module imports
from core.database import load_db, upsert_leads
from core.external_apis import geocode_location
from core.harvesters import harvest_places_nearby, harvest_osm_nearby
from core.utils import dbg

def render_map_search_tab(config):
    """Renders the UI for the 'Nearby Business Search' tab with source selection."""
    st.subheader("🗺️ Nearby Business Search")
    st.caption("Find businesses within a specific radius of a central point.")

    # Initialize session state for map search results and center coordinates
    if 'map_search_results' not in st.session_state:
        st.session_state.map_search_results = pd.DataFrame()
    if 'map_center_coords' not in st.session_state:
        st.session_state.map_center_coords = (43.6532, -79.3832) # Default to Toronto

    col1, col2 = st.columns([1, 2])
    
    with col1:
        # --- INPUTS ---
        source = st.selectbox(
            "Lead Source",
            ("OpenStreetMap (Free)", "Google Places (Paid)"),
            key="map_search_source",
            help="OpenStreetMap is free and great for broad discovery. Google Places provides more data but incurs API costs."
        )
        
        map_center_loc = st.text_input("Center Location", "Toronto, ON", key="map_center_input")
        map_radius_km = st.number_input("Search Radius (km)", 0.1, 50.0, 5.0, 0.5, key="map_radius_input")
        map_biz_keyword = st.text_input(
            "Business Keyword", "brewery", 
            key="map_keyword_input",
            help="For OSM, use tags like 'amenity=cafe'. For Google, use general keywords like 'plumber'."
        )

        if st.button("🔍 Fetch Businesses in Area", type="primary", key="fetch_nearby_btn"):
            st.session_state.map_search_results = pd.DataFrame() # Clear previous results
            
            if not map_center_loc or not map_biz_keyword:
                st.warning("Please enter a Center Location and a Business Keyword.")
                return

            with st.spinner(f"Geocoding '{map_center_loc}' and searching {source}..."):
                # --- FIXED: Use the general GCP_API_KEY for the geocoding fallback ---
                center_coords = geocode_location(
                    config.GCP_API_KEY, 
                    map_center_loc,
                    config.NOMINATIM_USER_AGENT,
                    config.API_USAGE_LOG_FILE
                )

                if center_coords:
                    st.session_state['map_center_coords'] = center_coords
                    hits = []
                    
                    if source == "Google Places (Paid)":
                        if not config.PLACES_API_KEY:
                            st.error("Google Places API Key is not configured.")
                            return
                        hits = harvest_places_nearby(
                            places_api_key=config.PLACES_API_KEY, 
                            keyword=map_biz_keyword,
                            center_lat=center_coords[0], 
                            center_lng=center_coords[1], 
                            radius_km=map_radius_km,
                            db_path=config.DB_FILE,
                            api_log_file=config.API_USAGE_LOG_FILE
                        )
                    else: # OpenStreetMap (Free)
                        osm_config = {'user_agent': config.NOMINATIM_USER_AGENT}
                        hits = harvest_osm_nearby(
                            osm_config=osm_config, 
                            keywords=map_biz_keyword, 
                            center_lat=center_coords[0], 
                            center_lng=center_coords[1],
                            radius_km=map_radius_km,
                            db_path=config.DB_FILE
                        )
                    
                    if hits:
                        st.session_state.map_search_results = pd.DataFrame(hits)
                    else:
                        st.info("Search returned no new results. Leads may already exist in your database or none were found in that area.")
                else:
                    st.error(f"Could not find coordinates for location: '{map_center_loc}'. This can happen if the free geocoder is busy or if your Google Geocoding API is not enabled or funded. Please try a more specific location (e.g., 'CN Tower, Toronto') or check your Google Cloud Console.")

    with col2:
        st.write("Map Preview")
        
        center_lat, center_lng = st.session_state.get('map_center_coords')
        zoom_level = 14 - math.log(map_radius_km * 2 if map_radius_km > 0.1 else 0.2, 2)
        
        layers = []
        if not st.session_state.map_search_results.empty:
            df_new_results = st.session_state.map_search_results.dropna(subset=['lat', 'lng']).copy()
            new_results_layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_new_results,
                get_position='[lng, lat]', get_radius=120,
                get_fill_color=[255, 0, 0, 180], # RED for new results
                pickable=True, auto_highlight=True
            )
            layers.append(new_results_layer)
        
        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(
                latitude=center_lat, longitude=center_lng,
                zoom=max(8, min(16, int(zoom_level))), pitch=30
            ),
            layers=layers,
            tooltip={"text": "NEW LEAD\nName: {name}\nAddress: {address}"}
        ))

    if not st.session_state.map_search_results.empty:
        st.divider()
        st.write(f"Found **{len(st.session_state.map_search_results)}** new potential leads:")
        st.dataframe(st.session_state.map_search_results)
        
        if st.button(f"✅ Add {len(st.session_state.map_search_results)} Leads to Database", key="add_map_leads_btn"):
            with st.spinner("Adding leads to the database..."):
                hits_list = st.session_state.map_search_results.to_dict('records')
                inserted, skipped = upsert_leads(config.DB_FILE, hits_list)
                st.success(f"Operation complete! Inserted: {inserted}, Skipped (final check): {skipped}")
                st.session_state.map_search_results = pd.DataFrame()
                time.sleep(1)
                st.rerun()

def render_full_map_tab(config):
    """Renders the UI for the 'Full Map of All Leads' tab."""
    st.subheader("📍 Map of All Geolocated Leads in Database")
    
    with st.spinner("Loading all geolocated leads..."):
        df_all_leads = load_db(config.DB_FILE, limit=10000)
        df_geocoded = df_all_leads.dropna(subset=['lat', 'lng']).copy()

    if df_geocoded.empty:
        st.info("No geolocated leads found in the database.")
        return

    st.info(f"Displaying {len(df_geocoded)} geolocated leads on the map.")
    center_lat = df_geocoded['lat'].mean()
    center_lng = df_geocoded['lng'].mean()
    lat_span = df_geocoded['lat'].max() - df_geocoded['lat'].min()
    lng_span = df_geocoded['lng'].max() - df_geocoded['lng'].min()
    max_span = max(lat_span, lng_span, 0.01)
    zoom_level = 11 - math.log(max_span * 1.5, 2)

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/streets-v11",
        initial_view_state=pdk.ViewState(
            latitude=center_lat, longitude=center_lng,
            zoom=max(1, min(16, int(zoom_level))), pitch=45
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=df_geocoded, get_position='[lng, lat]', get_radius=150,
                get_fill_color=[0, 100, 200, 160], # Blue dots for existing leads
                pickable=True, auto_highlight=True
            )
        ],
        tooltip={"html": "<b>{name}</b><br/>Source: {source}<br/>Address: {address}"}
    ))