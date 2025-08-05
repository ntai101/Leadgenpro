# ui/tabs/view_harvest.py
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from ..components import create_styled_download_button, AGGRID_AVAILABLE, render_link_js

def render_harvest_tab():
    st.subheader("🌟 Leads from Last Harvest/Import")
    
    df_latest = st.session_state.get("latest_harvest", pd.DataFrame())
    
    if not df_latest.empty:
        st.write(f"Displaying {len(df_latest)} new leads.")
        df_display = df_latest.astype(object).where(pd.notnull(df_latest), None)

        if AGGRID_AVAILABLE:
            gb = GridOptionsBuilder.from_dataframe(df_display)
            if 'website' in df_display.columns:
                gb.configure_column("website", cellRenderer=render_link_js)
            gb.configure_pagination(enabled=True, paginationAutoPageSize=True)
            AgGrid(df_display, gridOptions=gb.build(), height=400, width='100%',
                   allow_unsafe_jscode=True, key='latest_harvest_grid')
        else:
            st.dataframe(df_display, use_container_width=True)

        create_styled_download_button(
            "📥 Download These Leads (CSV)", df_display, "latest_leads.csv", "text/csv",
            key="download_latest_btn", bg_color="#28A745", hover_bg_color="#218838"
        )
    else:
        st.info("No leads harvested in this session. Use the sidebar to start a new harvest.")