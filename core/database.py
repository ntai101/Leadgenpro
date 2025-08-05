# core/database.py
"""
Handles all database operations for the lead generation application.
- Initializes the database schema.
- Performs CRUD (Create, Read, Update, Delete) operations on leads.
- Imports data from external files into the database.
"""
import os
import sqlite3
import pandas as pd
import datetime as dt
import json
from urllib.parse import urlparse
from .logging import dbg

def init_db(db_file):
    """Initializes the database and creates tables if they don't exist."""
    try:
        dbg(f"Connecting to DB at: {db_file}")
        with sqlite3.connect(db_file) as con:
            con.execute("PRAGMA case_sensitive_like = OFF;")
            con.execute("PRAGMA foreign_keys = ON;")
            
            con.execute("""
                CREATE TABLE IF NOT EXISTS leads(
                    id INTEGER PRIMARY KEY, ts TEXT, record_type TEXT, source TEXT,
                    name TEXT NOT NULL, title TEXT, linkedin TEXT, website TEXT,
                    phone TEXT, email TEXT, domain TEXT, lat REAL, lng REAL, address TEXT,
                    business_type TEXT,
                    UNIQUE(name, domain)
                )""")
            
            cursor = con.cursor()
            cursor.execute("PRAGMA table_info(leads)")
            existing_cols_leads = {row[1] for row in cursor.fetchall()}
            if 'address' not in existing_cols_leads:
                con.execute("ALTER TABLE leads ADD COLUMN address TEXT")
            if 'business_type' not in existing_cols_leads:
                con.execute("ALTER TABLE leads ADD COLUMN business_type TEXT")

            con.execute("""
                CREATE TABLE IF NOT EXISTS enriched(
                    id INTEGER PRIMARY KEY, lead_id INTEGER UNIQUE NOT NULL, psi INTEGER,
                    public_emails TEXT, pattern TEXT,
                    FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE
                )""")
            
            con.execute("""
            CREATE TABLE IF NOT EXISTS advanced_lead_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER UNIQUE NOT NULL, 
                identified_needs TEXT,          
                outreach_strategy TEXT,         
                critical_missing_info TEXT,
                pagespeed_score_latest REAL,
                website_analysis_notes TEXT,    
                social_media_links TEXT,        
                screenshot_path TEXT,
                last_analyzed_timestamp TEXT,
                FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE
            )""")

            con.execute("""
            CREATE TABLE IF NOT EXISTS smart_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_name TEXT NOT NULL,
                lead_id INTEGER NOT NULL,
                ai_category TEXT,
                ai_justification TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads (id) ON DELETE CASCADE,
                UNIQUE(list_name, lead_id)
            )""")
            
            con.commit()
            dbg("Database initialized/checked.")
    except sqlite3.Error as e:
        dbg(f"DB ERR: Initialization failed: {e} (DB Path: {db_file})")
        raise e

def remove_db_duplicates(db_file):
    """Removes duplicate leads from the database based on name and domain."""
    dbg("DB Duplicates: Starting check for duplicates...")
    total_removed = 0
    try:
        with sqlite3.connect(db_file) as con:
            cursor = con.cursor()
            query_delete_domain_dupes = """
                DELETE FROM leads
                WHERE id NOT IN (
                    SELECT MIN(id) FROM leads
                    WHERE domain IS NOT NULL AND domain != '' AND name IS NOT NULL AND name != ''
                    GROUP BY LOWER(TRIM(name)), LOWER(TRIM(domain))
                ) AND domain IS NOT NULL AND domain != '' AND name IS NOT NULL AND name != '';
            """
            cursor.execute(query_delete_domain_dupes)
            total_removed += cursor.rowcount
            query_delete_null_domain_dupes = """
                DELETE FROM leads
                WHERE id NOT IN (
                    SELECT MIN(id) FROM leads
                    WHERE (domain IS NULL OR domain = '') AND name IS NOT NULL AND name != ''
                    GROUP BY LOWER(TRIM(name))
                ) AND (domain IS NULL OR domain = '') AND name IS NOT NULL AND name != '';
            """
            cursor.execute(query_delete_null_domain_dupes)
            total_removed += cursor.rowcount
            con.commit()
            if total_removed > 0:
                dbg(f"DB Duplicates: Total removed: {total_removed}.")
            else:
                dbg("DB Duplicates: No duplicates found requiring removal.")
    except sqlite3.Error as e:
        dbg(f"DB Duplicates ERR: Failed: {e}")
    return total_removed

def upsert_leads(db_file, hits):
    """Inserts or ignores new leads into the database to avoid duplicates."""
    inserted_count, skipped_count = 0, 0
    if not hits:
        dbg("DB Upsert: No hits to process.")
        return 0, 0
    try:
        with sqlite3.connect(db_file) as con:
            existing_with_domain = set(con.execute(
                "SELECT LOWER(TRIM(name)), LOWER(TRIM(domain)) FROM leads WHERE domain IS NOT NULL AND domain != ''"
            ).fetchall())
            existing_without_domain = set(row[0] for row in con.execute(
                "SELECT LOWER(TRIM(name)) FROM leads WHERE domain IS NULL OR domain = ''"
            ).fetchall())
            
            cursor = con.cursor()
            for h in hits:
                name = str(h.get("name", "")).strip().lower()
                domain = str(h.get("domain", "")).strip().lower() if h.get("domain") else None
                if not name:
                    skipped_count += 1
                    continue
                is_duplicate = (domain and (name, domain) in existing_with_domain) or \
                               (not domain and name in existing_without_domain)
                if is_duplicate:
                    skipped_count += 1
                    continue
                data_tuple = (
                    h.get('ts', dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")),
                    h.get('record_type'), h.get('source'), h.get('name'), h.get('title'),
                    h.get('linkedin'), h.get('website'), h.get('phone'), h.get('email'),
                    h.get('domain'), h.get('lat'), h.get('lng'), h.get('address'),
                    h.get('business_type')
                )
                try:
                    cursor.execute("""
                        INSERT INTO leads(ts, record_type, source, name, title, linkedin, website, phone, email, domain, lat, lng, address, business_type)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, data_tuple)
                    if cursor.rowcount > 0:
                        inserted_count += 1
                        if domain:
                            existing_with_domain.add((name, domain))
                        else:
                            existing_without_domain.add(name)
                except sqlite3.IntegrityError:
                    skipped_count += 1
            con.commit()
        dbg(f"DB Upsert: Inserted: {inserted_count}, Skipped (dupes/errors): {skipped_count}")
    except sqlite3.Error as e:
        dbg(f"DB Upsert ERR: Transaction failed: {e}")
    return inserted_count, skipped_count

# --- MODIFIED: Added 'search_website' parameter ---
def _get_db_where_clauses(search_name="", search_domain="", search_source="", search_address="", search_business_type="", search_website="", has_phone=None, has_website=None):
    """A helper function to build the WHERE clause and parameters for filtering."""
    params = []
    where_clauses = []
    
    if search_name: 
        where_clauses.append("l.name LIKE ?")
        params.append(f"%{search_name}%")
    if search_domain: 
        where_clauses.append("l.domain LIKE ?")
        params.append(f"%{search_domain}%")
    if search_source: 
        where_clauses.append("l.source LIKE ?")
        params.append(f"%{search_source}%")
    if search_address: 
        where_clauses.append("l.address LIKE ?")
        params.append(f"%{search_address}%")
    if search_business_type: 
        where_clauses.append("l.business_type LIKE ?")
        params.append(f"%{search_business_type}%")
    
    # --- NEW: Added search_website condition ---
    if search_website:
        where_clauses.append("l.website LIKE ?")
        params.append(f"%{search_website}%")
        
    if has_phone is True: 
        where_clauses.append("(l.phone IS NOT NULL AND l.phone != '')")
    elif has_phone is False: 
        where_clauses.append("(l.phone IS NULL OR l.phone = '')")
        
    if has_website is True:
        where_clauses.append("(l.website IS NOT NULL AND l.website != '')")
    elif has_website is False:
        where_clauses.append("(l.website IS NULL OR l.website = '')")

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    return where_sql, params

def get_filtered_lead_count(db_file, **filters):
    """Gets the total count of leads that match the current filters."""
    where_sql, params = _get_db_where_clauses(**filters)
    query = f"SELECT COUNT(l.id) FROM leads l{where_sql}"
    try:
        with sqlite3.connect(db_file) as con:
            count = con.execute(query, params).fetchone()[0]
            return count
    except sqlite3.Error as e:
        dbg(f"DB Count ERR: {e}")
        return 0

def load_db_paginated(db_file, page_number=1, page_size=5000, query_override=None, **filters):
    """Loads a single page of leads from the database with optional filtering and sorting."""
    try:
        with sqlite3.connect(db_file) as con:
            if query_override:
                dbg(f"Executing override query: {query_override}")
                return pd.read_sql_query(query_override, con)
            where_sql, params = _get_db_where_clauses(**filters)
            offset = (page_number - 1) * page_size
            pagination_params = [page_size, offset]
            query = f"""
            SELECT l.*, ar.id as report_id, ar.identified_needs, ar.outreach_strategy, ar.social_media_links
            FROM leads l
            LEFT JOIN advanced_lead_reports ar ON l.id = ar.lead_id
            {where_sql}
            ORDER BY l.id DESC
            LIMIT ? OFFSET ?
            """
            df = pd.read_sql_query(query, con, params=(params + pagination_params))
            dbg(f"DB Load: Loaded page {page_number} ({len(df)} rows).")
            return df
    except sqlite3.Error as e:
        dbg(f"DB Paginated Load ERR: {e}")
        return pd.DataFrame()

def load_db(db_file, limit=None, **filters):
    """A backward-compatible wrapper for the paginated DB loader."""
    dbg("Called legacy `load_db`; redirecting to `load_db_paginated`.")
    return load_db_paginated(db_file, page_number=1, page_size=limit if limit else 5000, **filters)

def check_lead_exists(db_path, name, address):
    """Checks if a lead with a similar name and address already exists in the database."""
    if not name or not address: return False
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            query = "SELECT 1 FROM leads WHERE name LIKE ? AND address LIKE ? LIMIT 1"
            name_like = f"%{name}%"
            address_like = f"{address[:15]}%"
            cursor.execute(query, (name_like, address_like))
            result = cursor.fetchone()
            return result is not None
    except sqlite3.Error as e:
        dbg(f"[DB Check Error] Could not check for lead existence: {e}")
        return False

def unenriched(db_file):
    """Finds leads with domains that have not yet been basically enriched."""
    try:
        with sqlite3.connect(db_file) as con:
            query = "SELECT l.* FROM leads l LEFT JOIN enriched e ON l.id = e.lead_id WHERE l.domain IS NOT NULL AND l.domain != '' AND e.lead_id IS NULL;"
            df = pd.read_sql_query(query, con)
            dbg(f"DB Unenriched: Found {len(df)} leads needing basic enrichment.")
            return df
    except Exception as e:
        dbg(f"DB Unenriched ERR: {e}")
        return pd.DataFrame()

def save_enriched(db_file, lead_id, data):
    """Saves the results of basic (non-AI) enrichment to the 'enriched' table."""
    if not isinstance(data, dict): return False
    try:
        with sqlite3.connect(db_file) as con:
            con.execute("INSERT OR IGNORE INTO enriched(lead_id, psi, public_emails, pattern) VALUES(?,?,?,?)",
                        (lead_id, data.get("psi"), data.get("public_emails"), data.get("pattern")))
            con.commit()
            return True
    except sqlite3.Error as e:
        dbg(f"DB Save Enriched ERR: DB error for lead_id {lead_id}: {e}")
        return False

def update_lead_in_db(db_file, lead_id, column, new_value):
    """Updates a single column for a single lead in the database."""
    allowed_cols = ['name', 'title', 'linkedin', 'website', 'phone', 'email', 'domain', 'address', 'record_type', 'source', 'business_type']
    if column not in allowed_cols:
        dbg(f"DB Update ERR: Invalid column for update: {column}")
        return False
    try:
        with sqlite3.connect(db_file) as con:
            cursor = con.cursor()
            query = f"UPDATE leads SET {column} = ? WHERE id = ?"
            cursor.execute(query, (new_value, lead_id))
            con.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        dbg(f"DB Update ERR: DB error for ID {lead_id}: {e}")
        return False

def delete_leads_from_db(db_file, lead_ids):
    """Deletes a list of leads from the database by their IDs."""
    if not lead_ids: return 0
    dbg(f"DB Delete: Attempting to delete {len(lead_ids)} leads.")
    try:
        with sqlite3.connect(db_file) as con:
            cursor = con.cursor()
            placeholders = ', '.join('?' for _ in lead_ids)
            query = f"DELETE FROM leads WHERE id IN ({placeholders})"
            cursor.execute(query, lead_ids)
            con.commit()
            return cursor.rowcount
    except sqlite3.Error as e:
        dbg(f"DB Delete ERR: DB error: {e}")
        return 0
        
def import_file_to_db(db_file, filepath):
    """Imports leads from a CSV or Excel file into the database."""
    try:
        filename = os.path.basename(filepath)
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext == '.csv':
            df = pd.read_csv(filepath)
        elif file_ext in ['.xlsx', '.xls']:
            engine = 'openpyxl' if file_ext == '.xlsx' else 'xlrd'
            df = pd.read_excel(filepath, engine=engine)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")

        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
        db_cols_map = {
            'company': 'name', 'company_name': 'name', 'business_name': 'name', 'contact_name': 'name',
            'job_title': 'title', 'contact_email': 'email', 'email_address': 'email',
            'company_website': 'website', 'url': 'website', 'linkedin_url': 'linkedin',
            'phone_number': 'phone', 'contact_number': 'phone', 'company_domain': 'domain',
            'latitude': 'lat', 'longitude': 'lng', 'street_address': 'address', 'location': 'address',
            'type': 'business_type', 'category': 'business_type'
        }
        df.rename(columns=db_cols_map, inplace=True)
        if 'name' not in df.columns: raise KeyError("'name' column is required.")
        
        def get_domain_from_url(url):
            if not isinstance(url, str) or pd.isna(url): return None
            if '://' not in url: url = 'http://' + url
            try:
                domain = urlparse(url).netloc.replace("www.", "")
                return domain if domain else None
            except Exception:
                return None
        if 'domain' not in df.columns and 'website' in df.columns:
            df['domain'] = df['website'].apply(get_domain_from_url)

        df['ts'] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        df['source'] = df.get('source', f"import_{filename}")
        df['record_type'] = df.get('record_type', 'business')
        
        db_cols = ['ts', 'record_type', 'source', 'name', 'title', 'linkedin', 'website', 'phone', 'email', 'domain', 'address', 'lat', 'lng', 'business_type']
        df_to_insert = df[[col for col in db_cols if col in df.columns]].copy()
        
        hits_list = df_to_insert.to_dict('records')
        inserted, skipped = upsert_leads(db_file, hits_list)
        return inserted, skipped
        
    except Exception as e:
        dbg(f"Import ERR: Failed to process {filepath}: {e}")
        raise e

def get_total_lead_count(db_file):
    """Returns the total number of leads in the database."""
    try:
        with sqlite3.connect(db_file) as con:
            count = con.execute("SELECT COUNT(id) FROM leads").fetchone()[0]
            return count
    except sqlite3.Error as e:
        dbg(f"DB ERR: Could not get total lead count: {e}")
        return 0

def get_lead_by_id(db_file, lead_id):
    """Fetches a single lead's data by its ID."""
    try:
        with sqlite3.connect(db_file) as con:
            con.row_factory = sqlite3.Row
            cursor = con.cursor()
            cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        dbg(f"DB ERR: Could not fetch lead ID {lead_id}: {e}")
        return None

def save_advanced_report(db_file, report_data):
    """Inserts or updates an AI-generated advanced lead report."""
    if not report_data or 'lead_id' not in report_data:
        dbg("DB Save Advanced ERR: Invalid or missing lead_id in report data.")
        return False
    query = """
    INSERT OR REPLACE INTO advanced_lead_reports (
        lead_id, identified_needs, outreach_strategy, critical_missing_info,
        pagespeed_score_latest, website_analysis_notes, social_media_links,
        screenshot_path, last_analyzed_timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    needs_json = json.dumps(report_data.get("identified_needs", []))
    strategy_json = json.dumps(report_data.get("outreach_strategy", []))
    social_json = json.dumps(report_data.get("social_media_links", {}))
    params = (
        report_data['lead_id'], needs_json, strategy_json, report_data.get('critical_missing_info'),
        report_data.get('pagespeed_score_latest'), report_data.get('website_analysis_notes'),
        social_json, report_data.get('screenshot_path'),
        dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    )
    try:
        with sqlite3.connect(db_file) as con:
            con.execute(query, params)
            con.commit()
        dbg(f"Successfully saved advanced report for lead ID: {report_data['lead_id']}")
        return True
    except sqlite3.Error as e:
        dbg(f"DB Save Advanced ERR: Failed for lead ID {report_data['lead_id']}: {e}")
        return False
    
def get_leads_for_enrichment(db_file, limit=100):
    """Fetches leads missing key information, making them ideal candidates for enrichment."""
    try:
        with sqlite3.connect(db_file) as con:
            query = """
                SELECT id, name, website, phone, email, address
                FROM leads
                WHERE (website IS NULL OR website = '') OR (phone IS NULL OR phone = '') OR
                      (email IS NULL OR email = '') OR (address IS NULL OR address = '')
                ORDER BY id DESC LIMIT ?
            """
            df = pd.read_sql_query(query, con, params=(limit,))
            dbg(f"DB: Found {len(df)} leads needing enrichment.")
            return df
    except Exception as e:
        dbg(f"DB ERR: Could not get leads for enrichment: {e}")
        return pd.DataFrame()

# --- NEW FUNCTIONS FOR SMART LISTS ---

def add_lead_to_smart_list(db_file, list_name, lead_id, ai_category, ai_justification):
    """Saves an AI-categorized lead to a specific smart list."""
    query = """
    INSERT OR IGNORE INTO smart_lists (list_name, lead_id, ai_category, ai_justification, timestamp)
    VALUES (?, ?, ?, ?, ?)
    """
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    try:
        with sqlite3.connect(db_file) as conn:
            conn.execute(query, (list_name, lead_id, ai_category, ai_justification, now_iso))
            conn.commit()
            return True
    except sqlite3.Error as e:
        dbg(f"[DB Smart List ERR] Failed to add lead {lead_id} to list '{list_name}': {e}")
        return False

def get_smart_list_names(db_file):
    """Retrieves a list of all unique smart list names that have been created."""
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT list_name FROM smart_lists ORDER BY list_name")
            return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        dbg(f"[DB Smart List ERR] Could not retrieve list names: {e}")
        return []

def get_leads_for_smart_list(db_file, list_name):
    """Retrieves all leads belonging to a specific smart list, joining with the main leads table."""
    query = """
    SELECT
        l.id, l.name, l.website, l.phone, l.email, l.address,
        sl.ai_category,
        sl.ai_justification
    FROM leads l
    JOIN smart_lists sl ON l.id = sl.lead_id
    WHERE sl.list_name = ?
    """
    try:
        with sqlite3.connect(db_file) as conn:
            df = pd.read_sql_query(query, conn, params=(list_name,))
            return df
    except sqlite3.Error as e:
        dbg(f"[DB Smart List ERR] Could not retrieve leads for list '{list_name}': {e}")
        return pd.DataFrame()

def get_analyzed_lead_ids_for_list(db_file, list_name):
    """Gets all lead IDs that have already been analyzed for a given smart list."""
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT lead_id FROM smart_lists WHERE list_name = ?", (list_name,))
            return {row[0] for row in cursor.fetchall()}
    except sqlite3.Error as e:
        dbg(f"[DB Smart List ERR] Could not retrieve analyzed IDs for '{list_name}': {e}")
        return set()

def export_leads_to_file(db_file, output_filepath, output_format, **filters):
    """
    Exports leads from the database to a specified file format (CSV or Excel).
    Filters can be applied to select specific leads.
    """
    try:
        with sqlite3.connect(db_file) as con:
            where_sql, params = _get_db_where_clauses(**filters)
            query = f"""
            SELECT l.*, ar.identified_needs, ar.outreach_strategy
            FROM leads l
            LEFT JOIN advanced_lead_reports ar ON l.id = ar.lead_id
            {where_sql}
            ORDER BY l.id DESC
            """
            df = pd.read_sql_query(query, con, params=params)

            if output_format == "csv":
                df.to_csv(output_filepath, index=False)
                dbg(f"Export: Successfully exported {len(df)} leads to CSV: {output_filepath}")
            elif output_format == "excel":
                df.to_excel(output_filepath, index=False, engine='openpyxl')
                dbg(f"Export: Successfully exported {len(df)} leads to Excel: {output_filepath}")
            else:
                dbg(f"Export ERR: Unsupported output format: {output_format}")
                return False
        return True
    except sqlite3.Error as e:
        dbg(f"Export ERR: Database error during export: {e}")
        return False
    except Exception as e:
        dbg(f"Export ERR: An unexpected error occurred during export: {e}")
        return False