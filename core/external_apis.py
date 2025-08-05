# core/external_apis.py
"""
Manages all external API calls (Ollama, Google, Hunter, etc.).
"""
import requests
import json
import re
import time
from urllib.parse import quote_plus

# Core module imports
from .logging import dbg
from .utils import log_api_call

# --- OLLAMA AI INTEGRATION ---
def get_ollama_models(base_url: str) -> list[str]:
    """Fetches the list of all locally installed models from the Ollama server."""
    try:
        response = requests.get(f"{base_url}/api/tags")
        response.raise_for_status()
        models_data = response.json().get("models", [])
        return sorted([model['name'] for model in models_data])
    except requests.exceptions.RequestException as e:
        dbg(f"Could not connect to Ollama server at {base_url}. Is it running? Error: {e}")
        return []

def call_ollama_model(base_url, model_name, prompt, task_type="reasoning", expect_json=False, timeout=120):
    """Generic function to call a model on the Ollama server."""
    api_url = f"{base_url}/api/generate"
    payload = {"model": model_name, "prompt": prompt, "stream": False, "format": "json" if expect_json else None}
    headers = {"Content-Type": "application/json"}
    dbg(f"Ollama Call: Model: {model_name}, Expect JSON: {expect_json}, Prompt: {prompt[:100]}...")
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=timeout)
        response.raise_for_status()
        raw_text = response.text
        response_data = json.loads(raw_text)
        content = response_data.get("response", "").strip()
        if expect_json:
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                dbg(f"Ollama WARN: Expected JSON but got text. Content: {content}")
                return content
        else:
            return content
    except requests.exceptions.RequestException as e:
        dbg(f"Ollama ERR: Request failed for model {model_name}: {e}")
        return None
    except json.JSONDecodeError as e:
        dbg(f"Ollama ERR: Failed to parse Ollama response: {e}. Raw: {response.text[:200]}")
        return None

# --- GOOGLE APIs ---
def g_cse(api_key, cx_id, query, start=1, num_results=10, api_log_file=""):
    """Performs a Google Custom Search Engine query."""
    if not api_key or not cx_id:
        dbg("CSE Skip: Key/CX missing.")
        return []
    cost_per_g_cse_call = (5.0 / 1000.0)
    if api_log_file:
        log_api_call(api_log_file, "google_cse", cost_per_g_cse_call, query)
    encoded_query = quote_plus(query)
    url = f"https://www.googleapis.com/customsearch/v1?key={api_key}&cx={cx_id}&q={encoded_query}&start={start}&num={min(num_results, 10)}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json().get("items", [])
    except requests.exceptions.RequestException as e:
        dbg(f"CSE ERR: Query '{query}': {e}")
        return []

def pagespeed(api_key, domain, api_log_file=""):
    """Fetches PageSpeed Insights score for a domain."""
    if not api_key: dbg("PSI Skip: Key missing."); return None
    if not domain: dbg("PSI Skip: No domain."); return None
    if api_log_file:
        log_api_call(api_log_file, "pagespeed_insights", 0.0, domain)
    target_url = f"https://{domain}" if not domain.startswith(('http://', 'https://')) else domain
    api_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = {"url": target_url, "key": api_key, "category": "performance"}
    try:
        r = requests.get(api_url, params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            dbg(f"PSI API ERR: {domain} -> {data['error'].get('message')}")
            return None
        score = data.get("lighthouseResult", {}).get("categories", {}).get("performance", {}).get("score")
        return int(score * 100) if score is not None else None
    except requests.exceptions.RequestException as e:
        dbg(f"PSI ERR: {domain}: {e}")
        return None

# --- FIXED: THIS FUNCTION IS NOW MORE RESILIENT AND HAS BETTER DEBUGGING ---
def geocode_location(api_key, location_name, user_agent, api_log_file="", price=0.005):
    """
    Geocodes a location name to lat/lng.
    PRIORITY 1: Tries the free Nominatim (OpenStreetMap) service first.
    PRIORITY 2: Falls back to the paid Google Geocoding API if Nominatim fails.
    """
    if not location_name: return None

    # --- 1. Try free Nominatim service first ---
    dbg(f"[Geocode] Attempting to geocode '{location_name}' using FREE Nominatim service...")
    nominatim_url = "https://nominatim.openstreetmap.org/search"
    nominatim_params = {'q': location_name, 'format': 'json', 'limit': 1}
    headers = {'User-Agent': user_agent}
    try:
        response = requests.get(nominatim_url, params=nominatim_params, headers=headers, timeout=10)
        # Add detailed error logging
        if response.status_code != 200:
            dbg(f"[Geocode Nominatim WARN] Received non-200 status: {response.status_code}. Response: {response.text[:200]}")
            response.raise_for_status()
        
        data = response.json()
        if data:
            lat, lng = float(data[0]['lat']), float(data[0]['lng'])
            dbg(f"  -> Success with Nominatim: ({lat}, {lng})")
            return lat, lng
        else:
            # This case handles an empty but successful response
            dbg(f"[Geocode Nominatim WARN] Service returned an empty result list.")

    except requests.exceptions.RequestException as e:
        dbg(f"[Geocode Nominatim CRITICAL ERR] Request failed: {e}. Will try Google API.")
    except (KeyError, IndexError):
        dbg(f"[Geocode Nominatim WARN] No results found in response. Will try Google API.")

    # --- 2. Fallback to paid Google Geocoding API ---
    if not api_key:
        dbg("Geocode ERR: Nominatim failed and Google API Key is missing for fallback.")
        return None
        
    dbg(f"[Geocode] Nominatim failed. Falling back to PAID Google Geocoding API for '{location_name}'...")
    if api_log_file:
        log_api_call(api_log_file, "google_geocoding", price, location_name)
    
    google_url = "https://maps.googleapis.com/maps/api/geocode/json"
    google_params = {'address': location_name, 'key': api_key}
    
    try:
        response = requests.get(google_url, params=google_params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('status') == 'OK' and data.get('results'):
            loc = data['results'][0]['geometry']['location']
            dbg(f"  -> Success with Google: ({loc['lat']}, {loc['lng']})")
            return loc['lat'], loc['lng']
        else:
            dbg(f"Geocode Google ERR: Status: {data.get('status')}")
            return None
    except requests.exceptions.RequestException as e:
        dbg(f"Geocode Google ERR: Request failed for '{location_name}': {e}")
        return None

# --- OTHER 3RD PARTY APIs ---
def hunter_email(api_key, full_name, domain=None, api_log_file="", price=0.01):
    """Finds an email address using the Hunter.io API."""
    if not api_key: dbg("Hunter Skip: No API Key."); return None
    if not full_name: dbg("Hunter Skip: No name."); return None
    if api_log_file:
        log_api_call(api_log_file, "hunter_io", price, f"{full_name}@{domain or '?'}")
    api_url = "https://api.hunter.io/v2/email-finder"
    params = {"full_name": full_name, "api_key": api_key}
    if domain: params["domain"] = domain
    try:
        res = requests.get(api_url, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()
        return data.get("data", {}).get("email")
    except requests.exceptions.RequestException as e:
        dbg(f"Hunter ERR: for '{full_name}': {e}")
        return None

def public_emails(gcp_config, domain):
    """Finds public emails on a domain using Google Search."""
    if not domain: return None
    emails_found = set()
    queries = [f"'@{domain}' contact", f"'@{domain}' email", f"site:{domain} contact"]
    try:
        for q in queries:
            cse_results = g_cse(gcp_config['api_key'], gcp_config['cx_id'], q, api_log_file=gcp_config.get('api_log_file', ''))
            for item in cse_results:
                text_to_search = f"{item.get('title','')} {item.get('snippet','')}"
                found = re.findall(r'\b[A-Za-z0-9._%+-]+@' + re.escape(domain) + r'\b', text_to_search, re.I)
                emails_found.update(f.lower() for f in found)
            time.sleep(0.2)
        return "; ".join(sorted(list(emails_found))[:5]) or None
    except Exception as e:
        dbg(f"Pub Email ERR for {domain}: {e}")
        return None