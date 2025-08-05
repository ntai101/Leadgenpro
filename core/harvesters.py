# core/harvesters.py
"""
Contains all lead harvesting functions that gather raw lead data from various sources.
"""
import re
import time
import requests
import datetime as dt
import pandas as pd

from .utils import dbg, clean_name, log_api_call
from .external_apis import g_cse, geocode_location, hunter_email
from .database import check_lead_exists

# --- LinkedIn Harvester (via Google Search) ---
def harvest_linkedin(gcp_config, hunter_key, ollama_config, query, pages, mode):
    hits = []
    path = "/in/" if mode == "linkedin_person" else "/company/"
    dbg(f"[Harvest LinkedIn] q='{query}', pages={pages}, mode={mode}")

    for s in range(1, pages * 10, 10):
        search_query = f"site:linkedin.com {query}"
        cse_results = g_cse(gcp_config['api_key'], gcp_config['cx_id'], search_query, start=s)
        if not cse_results:
            break

        for item in cse_results:
            link = item.get("link")
            title_raw = item.get("title")
            snippet = item.get("snippet", "")
            
            if not link or any(k in link for k in ["/jobs/", "/showcase/", "/posts/"]):
                continue

            hit = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "source": mode}
            
            if "/company/" in link:
                hit["record_type"] = "business"
                hit["name"] = title_raw.split(" | ")[0].strip() if title_raw else "Unknown Company"
                web_match = re.search(r'https?://(?:www\.)?([\w-]+\.\w+)', snippet)
                if web_match:
                    hit["website"] = web_match.group(1)
                    hit["domain"] = web_match.group(1).split("//")[-1].split("/")[0].replace("www.","")
            elif "/in/" in link:
                hit["record_type"] = "person"
                hit["name"] = clean_name(title_raw, ollama_config)
                hit["title"] = snippet
                domain_match = re.search(r'([\w-]+\.(?:com|org|net|io|ca|co\.uk))\b', snippet, re.I)
                domain = domain_match.group(1).lower() if domain_match else None
                if hit["name"] and domain:
                    hit["email"] = hunter_email(hunter_key, hit["name"], domain, gcp_config['api_log_file'])
                    if hit.get("email"):
                         hit["domain"] = domain
            else:
                continue
            
            hit["linkedin"] = link
            hits.append(hit)
        time.sleep(0.5)
    dbg(f"[Harvest LinkedIn] Found {len(hits)} hits.")
    return hits

# --- Google Places Harvester (with Pre-emptive Duplicate Checking) ---
def harvest_places(places_api_key, keyword, location, db_path, api_log_file="", result_limit=20):
    if not places_api_key:
        dbg("[Places Skip] API Key missing.")
        return []
        
    rows, skipped_count = [], 0
    query = f"{keyword} in {location}".strip()
    
    api_limit = 20 if result_limit > 20 else result_limit
    dbg(f"[Places] Searching for {api_limit} results: '{query}'")

    search_url = "https://places.googleapis.com/v1/places:searchText"
    field_mask = "places.id,places.displayName,places.websiteUri,places.internationalPhoneNumber,places.businessStatus,places.types,places.location,places.formattedAddress"
    headers = { "Content-Type": "application/json", "X-Goog-Api-Key": places_api_key, "X-Goog-FieldMask": field_mask }
    data = { "textQuery": query, "maxResultCount": api_limit }

    log_api_call(api_log_file, "google_places_searchText", 0.035, query)
    
    try:
        res = requests.post(search_url, json=data, headers=headers, timeout=20)
        res.raise_for_status()
        results = res.json().get("places", [])
    except requests.exceptions.RequestException as e:
        dbg(f"[Places] API call failed: {e}"); return []

    for place in results:
        place_name = place.get("displayName", {}).get("text")
        place_address = place.get("formattedAddress")

        if check_lead_exists(db_path, place_name, place_address):
            skipped_count += 1
            continue
        if place.get("businessStatus") != "OPERATIONAL": continue
        
        website = place.get("websiteUri")
        domain = website.split("//")[-1].split("/")[0].replace("www.", "") if website else None
        
        rows.append({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "record_type": "business",
            "source": "places_search_new", "name": place_name,
            "title": ", ".join(place.get("types", [])[:3]), "website": website,
            "phone": place.get("internationalPhoneNumber"), "domain": domain,
            "lat": place.get("location", {}).get("latitude"), "lng": place.get("location", {}).get("longitude"),
            "address": place_address, "business_type": (place.get("types", []) or [None])[0]
        })
        time.sleep(0.05)
        
    dbg(f"[Places] Processed {len(rows)} new rows. Skipped {skipped_count} pre-existing leads.")
    return rows

# --- FIXED: RE-ADDED THIS FUNCTION ---
def harvest_openstreetmap(osm_config, keywords, location, db_path, result_limit=50):
    """
    Performs a standard search using the Nominatim API for the sidebar harvester.
    """
    user_agent = osm_config.get('user_agent')
    if not user_agent:
        dbg("[OSM Skip] User-Agent is missing from config.")
        return []

    rows, skipped_count = [], 0
    # The keywords for this function are expected as a list
    query = f"{' '.join(keywords)} in {location}"
    dbg(f"[OSM] Searching Nominatim for '{query}' with a limit of {result_limit} results.")

    nominatim_url = "https://nominatim.openstreetmap.org/search"
    params = { 'q': query, 'format': 'json', 'addressdetails': 1, 'limit': result_limit }
    headers = { 'User-Agent': user_agent }

    try:
        response = requests.get(nominatim_url, params=params, headers=headers, timeout=20)
        response.raise_for_status()
        results = response.json()
    except requests.exceptions.RequestException as e:
        dbg(f"[OSM] API call failed: {e}")
        return []

    for place in results:
        place_name = place.get('display_name', '').split(',')[0]
        place_address = place.get('display_name')
        
        if check_lead_exists(db_path, place_name, place_address):
            skipped_count += 1
            continue
            
        if place.get('category') not in ['amenity', 'shop', 'office', 'tourism']:
            continue

        rows.append({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "record_type": "business",
            "source": "openstreetmap", "name": place_name,
            "title": f"{place.get('type', 'N/A')} ({place.get('category')})", "website": None,
            "phone": None, "domain": None, "lat": place.get('lat'), "lng": place.get('lon'),
            "address": place_address, "business_type": place.get('type')
        })
        time.sleep(1)

    dbg(f"[OSM Harvest] Found {len(rows)} new hits. Skipped {skipped_count} pre-existing leads.")
    return rows

# --- Bulk OpenStreetMap Harvester ---
def harvest_openstreetmap_bulk(osm_config, keywords, area_name):
    """
    Performs a bulk download of features from OpenStreetMap using the Overpass API.
    """
    user_agent = osm_config.get('user_agent')
    if not user_agent:
        dbg("[OSM Bulk Skip] User-Agent is missing from config.")
        return pd.DataFrame()

    overpass_url = "https://overpass-api.de/api/interpreter"
    
    query_parts = []
    for keyword in keywords.split(','):
        keyword = keyword.strip()
        if not keyword: continue
        
        if '=' in keyword:
            k, v = keyword.split('=', 1)
            query_parts.append(f'node["{k}"="{v}"](area.searchArea); way["{k}"="{v}"](area.searchArea);')
        else:
            query_parts.append(f'node["amenity"="{keyword}"](area.searchArea); way["amenity"="{keyword}"](area.searchArea);')

    overpass_query = f"""
    [out:json][timeout:180];
    area[name~"{area_name}",i]->.searchArea;
    (
      {' '.join(query_parts)}
    );
    out center;
    """
    dbg(f"[OSM Bulk] Sending Overpass query for '{keywords}' in '{area_name}'")
    
    try:
        response = requests.post(overpass_url, data={"data": overpass_query}, headers={'User-Agent': user_agent}, timeout=190)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        dbg(f"[OSM Bulk Error] Overpass API call failed: {e}")
        return pd.DataFrame()

    results = []
    for element in data.get('elements', []):
        tags = element.get('tags', {})
        if not tags.get('name'): continue

        lat = element.get('lat') or element.get('center', {}).get('lat')
        lon = element.get('lon') or element.get('center', {}).get('lon')
        
        addr_parts = [
            tags.get('addr:housenumber'), tags.get('addr:street'), tags.get('addr:city'),
            tags.get('addr:province') or tags.get('addr:state'), tags.get('addr:postcode')
        ]
        address = ', '.join(part for part in addr_parts if part)

        results.append({
            'name': tags.get('name'),
            'business_type': next((tags.get(k) for k in ['amenity', 'shop', 'craft', 'office', 'tourism'] if k in tags), 'unknown'),
            'lat': lat, 'lng': lon, 'address': address or "Address not available",
            'phone': tags.get('phone') or tags.get('contact:phone'),
            'website': tags.get('website') or tags.get('contact:website'),
            'osm_id': element.get('id'), 'type': element.get('type'),
        })

    dbg(f"[OSM Bulk] Processed {len(results)} features from Overpass.")
    return pd.DataFrame(results)

# --- Nearby (Map-Based) Harvesters ---

def harvest_places_nearby(places_api_key, keyword, center_lat, center_lng, radius_km, db_path, api_log_file=""):
    """
    Finds businesses using the Google Places API within a specific radius of a central point.
    """
    if not all([places_api_key, keyword, center_lat, center_lng, radius_km, db_path]):
        dbg("[Places Nearby Skip] Missing one or more required parameters.")
        return []

    rows, skipped_count = [], 0
    radius_meters = float(radius_km) * 1000
    dbg(f"[Places Nearby] Searching for '{keyword}' within {radius_km}km of ({center_lat}, {center_lng}).")
    search_url = "https://places.googleapis.com/v1/places:searchText"
    field_mask = "places.id,places.displayName,places.websiteUri,places.internationalPhoneNumber,places.businessStatus,places.types,places.location,places.formattedAddress"
    headers = { "Content-Type": "application/json", "X-Goog-Api-Key": places_api_key, "X-Goog-FieldMask": field_mask }
    data = {
        "textQuery": keyword,
        "locationRestriction": {"circle": {"center": {"latitude": center_lat, "longitude": center_lng}, "radius": radius_meters}},
        "maxResultCount": 20
    }
    log_api_call(api_log_file, "google_places_searchText_nearby", 0.035, f"{keyword} near {center_lat},{center_lng}")
    try:
        res = requests.post(search_url, json=data, headers=headers, timeout=20)
        res.raise_for_status()
        results = res.json().get("places", [])
    except requests.exceptions.RequestException as e:
        dbg(f"[Places Nearby ERR] API call failed: {e}"); return []

    for place in results:
        place_name = place.get("displayName", {}).get("text")
        place_address = place.get("formattedAddress")
        if check_lead_exists(db_path, place_name, place_address):
            skipped_count += 1
            continue
        if place.get("businessStatus") != "OPERATIONAL": continue
        website = place.get("websiteUri")
        domain = website.split("//")[-1].split("/")[0].replace("www.", "") if website else None
        rows.append({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "record_type": "business",
            "source": "places_nearby", "name": place_name, "title": ", ".join(place.get("types", [])[:3]),
            "website": website, "phone": place.get("internationalPhoneNumber"), "domain": domain,
            "lat": place.get("location", {}).get("latitude"), "lng": place.get("location", {}).get("longitude"),
            "address": place_address, "business_type": (place.get("types", []) or [None])[0]
        })
    dbg(f"[Places Nearby] Processed {len(rows)} new rows. Skipped {skipped_count} pre-existing leads.")
    return rows

def harvest_osm_nearby(osm_config, keywords, center_lat, center_lng, radius_km, db_path):
    """
    Finds features using the Overpass API within a specific radius of a central point.
    """
    user_agent = osm_config.get('user_agent')
    if not user_agent:
        dbg("[OSM Nearby Skip] User-Agent is missing.")
        return []

    rows, skipped_count = [], 0
    radius_meters = float(radius_km) * 1000
    overpass_url = "https://overpass-api.de/api/interpreter"
    query_parts = []
    for keyword in keywords.split(','):
        keyword = keyword.strip()
        if not keyword: continue
        if '=' in keyword:
            k, v = keyword.split('=', 1)
            query_parts.append(f'node["{k}"="{v}"](around:{radius_meters},{center_lat},{center_lng}); way["{k}"="{v}"](around:{radius_meters},{center_lat},{center_lng});')
        else:
            query_parts.append(f'node["amenity"="{keyword}"](around:{radius_meters},{center_lat},{center_lng}); way["amenity"="{keyword}"](around:{radius_meters},{center_lat},{center_lng});')

    overpass_query = f"""[out:json][timeout:60]; ({' '.join(query_parts)}); out center;"""
    dbg(f"[OSM Nearby] Sending Overpass query for '{keywords}' within {radius_km}km of {center_lat},{center_lng}")
    try:
        response = requests.post(overpass_url, data={"data": overpass_query}, headers={'User-Agent': user_agent}, timeout=70)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        dbg(f"[OSM Nearby ERR] Overpass API call failed: {e}"); return []

    for element in data.get('elements', []):
        tags = element.get('tags', {})
        place_name = tags.get('name')
        if not place_name: continue
        addr_parts = [tags.get('addr:housenumber'), tags.get('addr:street'), tags.get('addr:city')]
        place_address = ', '.join(part for part in addr_parts if part).strip(', ')
        if not place_address: place_address = f"Near {place_name}"
        if check_lead_exists(db_path, place_name, place_address):
            skipped_count += 1
            continue
        lat = element.get('lat') or element.get('center', {}).get('lat')
        lon = element.get('lon') or element.get('center', {}).get('lon')
        rows.append({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "record_type": "business",
            "source": "osm_nearby", "name": place_name,
            "title": next((tags.get(k) for k in ['amenity', 'shop', 'craft', 'office', 'tourism'] if k in tags), 'unknown'),
            "website": tags.get('website') or tags.get('contact:website'), "phone": tags.get('phone') or tags.get('contact:phone'),
            "domain": None, "lat": lat, "lng": lon, "address": place_address,
            "business_type": next((k for k in ['amenity', 'shop', 'craft', 'office', 'tourism'] if k in tags), 'unknown'),
        })
    dbg(f"[OSM Nearby] Processed {len(rows)} new rows. Skipped {skipped_count} pre-existing leads.")
    return rows