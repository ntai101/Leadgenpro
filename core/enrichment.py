# core/enrichment.py
"""
Contains all data enrichment logic, from simple rule-based functions
to the advanced AI agent that uses Selenium and LLMs for deep analysis.
"""
import time
import json
import os
import pandas as pd
from urllib.parse import urlparse
import re

# Core module imports
from .external_apis import pagespeed, public_emails, call_ollama_model, g_cse
from .database import save_enriched, get_lead_by_id, unenriched, save_advanced_report, update_lead_in_db
from .ai_prompts import (
    get_prompt_for_outreach_strategy,
    get_company_report_prompt,
    get_prompt_for_contact_extraction,
    get_prompt_for_website_validation,
    get_prompt_for_file_report
)
from .utils import dbg
from .harvesters import harvest_places

# Gracefully handle missing automation libraries
try:
    from .agent_tools import BrowserAutomation, OCRService
    AUTOMATION_AVAILABLE = True
except ImportError:
    dbg("Warning: BrowserAutomation or OCRService not available. AI Agent will be limited.")
    BrowserAutomation = None
    OCRService = None
    AUTOMATION_AVAILABLE = False


def run_basic_enrichment(db_file, config):
    """
    Finds and enriches leads with non-AI data like PageSpeed and public emails.
    """
    dbg("Starting basic enrichment process...")
    leads_to_enrich = unenriched(db_file)
    if leads_to_enrich.empty:
        dbg("No new leads to enrich at this time.")
        return 0, 0

    success_count = 0
    gcp_config = {'api_key': config.GCP_API_KEY, 'cx_id': config.GCP_CX, 'api_log_file': config.API_USAGE_LOG_FILE}

    for _, row in leads_to_enrich.iterrows():
        enriched_data = {}
        psi_score = pagespeed(gcp_config['api_key'], row['domain'], gcp_config['api_log_file'])
        if psi_score is not None: enriched_data['psi'] = psi_score

        emails = public_emails(gcp_config, row['domain'])
        if emails: enriched_data['public_emails'] = emails

        if enriched_data:
            save_enriched(db_file, row['id'], enriched_data)
            success_count += 1
        time.sleep(0.5)

    dbg(f"Basic enrichment complete. Processed {success_count} leads.")
    return success_count, 0

def run_manual_enrichment(tool_choice: str, user_input, config):
    """
    Acts as a router to run the specific enrichment tool selected by the user.
    """
    dbg(f"Running manual enrichment with tool: '{tool_choice}'")

    if tool_choice == "Browser Automation Report":
        if not AUTOMATION_AVAILABLE:
            return "## Error\nBrowser automation is not available. Please check your Selenium/ChromeDriver setup."
        if not user_input:
            return "## Input Required\nPlease provide a company name or website."
        return generate_company_report(user_input, config)

    elif tool_choice == "Google Places Search":
        if not user_input:
            return "## Input Required\nPlease provide a search query (e.g., 'plumbers in toronto')."
        hits = harvest_places(
            places_api_key=config.PLACES_API_KEY, 
            keyword=user_input, 
            location="", 
            db_path=config.DB_FILE, 
            api_log_file=config.API_USAGE_LOG_FILE
        )
        if not hits:
            return f"No new results found from Google Places for '{user_input}'. Existing leads were skipped."
        report = f"## Google Places Results for '{user_input}'\n\nFound {len(hits)} new potential leads:\n\n"
        for i, hit in enumerate(hits, 1):
            report += f"**{i}. {hit.get('name', 'N/A')}**\n- **Address:** {hit.get('address', 'N/A')}\n- **Phone:** {hit.get('phone', 'N/A')}\n- **Website:** {hit.get('website', 'N/A')}\n\n"
        return report

    elif tool_choice == "OCR from Image":
        if not user_input:
            return "## Input Required\nPlease upload an image file."
        ocr_service = OCRService()
        extracted_text = ocr_service.extract_text_from_image(user_input)
        return f"## OCR Extraction Results\n\n---\n\n{extracted_text}"

    else:
        return f"Error: Invalid tool selected ('{tool_choice}')."

# --- AI Data Enrichment Agent ---

def enrich_lead_with_ai_agent(lead_id, db_file, config):
    """
    Performs a deep, AI-driven analysis on a single lead and saves a report file.
    """
    if not AUTOMATION_AVAILABLE:
        raise ImportError("BrowserAutomation is not available. Cannot run AI agent.")
    lead_data = get_lead_by_id(db_file, lead_id)
    if not lead_data:
        return False, "Lead data not found"
    website_url = lead_data.get('website')
    if not website_url:
        return False, "No website URL for this lead"

    dbg(f"Starting deep AI analysis for {lead_data['name']} (ID: {lead_id})")
    with BrowserAutomation() as browser:
        if not browser.navigate_to_url(website_url):
            return False, "Failed to navigate to website"
        website_text = browser.get_full_page_text()
        screenshot_dir = os.path.join(os.path.dirname(db_file), "..", "user_data", "screenshots")
        screenshot_path = browser.screenshot(os.path.join(screenshot_dir, f"lead_{lead_id}_{int(time.time())}.png"))

    prompt = get_prompt_for_file_report(lead_data, website_text, config.TMC_MEDIA_PROFILE)
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
    markdown_report = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=False)

    if not markdown_report or not isinstance(markdown_report, str):
        return False, "LLM failed to return a valid report string."

    reports_dir = getattr(config, 'REPORTS_SAVE_PATH', os.path.join(config.project_root, 'user_data', 'reports'))
    os.makedirs(reports_dir, exist_ok=True)
    safe_lead_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', lead_data['name'])
    report_filename = f"report_{lead_id}_{safe_lead_name}.md"
    report_file_path = os.path.join(reports_dir, report_filename)
    try:
        with open(report_file_path, "w", encoding="utf-8") as f:
            f.write(markdown_report)
        dbg(f"Successfully saved report to {report_file_path}")
    except IOError as e:
        dbg(f"Failed to write report file: {e}")
        return False, f"Could not write report to file: {e}"

    report_data = {
        'lead_id': lead_id, 'website_analysis_notes': website_text[:2000],
        'screenshot_path': screenshot_path, 'report_file_path': report_file_path,
        'identified_needs': [], 'outreach_strategy': [], 'critical_missing_info': "See report file for details."
    }
    return (True, "Success") if save_advanced_report(db_file, report_data) else (False, "Failed to save report to database")

def enrich_leads_with_ai_agent_batch(db_file, config, lead_ids):
    """
    Fetches a batch of leads and runs the deep enrichment agent on them.
    """
    if not AUTOMATION_AVAILABLE: return 0, len(lead_ids)
    dbg(f"Starting AI agent batch for {len(lead_ids)} leads.")
    success_count, failure_count = 0, 0
    for lead_id in lead_ids:
        try:
            success, reason = enrich_lead_with_ai_agent(lead_id, db_file, config)
            if success: success_count += 1
            else: failure_count += 1; dbg(f"AI enrichment failed for lead ID {lead_id}: {reason}")
            time.sleep(1)
        except Exception as e:
            failure_count += 1
            dbg(f"A critical error occurred while processing lead ID {lead_id}: {e}")
    dbg(f"AI batch process finished. Success: {success_count}, Failure: {failure_count}")
    return success_count, failure_count

def generate_company_report(query: str, config):
    """
    Generates a full research report for a company based on a name or website.
    """
    if not AUTOMATION_AVAILABLE: return "Error: Browser Automation tools are not available."
    website = query if "http" in query or "www." in query else None
    gcp_config = {'api_key': config.GCP_API_KEY, 'cx_id': config.GCP_CX}
    if not website:
        search_results = g_cse(gcp_config['api_key'], gcp_config['cx_id'], f'official website for {query}')
        if search_results and 'link' in search_results[0]: website = search_results[0]['link']
        else: return f"## Report Failed\n\nCould not find a website for: **'{query}'**."
    with BrowserAutomation() as browser:
        if not browser.driver: return "## Report Failed\n\nError: WebDriver could not be initialized."
        analysis_data = browser.analyze_site_deep(website)
    if analysis_data.get("error"): return f"## Report Failed\n\nError during site analysis: {analysis_data['error']}"
    prompt = get_company_report_prompt(query, analysis_data, config.TMC_MEDIA_PROFILE)
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
    report = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt)
    return report or "## Report Failed\n\nThe AI model did not return a valid report."

def fill_missing_data_for_leads(db_file: str, lead_ids: list, config):
    """
    A comprehensive AI agent workflow that finds and fills missing websites and contact information for leads using Google APIs.
    """
    if not AUTOMATION_AVAILABLE:
        dbg("Missing data enrichment skipped: BrowserAutomation not available.")
        return 0
    dbg(f"Starting comprehensive 'Find & Fill' (Google API) for {len(lead_ids)} leads.")
    updated_leads_count = 0
    gcp_config = {'api_key': config.GCP_API_KEY, 'cx_id': config.GCP_CX}
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
    with BrowserAutomation() as browser:
        if not browser.driver:
            dbg("Browser could not be initialized. Aborting.")
            return 0
        for lead_id in lead_ids:
            is_lead_updated = False
            lead_data = get_lead_by_id(db_file, lead_id)
            if not lead_data: continue
            website_url = lead_data.get('website')
            if not website_url:
                dbg(f"Lead ID {lead_id} is missing a website. Searching with Google API...")
                query = f"official website for \"{lead_data['name']}\" at \"{lead_data.get('address', '')}\""
                search_results = g_cse(gcp_config['api_key'], gcp_config['cx_id'], query)
                if search_results:
                    top_result = search_results[0]
                    prompt = get_prompt_for_website_validation(lead_data, top_result)
                    validation = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
                    if isinstance(validation, dict) and validation.get("is_correct_website"):
                        website_url = top_result.get('link')
                        dbg(f"  -> AI validated new website: {website_url}")
                        update_lead_in_db(db_file, lead_id, "website", website_url)
                        try:
                            domain = urlparse(website_url).netloc.replace("www.", "")
                            update_lead_in_db(db_file, lead_id, "domain", domain)
                        except: pass
                        is_lead_updated = True
            if website_url:
                lead_data = get_lead_by_id(db_file, lead_id)
                if not lead_data.get('phone') or not lead_data.get('email') or not lead_data.get('address'):
                    dbg(f"Searching for contact info on {website_url}...")
                    browser.navigate_to_url(website_url)
                    browser.find_and_click_link(["contact", "connect"])
                    contact_text = browser.get_full_page_text()
                    if contact_text:
                        prompt = get_prompt_for_contact_extraction(contact_text)
                        extracted_info = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
                        if isinstance(extracted_info, dict):
                            for field in ["phone", "email", "address"]:
                                if not lead_data.get(field) and extracted_info.get(field):
                                    new_value = extracted_info[field]
                                    dbg(f"  -> Found new {field}: {new_value}")
                                    update_lead_in_db(db_file, lead_id, field, new_value)
                                    is_lead_updated = True
            if is_lead_updated:
                updated_leads_count += 1
            time.sleep(1)
    dbg(f"'Find & Fill' process complete. Updated {updated_leads_count} lead(s).")
    return updated_leads_count

def find_missing_websites_for_leads(db_file: str, lead_ids: list, config):
    """
    Uses Google Search and an LLM to find and validate missing websites for a list of leads.
    """
    dbg(f"Starting to find missing websites for {len(lead_ids)} leads.")
    updated_leads_count = 0
    gcp_config = {'api_key': config.GCP_API_KEY, 'cx_id': config.GCP_CX}
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
    for lead_id in lead_ids:
        lead_data = get_lead_by_id(db_file, lead_id)
        if not lead_data or lead_data.get('website'): continue
        query = f"official website for \"{lead_data['name']}\" at \"{lead_data.get('address', '')}\""
        search_results = g_cse(gcp_config['api_key'], gcp_config['cx_id'], query)
        if not search_results: continue
        top_result = search_results[0]
        prompt = get_prompt_for_website_validation(lead_data, top_result)
        validation_response = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
        if isinstance(validation_response, dict) and validation_response.get("is_correct_website") is True:
            found_url = top_result.get('link')
            if found_url:
                update_lead_in_db(db_file, lead_id, "website", found_url)
                try:
                    domain = urlparse(found_url).netloc.replace("www.", "")
                    update_lead_in_db(db_file, lead_id, "domain", domain)
                except Exception: pass
                updated_leads_count += 1
        time.sleep(1)
    dbg(f"Missing website search complete. Updated {updated_leads_count} lead(s).")
    return updated_leads_count
    
def find_missing_websites_with_selenium(db_file: str, lead_ids: list, config):
    """
    Uses BrowserAutomation (Selenium) and an LLM to find and validate missing websites for leads.
    This version does NOT require a Google Search API key.
    """
    if not AUTOMATION_AVAILABLE:
        dbg("Missing website search skipped: BrowserAutomation not available.")
        return 0
    dbg(f"Starting Selenium-based website search for {len(lead_ids)} leads.")
    updated_leads_count = 0
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}
    with BrowserAutomation() as browser:
        if not browser.driver:
            dbg("Browser could not be initialized. Aborting.")
            return 0
        for lead_id in lead_ids:
            lead_data = get_lead_by_id(db_file, lead_id)
            if not lead_data or lead_data.get('website'):
                continue
            search_queries = [
                f"\"{lead_data['name']}\" \"{lead_data.get('address', '')}\" official website",
                f"\"{lead_data['name']}\" official website"
            ]
            found_url = None
            for query in search_queries:
                if found_url: break 
                dbg(f"  -> Searching for lead ID {lead_id} with query: '{query}'")
                search_results = browser.search_and_scrape_results(query, num_results=5)
                if not search_results:
                    continue
                for result in search_results:
                    prompt = get_prompt_for_website_validation(lead_data, result)
                    validation = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
                    if isinstance(validation, dict) and validation.get("is_correct_website") is True:
                        url_to_update = result.get('link')
                        if url_to_update:
                            dbg(f"  -> AI validated new website: {url_to_update}")
                            update_lead_in_db(db_file, lead_id, "website", url_to_update)
                            try:
                                domain = urlparse(url_to_update).netloc.replace("www.", "")
                                update_lead_in_db(db_file, lead_id, "domain", domain)
                            except: pass
                            updated_leads_count += 1
                            found_url = url_to_update
                            break
                    time.sleep(1)
            if not found_url:
                dbg(f"  -> Could not validate a website for lead ID {lead_id} after all attempts.")
    dbg(f"Selenium website search complete. Updated {updated_leads_count} lead(s).")
    return updated_leads_count

# --- NEW FUNCTION: Added to the end of the file ---
def find_and_fill_with_selenium(db_file: str, lead_ids: list, config):
    """
    A comprehensive AI agent workflow that uses Selenium and an LLM to find and
    fill missing websites, phone numbers, addresses, and emails for a list of leads.
    This is a cost-effective alternative to using paid APIs for everything.
    """
    if not AUTOMATION_AVAILABLE:
        dbg("Missing data enrichment skipped: BrowserAutomation not available.")
        return 0, len(lead_ids)

    dbg(f"Starting Selenium-based 'Find & Fill' for {len(lead_ids)} leads.")
    updated_leads_count = 0
    ollama_cfg = {"base_url": config.OLLAMA_BASE_URL, "reasoning_model": config.OLLAMA_REASONING_MODEL}

    with BrowserAutomation() as browser:
        if not browser.driver:
            dbg("Browser could not be initialized. Aborting.")
            return 0, len(lead_ids)

        for lead_id in lead_ids:
            is_lead_updated = False
            lead_data = get_lead_by_id(db_file, lead_id)
            if not lead_data: continue

            website_url = lead_data.get('website')

            # --- Step 1: Find a missing website ---
            if not website_url:
                dbg(f"Lead ID {lead_id} ({lead_data['name']}) is missing a website. Searching with Selenium...")
                search_queries = [
                    f"\"{lead_data['name']}\" \"{lead_data.get('address', '')}\" official website",
                    f"\"{lead_data['name']}\" official website"
                ]
                
                for query in search_queries:
                    if website_url: break # Exit loop if we've found a valid URL
                    
                    dbg(f"  -> Searching with query: '{query}'")
                    search_results = browser.search_and_scrape_results(query, num_results=3)
                    
                    for result in search_results:
                        prompt = get_prompt_for_website_validation(lead_data, result)
                        validation = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
                        
                        if isinstance(validation, dict) and validation.get("is_correct_website"):
                            found_url = result.get('link')
                            dbg(f"  -> AI validated new website: {found_url}")
                            update_lead_in_db(db_file, lead_id, "website", found_url)
                            try:
                                domain = urlparse(found_url).netloc.replace("www.", "")
                                update_lead_in_db(db_file, lead_id, "domain", domain)
                            except Exception: pass
                            website_url = found_url # Use this new URL for the next step
                            is_lead_updated = True
                            break # Stop searching this query's results
                    time.sleep(1) # Courtesy delay between searches

            # --- Step 2: Extract contact info from the website ---
            if website_url:
                current_lead_data = get_lead_by_id(db_file, lead_id)
                needs_contact_info = not all([current_lead_data.get(k) for k in ['phone', 'email', 'address']])

                if needs_contact_info:
                    dbg(f"Searching for contact info on {website_url}...")
                    if browser.navigate_to_url(website_url):
                        # Try to navigate to a contact page for better results
                        browser.find_and_click_link(["contact", "about", "connect"])
                        contact_page_text = browser.get_full_page_text()

                        if contact_page_text:
                            prompt = get_prompt_for_contact_extraction(contact_page_text)
                            extracted_info = call_ollama_model(ollama_cfg['base_url'], ollama_cfg['reasoning_model'], prompt, expect_json=True)
                            
                            if isinstance(extracted_info, dict):
                                for field in ["phone", "email", "address"]:
                                    if not current_lead_data.get(field) and extracted_info.get(field):
                                        new_value = extracted_info[field]
                                        dbg(f"  -> Found new {field}: {new_value}")
                                        update_lead_in_db(db_file, lead_id, field, new_value)
                                        is_lead_updated = True
            
            if is_lead_updated:
                updated_leads_count += 1
            time.sleep(1) # Courtesy delay between processing each lead

    dbg(f"Selenium 'Find & Fill' process complete. Updated {updated_leads_count} lead(s).")
    return updated_leads_count, len(lead_ids) - updated_leads_count