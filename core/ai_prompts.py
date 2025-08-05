# core/ai_prompts.py
"""
Centralizes all large, multi-line AI prompt templates used throughout the application.
This makes prompts easier to manage, version, and improve over time.
"""

def get_prompt_for_sql_generation(user_query: str, company_profile: str) -> str:
    """Generates the prompt for the AI DB Query Assistant."""
    # ... (existing function)
    return f"""
You are an expert SQL writer for a company called TMC Media.
Your task is to convert the user's natural language question into a valid SQLite query for a database with one main table: 'leads'.

**Database Schema:**
---
**Table: leads** (columns: id, ts, record_type, source, name, title, linkedin, website, phone, email, domain, lat, lng, address, business_type)
---

**Geographic Context:**
- "GTA" or "Greater Toronto Area" includes Toronto, Mississauga, Brampton, Markham, Vaughan, Richmond Hill, and Oakville.
- When a user asks for a location like the GTA, you must create a query with multiple `OR` conditions on the `address` column for each city.

**Search Strategy:**
- Prioritize the `business_type` and `name` columns for categorizing businesses.
- Use the `address` column for location filtering.
- **Example 1:** "find me restaurants in the GTA" should become:
  `SELECT * FROM leads WHERE (business_type LIKE '%restaurant%' OR name LIKE '%restaurant%') AND (address LIKE '%Toronto%' OR address LIKE '%Mississauga%' OR address LIKE '%Brampton%' OR address LIKE '%Markham%' OR address LIKE '%Vaughan%' OR address LIKE '%Richmond Hill%' OR address LIKE '%Oakville%');`
- **Example 2:** "show me all plumbers" should become:
  `SELECT * FROM leads WHERE business_type LIKE '%plumber%' OR name LIKE '%plumber%';`

**CRITICAL INSTRUCTIONS:**
1.  The 'address' column is a single text field. **DO NOT** use non-existent columns like `city` or `state`.
2.  Your response **MUST** contain ONLY the raw SQL query. Do not include explanations, comments, or markdown like ```sql.

**User's question:** "{user_query}"
**SQL Query:**
"""

def get_prompt_for_web_search_generation(user_query: str, company_profile: str) -> str:
    """Generates the prompt for the AI to create an optimized web search query."""
    # ... (existing function)
    return f"""
You are an expert at crafting Google search queries to find potential clients for a company called TMC Media.

TMC Media's Profile:
{company_profile}

Based on the user's request, generate a single, highly-optimized Google Search query string. The goal is to find businesses or individuals who would likely need TMC Media's services.
Return ONLY the search query string itself, with no preamble or explanation.

User request: "{user_query}"
Optimized Google Search Query:
"""

def get_prompt_for_parsing_search_results(search_result_item: dict, company_profile: str) -> str:
    """Generates a prompt to parse a single web search result into a new lead."""
    # ... (existing function)
    return f"""
You are a data parsing AI working for TMC Media. From the Google Search Result item below, extract potential lead information.

Title: {search_result_item.get('title')}
Link: {search_result_item.get('link')}
Snippet: {search_result_item.get('snippet')}

Analyze the provided data. Respond ONLY with a single, valid JSON object containing the following keys: "name", "website", "title", "address", "business_type".
If a value cannot be reliably inferred, use null.
Your entire response must be only the JSON object.
"""

def get_prompt_for_outreach_strategy(lead_data: dict, website_summary: str, company_profile: str) -> str:
    """Generates the prompt for creating a detailed lead analysis and outreach strategy."""
    # ... (existing function)
    return f"""
You are an AI sales and marketing strategist for TMC Media. Your task is to analyze the following lead and generate a concise but insightful outreach plan.

--- TMC Media Profile (Your Company) ---
{company_profile}

--- Lead Information ---
- Name: {lead_data.get('name', 'N/A')}
- Website: {lead_data.get('website', 'N/A')}
- Scraped Website Analysis: {website_summary[:2000]}

--- Your Task ---
Analyze all the information above. Respond ONLY with a valid JSON object using the following keys:
1. "determined_business_type": (string) Your expert determination of their specific business.
2. "identified_needs": (list of strings) 1-2 key business needs this lead likely has that TMC Media can solve.
3. "outreach_strategy": (list of strings) 2-3 specific, actionable outreach suggestions.
4. "critical_missing_info": (string) The most important piece of missing information for an effective sales approach.

Your entire response must be only the JSON object.
"""

def get_company_report_prompt(query: str, analysis_data: dict, company_profile: str) -> str:
    """Generates a prompt for the AI to synthesize browsed data into a research report."""
    # ... (existing function)
    full_text = ""
    for page, content in analysis_data.get("page_content", {}).items():
        full_text += f"\n--- Content from {page.upper()} page ---\n{content}\n"
    social_links = ", ".join(analysis_data.get("social_links", {}).values()) or "None found"
    return f"""
You are an expert business analyst working for TMC Media. Your task is to generate a detailed company research report based on the user's query and data scraped from the company's website.

**Data Gathered from Website Analysis:**
---
- **Pages Visited:** {', '.join(analysis_data.get("pages_visited", ["N/A"]))}
- **Social Media Links Found:** {social_links}
- **Consolidated Website Text:**
{full_text[:8000]}
---

**Your Task:**
Generate a comprehensive company research report in Markdown format. The report must be well-structured and insightful. Do not include any text before the first heading.

**Report Structure:**
- ## Company Report: [Company Name]
- **Company Overview:** A brief summary of the company, its mission, and what it does.
- **Services/Products:** A bulleted list of their main offerings.
- **Target Audience:** Your best assessment of who their customers are.
- **Online Presence Analysis:** Comment on their website's clarity and social media footprint.
- **Potential Opportunities for TMC Media:** A bulleted list of 2-4 specific, actionable opportunities.
- **Overall Summary & Recommendation:** A concluding paragraph on whether they are a good potential client.

Generate the Markdown report now.
"""

def get_prompt_for_contact_extraction(scraped_text: str) -> str:
    """Generates a prompt to extract structured contact info from scraped website text."""
    # ... (existing function)
    return f"""
You are an expert data extraction AI. Your task is to find and extract contact information from the block of text provided below.

**Scraped Website Text:**
---
{scraped_text[:6000]}
---

**Your Task:**
Analyze the text and respond ONLY with a single, valid JSON object containing the following keys: "phone", "email", and "address".
- If a specific piece of information cannot be found, its value in the JSON object should be null.
- The physical address should be a single, complete string.
- The phone number should be in a standard format.
- The email should be a valid email address.
- Prioritize general contact info (e.g., info@, contact@) over personal emails.

Your entire response must be only the JSON object.
"""

def get_prompt_for_website_validation(lead_data: dict, search_result: dict) -> str:
    """
    Generates a prompt to validate if a found website belongs to a specific lead.
    """
    # ... (existing function)
    return f"""
You are an expert data validation AI. Your task is to determine if the provided Web Search Result is the official website for the given company lead.

**Company Lead Information:**
---
- **Name:** "{lead_data.get('name', 'N/A')}"
- **Known Address:** "{lead_data.get('address', 'N/A')}"
- **Known Business Type:** "{lead_data.get('business_type', 'N/A')}"

**Web Search Result to Validate:**
---
- **Title:** "{search_result.get('title', 'N/A')}"
- **URL:** "{search_result.get('link', 'N/A')}"
- **Snippet:** "{search_result.get('snippet', 'N/A')}"

**Your Task:**
Analyze all the information. Respond ONLY with a single, valid JSON object with one key, "is_correct_website", and a boolean value (true or false).
- Set the value to **true** if you are highly confident the URL is the official website for the company lead.
- Set the value to **false** if it is a directory, a different company, a social media page, or if you are uncertain.

Your entire response must be only the JSON object.
"""

def get_prompt_for_file_report(lead_data: dict, website_summary: str, company_profile: str) -> str:
    """Generates a prompt for creating a detailed, file-based lead report in Markdown."""
    # ... (existing function)
    return f"""
You are an AI sales and marketing strategist for TMC Media. Your task is to analyze the following lead and generate a comprehensive research report in Markdown format.

--- TMC Media Profile (Your Company) ---
{company_profile}

--- Lead Information ---
- Name: {lead_data.get('name', 'N/A')}
- Website: {lead_data.get('website', 'N/A')}
- Phone: {lead_data.get('phone', 'N/A')}
- Address: {lead_data.get('address', 'N/A')}
- Initial Business Type: {lead_data.get('business_type', 'N/A')}

--- Scraped Website Analysis (Summary) ---
{website_summary[:3000]}

--- Your Task ---
Generate a detailed company research report in Markdown format. The report should be well-structured and insightful. Do not add any text before the first heading.

**Report Structure:**
- ## Lead Analysis Report: [Company Name]
- **Company Overview:** A brief summary of the company, its mission, and what it does.
- **Identified Needs:** A bulleted list of 2-4 key business needs this lead likely has that TMC Media can solve.
- **Suggested Outreach Strategy:** A bulleted list of 2-3 specific, actionable outreach suggestions.
- **Critical Missing Information:** Note the most important piece of missing information for an effective sales approach (e.g., key contact person, direct email).
- **Overall Summary & Recommendation:** A concluding paragraph on whether they are a good potential client and the next steps.

Generate the Markdown report now.
"""

def get_prompt_for_smart_list_categorization(lead_data_json: str, list_goal: str, list_name: str) -> str:
    """Creates a detailed prompt for an LLM to categorize a lead for a "Smart List"."""
    # ... (existing function)
    return f"""
You are an expert business analyst AI. Your task is to determine if a specific business lead is a good fit for a list I am building.

My Goal for this List: "{list_goal}"
The Name of this List: "{list_name}"

Here is the data for the business lead you must analyze:
---
{lead_data_json}
---

Analyze the provided lead data. Based on the name, title, source, address, and business type, decide if this lead matches my goal.

You must respond with ONLY a JSON object in the following format. Do not add any other text or markdown.
Your entire response must be a single, valid JSON object.

{{
  "match": boolean,
  "category": "A very short, one-or-two-word category for this lead if it's a match (e.g., 'Startup Prospect', 'Local Restaurant', 'B2B Service'). If it is not a match, this should be null.",
  "justification": "A brief, one-sentence explanation for your decision. Explain WHY it is or is not a match based on my goal and the lead's data."
}}
"""

# --- NEW PROMPT: For validating a lead entry ---
def get_prompt_for_entry_validation(lead_data: dict) -> str:
    """Generates a prompt to validate if a lead entry seems correct or is junk data."""
    return f"""
You are an expert data validation AI. Your task is to determine if the provided lead entry represents a real person or business, or if it is likely junk data.

**Lead Data to Validate:**
---
- **Name:** "{lead_data.get('name', 'N/A')}"
- **Title:** "{lead_data.get('title', 'N/A')}"
- **Business Type:** "{lead_data.get('business_type', 'N/A')}"
- **Source:** "{lead_data.get('source', 'N/A')}"
---

**Your Task:**
Analyze the "Name", "Title", and "Business Type" fields.
- **Valid entries** are names of people (e.g., "John Smith") or businesses (e.g., "Main Street Pizza").
- **Invalid entries** are phone numbers, random character strings, descriptive sentences that are not names (e.g., "Call for a free quote"), placeholder text, or URLs.

Respond ONLY with a single, valid JSON object with two keys:
1. "is_valid": A boolean value (true or false). Set to **false** if you are confident the entry is junk data.
2. "reason": A brief, one-sentence explanation for your decision.

Your entire response must be only the JSON object.
"""