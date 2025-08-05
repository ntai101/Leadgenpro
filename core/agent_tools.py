# core/agent_tools.py
"""
Contains advanced tools for the AI Enrichment Agent, including a feature-rich
BrowserAutomation class and a functional OCRService.
"""
import os
import time
import re
from urllib.parse import urlparse, quote_plus
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from .logging import dbg

# --- OCR Service Dependencies ---
try:
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False


class BrowserAutomation:
    """
    Automates browser interactions using Selenium for web scraping and analysis.
    """
    def __init__(self, headless: bool = True):
        self.driver = None
        self._setup_driver(headless)

    def _setup_driver(self, headless: bool):
        """Sets up the Chrome WebDriver instance."""
        try:
            options = webdriver.ChromeOptions()
            # A realistic User-Agent is always a good practice
            options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
            
            # --- IMPROVEMENT: Add more options to evade bot detection ---
            options.add_argument("--disable-blink-features=AutomationControlled") # Makes it harder for sites to detect automation
            options.add_argument("--start-maximized") # More natural browsing behavior

            if headless:
                options.add_argument("--headless=new")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")
            
            options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(30)
            dbg("BrowserAutomation initialized successfully.")
        except Exception as e:
            dbg(f"Failed to initialize Chrome WebDriver: {e}")
            self.driver = None

    def __enter__(self):
        if not self.driver: self._setup_driver(headless=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_browser()

    def navigate_to_url(self, url: str) -> bool:
        """Navigates to the given URL, waiting for the body to be present."""
        if not self.driver: return False
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            return True
        except (TimeoutException, WebDriverException) as e:
            dbg(f"Error navigating to {url}: {e.__class__.__name__}")
            return False

    def get_full_page_text(self, max_chars: int = 4000) -> str:
        """Extracts text content from the body, excluding script and style tags."""
        if not self.driver: return ""
        try:
            # --- IMPROVEMENT: Use JavaScript to get cleaner text content ---
            body_text = self.driver.execute_script("""
                var element = document.body;
                // Remove script and style elements
                var scripts = element.getElementsByTagName('script');
                while (scripts.length > 0) { scripts[0].parentNode.removeChild(scripts[0]); }
                var styles = element.getElementsByTagName('style');
                while (styles.length > 0) { styles[0].parentNode.removeChild(styles[0]); }
                return element.innerText;
            """)
            # Clean up excessive newlines
            cleaned_text = re.sub(r'(\n\s*){3,}', '\n\n', body_text).strip()
            return (cleaned_text[:max_chars] + "...") if max_chars and len(cleaned_text) > max_chars else cleaned_text
        except Exception as e:
            dbg(f"Error extracting page text: {e}")
            return ""

    def find_and_click_link(self, link_texts: list[str]) -> bool:
        """Tries to find and click a link containing any of the given texts."""
        if not self.driver: return False
        original_url = self.driver.current_url
        for text in link_texts:
            try:
                # Use a more flexible XPath to find links
                xpath = f"//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text.lower()}')]"
                link = WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH, xpath)))
                
                if link.is_displayed() and link.is_enabled():
                    # --- IMPROVEMENT: Use JavaScript click as a more reliable fallback ---
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", link)
                        time.sleep(0.5)
                        link.click()
                    except WebDriverException:
                        dbg("Standard click failed, trying JavaScript click.")
                        self.driver.execute_script("arguments[0].click();", link)
                        
                    WebDriverWait(self.driver, 10).until(lambda d: d.current_url != original_url)
                    return True
            except (NoSuchElementException, TimeoutException):
                continue
        return False

    def search_and_scrape_results(self, query: str, num_results: int = 3) -> list[dict]:
        """Uses DuckDuckGo's HTML version to perform a search and scrape results."""
        if not self.driver: return []
        try:
            encoded_query = quote_plus(query)
            self.navigate_to_url(f"https://html.duckduckgo.com/html/?q={encoded_query}")
            
            result_selector = "div.result"
            WebDriverWait(self.driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, result_selector)))
            
            results = []
            result_containers = self.driver.find_elements(By.CSS_SELECTOR, result_selector)
            
            for container in result_containers[:num_results]:
                try:
                    title_element = container.find_element(By.CSS_SELECTOR, "h2.result__title a")
                    link_element = container.find_element(By.CSS_SELECTOR, "a.result__url")
                    snippet_element = container.find_element(By.CSS_SELECTOR, "a.result__snippet")
                    title, link, snippet = title_element.text, link_element.get_attribute("href"), snippet_element.text
                    if title and link and snippet:
                        results.append({"title": title, "link": link, "snippet": snippet})
                except NoSuchElementException:
                    continue
            
            dbg(f"Scraped {len(results)} results from DuckDuckGo for query: '{query}'")
            return results
        except Exception as e:
            dbg(f"An error occurred during DuckDuckGo scrape: {e.__class__.__name__} - {e}")
            return []

    # --- NEW FUNCTION ---
    def extract_social_media_links(self) -> dict:
        """Finds all social media links on the current page."""
        if not self.driver: return {}
        
        social_platforms = {
            'linkedin': r"linkedin\.com/company/|linkedin\.com/in/",
            'twitter': r"twitter\.com/|x\.com/",
            'facebook': r"facebook\.com/",
            'instagram': r"instagram\.com/",
            'youtube': r"youtube\.com/channel/|youtube\.com/user/"
        }
        
        links = self.driver.find_elements(By.TAG_NAME, "a")
        found_links = {}
        
        for link in links:
            href = link.get_attribute('href')
            if not href: continue
            
            for platform, pattern in social_platforms.items():
                if platform not in found_links and re.search(pattern, href, re.IGNORECASE):
                    found_links[platform] = href
                    break # Move to the next link once a platform is found
                    
        dbg(f"Found social media links: {found_links}")
        return found_links

    def analyze_site_deep(self, base_url: str) -> dict:
        """Performs a deeper analysis of a website by visiting common pages."""
        if not self.driver or not base_url: return {"error": "Driver not available"}

        analysis = {"pages_visited": [], "page_content": {}, "social_links": {}}
        
        if self.navigate_to_url(base_url):
            analysis["pages_visited"].append(self.driver.current_url)
            analysis["page_content"]["homepage"] = self.get_full_page_text()
            # --- IMPROVEMENT: Call the new social media extraction function ---
            analysis["social_links"].update(self.extract_social_media_links())

        # Reset to homepage before trying to find the next link
        self.navigate_to_url(base_url)
        if self.find_and_click_link(["about", "company", "who we are"]):
            analysis["pages_visited"].append(self.driver.current_url)
            analysis["page_content"]["about"] = self.get_full_page_text()
            # Also check for social links on the about page
            analysis["social_links"].update(self.extract_social_media_links())

        dbg(f"Deep analysis for {base_url} complete. Visited: {len(analysis['pages_visited'])} pages.")
        return analysis
    
    def screenshot(self, path: str) -> str | None:
        if not self.driver: return None
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.driver.save_screenshot(path)
            dbg(f"Screenshot saved to: {path}")
            return path
        except Exception as e:
            dbg(f"Failed to save screenshot to {path}: {e}")
            return None

    def close_browser(self):
        """Quits the browser and closes the WebDriver session."""
        if self.driver:
            self.driver.quit()
            self.driver = None


class OCRService:
    """A functional wrapper for the Tesseract OCR engine."""
    def extract_text_from_image(self, image_path: str) -> str:
        if not OCR_AVAILABLE:
            msg = "OCR libraries not installed. Please run 'pip install pytesseract pillow'."
            dbg(f"[OCRService ERR] {msg}")
            return msg
        try:
            text = pytesseract.image_to_string(Image.open(image_path))
            dbg(f"[OCRService] Successfully extracted text from {os.path.basename(image_path)}")
            return text or "No text could be extracted from the image."
        except Exception as e:
            dbg(f"[OCRService ERR] Failed to process image {image_path}: {e}")
            return f"Error during OCR processing: {e}. Ensure Tesseract is installed and in your system's PATH."