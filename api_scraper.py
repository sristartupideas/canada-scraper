#!/usr/bin/env python3
"""
Business Scraper API - FastAPI Version
Provides HTTP endpoints for business data instead of file saving
"""

import sys
import subprocess
import importlib
import time
import logging
import re
import json
import random
from typing import List, Dict, Union, Optional
from dataclasses import dataclass
import asyncio
import threading

# Global semaphore to limit concurrent browser instances (prevent OOM/crashes)
BROWSER_SEMAPHORE = threading.BoundedSemaphore(1)  # Max 1 browser for low-memory environments

# Check and install dependencies
def install_and_import(package_name: str, import_name: str = None):
    """Install package if not available and import it."""
    if import_name is None:
        import_name = package_name
    
    try:
        return importlib.import_module(import_name)
    except ImportError:
        print(f"Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        return importlib.import_module(import_name)

# Install required dependencies
print("Checking dependencies...")
try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
    print("✓ FastAPI available")
except ImportError:
    FASTAPI_AVAILABLE = False
    print("! FastAPI not available - install fastapi")

# Try to import botasaurus (required for browser mode)
try:
    from botasaurus import browser
    from botasaurus.browser import Driver
    BOTASAURUS_AVAILABLE = True
    print("✓ Botasaurus available for browser mode")
except ImportError:
    BOTASAURUS_AVAILABLE = False
    print("! Botasaurus not available - install botasaurus")

# --- Botasaurus runtime adapter: expand `options` dict into real kwargs if needed ---
import logging
logger = logging.getLogger(__name__)

def install_botasaurus_adapter():
    try:
        import inspect
        from importlib import import_module
        mod = import_module('botasaurus.browser')
        orig_browser = getattr(mod, 'browser')
        sig = inspect.signature(orig_browser)
        if 'options' not in sig.parameters:
            def _browser_wrapper(*args, **kwargs):
                if 'options' in kwargs and isinstance(kwargs['options'], dict):
                    opts = kwargs.pop('options')
                    for k, v in opts.items():
                        kwargs.setdefault(k, v)
                return orig_browser(*args, **kwargs)
            _browser_wrapper.__name__ = getattr(orig_browser, "__name__", "browser")
            _browser_wrapper.__doc__ = getattr(orig_browser, "__doc__", "")
            setattr(mod, 'browser', _browser_wrapper)
            # also set on package root if present
            try:
                import botasaurus as _bt
                if getattr(_bt, 'browser', None) is None:
                    setattr(_bt, 'browser', _browser_wrapper)
            except Exception:
                pass
            logger.info("Botasaurus adapter installed (options -> kwargs).")
    except Exception as e:
        logger.warning("Botasaurus adapter install failed or not needed: %s", e)

# call it if botasaurus is available
try:
    import botasaurus
    install_botasaurus_adapter()
except Exception:
    pass

# --- CONFIGURATION ---
BASE_URL = "https://canada.businessesforsale.com/canadian/search/businesses-for-sale?Price.From=4000000&PriceDisclosedOnly=1"
MAX_PAGES_TO_SCRAPE = 7  # 7 pages should give us 165 businesses
MAX_BUSINESSES_TO_SCRAPE = 165  # Target all 165 listings available
SCRAPE_FULL_DESCRIPTIONS = False  # Set to False for fast scraping, True for detailed scraping
TIMEOUT_SECONDS = 30

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingStats:
    total_businesses: int = 0
    pages_processed: int = 0
    pages_failed: int = 0
    start_time: float = 0
    mode_used: str = ""

# Global storage for scraped data
scraped_data = []
scraping_in_progress = False
last_scrape_time = None

def clean_and_convert_to_float(value_str: str) -> Optional[float]:
    """Clean and convert financial strings to float."""
    if not isinstance(value_str, str):
        return None
    try:
        clean_str = value_str.lower()
        clean_str = re.sub(r'[\$,cad,usd]', '', clean_str)
        clean_str = re.sub(r'\(.*?\)', '', clean_str)
        clean_str = clean_str.replace(',', '').strip()

        if 'm' in clean_str:
            return float(clean_str.replace('m', '')) * 1_000_000
        if 'k' in clean_str:
            return float(clean_str.replace('k', '')) * 1_000
        return float(clean_str)
    except (ValueError, TypeError):
        return None

def fast_scrape_with_browser() -> List[Dict]:
    """Fast browser scraping using optimized logic from standalone scraper."""
    if not BOTASAURUS_AVAILABLE:
        logger.error("❌ Botasaurus not available - install botasaurus")
        return []
    
    logger.info("🚀 Starting FAST optimized browser scraping...")
    
    from botasaurus.browser import browser, Driver
    
    @browser(headless=True)
    def optimized_scraper(driver: Driver, _):
        """Optimized scraper that gets all 165 businesses with full descriptions."""
        all_businesses = []
        base_url = "https://canada.businessesforsale.com/canadian/search/businesses-for-sale?Price.From=4000000&PriceDisclosedOnly=1"
        
        for page_num in range(1, 8):  # 7 pages should give us 165 businesses
            try:
                url = f"{base_url}&page={page_num}"
                logger.info(f"📡 Accessing page {page_num}: {url}")
                
                driver.get(url)
                driver.sleep(5)  # 5 second wait works for Cloudflare bypass
                
                title = driver.title
                logger.info(f"📄 Page {page_num} title: {title}")
                
                # Get all listings on this page
                listings = driver.select_all('.result')
                logger.info(f"🔍 Page {page_num}: Found {len(listings)} listings")
                
                if len(listings) > 0:
                    logger.info(f"✅ Page {page_num}: Success! Found listings")
                    
                    page_businesses = []
                    for i in range(len(listings)):
                        try:
                            # Re-fetch listings after each individual page visit to avoid DOM disconnection
                            current_listings = driver.select_all('.result')
                            if i >= len(current_listings):
                                logger.warning(f"❌ Listing {i+1}: Not enough listings found")
                                continue
                                
                            listing = current_listings[i]
                            business_data = {}
                            
                            # Get title and URL
                            title_elem = listing.select('h2 a')
                            if title_elem:
                                business_data['title'] = title_elem.text.strip()
                                business_data['url'] = title_elem.get_attribute('href')
                            else:
                                logger.warning(f"❌ Listing {i+1}: No title found")
                                continue
                            
                            # Get location
                            loc_elem = listing.select('tr.t-loc td')
                            business_data['location'] = loc_elem.text.strip() if loc_elem else 'N/A'
                            
                            # Get summary description
                            desc_elem = listing.select('tr.t-desc p')
                            business_data['summary_description'] = desc_elem.text.strip() if desc_elem else 'N/A'
                            
                            # Get financial info
                            finance_row = listing.select('tr.t-finance')
                            if finance_row:
                                nested_table = finance_row.select('table')
                                if nested_table:
                                    rows = nested_table.select_all('tr')
                                    for row in rows:
                                        try:
                                            header_el = row.select('th')
                                            value_el = row.select('td')
                                            if header_el and value_el:
                                                header = header_el.text.strip().lower().replace(':', '').replace(' ', '_')
                                                value = value_el.text.strip()
                                                if header and value:
                                                    business_data[header] = value
                                        except:
                                            continue
                            
                            # Get tags
                            tags_container = listing.select('.t-tags')
                            if tags_container:
                                tag_elements = tags_container.select_all('li')
                                tags = []
                                for tag_element in tag_elements:
                                    try:
                                        tag_text = tag_element.text.strip()
                                        if tag_text:
                                            tags.append(tag_text)
                                    except:
                                        continue
                                business_data['business_type_tags'] = '\n'.join(tags) if tags else 'N/A'
                            else:
                                business_data['business_type_tags'] = 'N/A'
                            
                            # Add metadata
                            business_data['contact_url'] = f"{business_data['url']}/contact" if business_data.get('url') else 'N/A'
                            business_data['listing_id'] = 'N/A'
                            business_data['thumbnail_url'] = 'N/A'
                            business_data['scraped_page'] = page_num
                            business_data['scraped_method'] = 'fast_optimized_api_scraper'
                            
                            # NOW GET FULL DESCRIPTION FROM INDIVIDUAL PAGE
                            if business_data.get('url') and business_data['url'] != 'N/A':
                                logger.info(f"📄 Getting full description for: {business_data['title'][:50]}...")
                                
                                try:
                                    driver.get(business_data['url'])
                                    driver.sleep(2)  # Quick wait for page load
                                    
                                    # Check for Cloudflare
                                    page_title = driver.title
                                    if "Just a moment" in page_title:
                                        logger.info(f"⏳ Cloudflare detected, waiting...")
                                        driver.sleep(3)
                                    
                                    # Get full description and additional data from individual page
                                    full_description = ""
                                    
                                    # Try the exact selector we found: .listing-paragraph
                                    try:
                                        desc_elements = driver.select_all('.listing-paragraph')
                                        if desc_elements:
                                            # Combine all paragraph texts
                                            desc_texts = []
                                            for elem in desc_elements:
                                                text = elem.text.strip()
                                                if text and len(text) > 20:  # Skip very short paragraphs
                                                    desc_texts.append(text)
                                            
                                            if desc_texts:
                                                full_description = ' '.join(desc_texts)
                                                logger.info(f"✅ Found full description: {len(full_description)} chars")
                                            else:
                                                logger.warning(f"⚠️ No substantial paragraphs found")
                                        else:
                                            logger.warning(f"⚠️ No .listing-paragraph elements found")
                                            
                                    except Exception as e:
                                        logger.error(f"❌ Error getting description: {e}")
                                    
                                    # Extract additional enhanced data from individual page
                                    try:
                                        # Get real listing ID
                                        listing_id_elem = driver.select('#listing-id')
                                        if listing_id_elem:
                                            business_data['listing_id'] = listing_id_elem.text.strip()
                                            logger.info(f"✅ Found listing ID: {business_data['listing_id']}")
                                        
                                        # Get thumbnail from meta tags
                                        og_image_elem = driver.select('meta[property="og:image"]')
                                        if og_image_elem:
                                            thumbnail_url = og_image_elem.get_attribute('content')
                                            if thumbnail_url and 'facebookDefaultImage' not in thumbnail_url:
                                                business_data['thumbnail_url'] = thumbnail_url
                                                logger.info(f"✅ Found thumbnail: {thumbnail_url}")
                                        
                                        # Get additional financial details
                                        revenue_elem = driver.select('#revenue dd')
                                        if revenue_elem:
                                            detailed_revenue = revenue_elem.text.strip()
                                            if detailed_revenue and detailed_revenue != business_data.get('revenue', ''):
                                                business_data['detailed_financials'] = f"Revenue: {detailed_revenue}"
                                                logger.info(f"✅ Found detailed revenue: {detailed_revenue}")
                                        
                                    except Exception as e:
                                        logger.error(f"❌ Error extracting additional data: {e}")
                                    
                                    if full_description:
                                        business_data['full_description'] = full_description
                                    else:
                                        business_data['full_description'] = business_data.get('summary_description', 'N/A')
                                        logger.warning(f"⚠️ Using summary as full description")
                                    
                                    # Navigate back to search results page
                                    driver.get(url)
                                    driver.sleep(2)  # Wait for page to load
                                    
                                except Exception as e:
                                    logger.error(f"❌ Error scraping detail page: {e}")
                                    business_data['full_description'] = business_data.get('summary_description', 'N/A')
                            else:
                                business_data['full_description'] = business_data.get('summary_description', 'N/A')
                            
                            page_businesses.append(business_data)
                            logger.info(f"✅ Listing {i+1}: {business_data['title'][:50]}...")
                            
                        except Exception as e:
                            logger.warning(f"❌ Error processing listing {i+1}: {e}")
                            continue
                    
                    all_businesses.extend(page_businesses)
                    logger.info(f"✅ Page {page_num}: Added {len(page_businesses)} businesses. Total: {len(all_businesses)}")
                else:
                    logger.warning(f"❌ Page {page_num}: No listings found")
                    break
                
                # Random delay between pages
                if page_num < 7:
                    delay = random.uniform(2, 5)
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before next page...")
                    driver.sleep(delay)
                    
            except Exception as e:
                logger.error(f"❌ Error on page {page_num}: {e}")
                continue
        
        return all_businesses
    
    try:
        return optimized_scraper()
    except Exception as e:
        logger.error(f"Fast optimized scraping failed: {e}")
        return []

def scrape_individual_listing_page(driver: Driver, listing_url: str) -> Dict:
    """Scrape an individual listing page for full details."""
    try:
        logger.info(f"📄 Scraping detail page: {listing_url}")
        
        driver.get(listing_url)
        driver.sleep(2)  # Quick wait for page load
        
        # Check for Cloudflare
        page_title = driver.title
        if "Just a moment" in page_title:
            logger.info("Detail page: Cloudflare detected, waiting...")
            driver.sleep(3)  # Wait for Cloudflare
        
        detail_data = {}
        
        # Extract full description from detail page with only the most effective selectors
        description_selectors = [
            '.listing-paragraph',  # Main description paragraph (most effective)
            'div[class*="details"]',  # Second most effective
            '.listing-description'  # Third option
        ]
        
        full_description = ""
        for selector in description_selectors:
            try:
                desc_elem = driver.select(selector)
                if desc_elem:
                    desc_text = desc_elem.text.strip()
                    if desc_text and len(desc_text) > 100:  # Ensure it's substantial
                        full_description = desc_text
                        logger.info(f"✅ Found description with selector: {selector}")
                        break
            except:
                continue
        
        if not full_description:
            # Fallback: look for paragraphs in main content areas
            try:
                paragraph_selectors = [
                    'main p',
                    '.content p', 
                    '.listing p',
                    '.business p',
                    '.description p',
                    '.details p',
                    '.main-content p',
                    '.listing-content p',
                    '.business-content p'
                ]
                
                for selector in paragraph_selectors:
                    try:
                        paragraphs = driver.select_all(selector)
                        if paragraphs:
                            desc_parts = []
                            for p in paragraphs:
                                text = p.text.strip()
                                if text and len(text) > 20:  # Skip very short paragraphs
                                    desc_parts.append(text)
                            
                            if desc_parts:
                                full_description = ' '.join(desc_parts)
                                logger.info(f"✅ Found description with paragraph selector: {selector}")
                                break
                    except:
                        continue
            except:
                pass
        
        # If still no description, try to get any substantial text content
        if not full_description:
            try:
                # Get all text content and look for substantial paragraphs
                page_text = driver.run_js("return document.body.innerText;")
                if page_text:
                    # Split into paragraphs and find substantial ones
                    paragraphs = [p.strip() for p in page_text.split('\n') if p.strip()]
                    substantial_paragraphs = [p for p in paragraphs if len(p) > 100]
                    if substantial_paragraphs:
                        full_description = ' '.join(substantial_paragraphs[:3])  # Take first 3 substantial paragraphs
                        logger.info("✅ Found description from page text content")
            except:
                pass
        
        detail_data['full_description'] = full_description if full_description else 'N/A'
        
        # Extract additional enhanced data from individual page
        try:
            # Get real listing ID
            listing_id_elem = driver.select('#listing-id')
            if listing_id_elem:
                detail_data['listing_id'] = listing_id_elem.text.strip()
                logger.info(f"✅ Found listing ID: {detail_data['listing_id']}")
            
            # Get thumbnail from meta tags
            og_image_elem = driver.select('meta[property="og:image"]')
            if og_image_elem:
                thumbnail_url = og_image_elem.get_attribute('content')
                if thumbnail_url and 'facebookDefaultImage' not in thumbnail_url:
                    detail_data['thumbnail_url'] = thumbnail_url
                    logger.info(f"✅ Found thumbnail: {thumbnail_url}")
            
            # Get additional financial details
            revenue_elem = driver.select('#revenue dd')
            if revenue_elem:
                detailed_revenue = revenue_elem.text.strip()
                if detailed_revenue:
                    detail_data['detailed_financials'] = f"Revenue: {detailed_revenue}"
                    logger.info(f"✅ Found detailed revenue: {detailed_revenue}")
            
        except Exception as e:
            logger.error(f"❌ Error extracting additional data: {e}")
        
        # Extract additional details from detail page
        try:
            # Look for more detailed financial information
            financial_selectors = [
                '.financial-info',
                '.business-financials', 
                '.listing-financials',
                '.financial-details',
                '.business-financials',
                '.listing-financials',
                '[class*="financial"]',
                '.revenue-details',
                '.cash-flow-details'
            ]
            
            financial_details = []
            for selector in financial_selectors:
                try:
                    financial_sections = driver.select_all(selector)
                    if financial_sections:
                        for section in financial_sections:
                            text = section.text.strip()
                            if text and len(text) > 20:
                                financial_details.append(text)
                except:
                    continue
            
            detail_data['detailed_financials'] = ' | '.join(financial_details) if financial_details else 'N/A'
        except:
            detail_data['detailed_financials'] = 'N/A'
        
        # Extract contact information
        try:
            contact_selectors = [
                '.contact-info',
                '.seller-contact',
                '.listing-contact',
                '.business-contact',
                '.contact-details',
                '.broker-info',
                '.agent-info',
                '[class*="contact"]',
                '[class*="broker"]',
                '[class*="agent"]'
            ]
            
            contact_info = ""
            for selector in contact_selectors:
                try:
                    contact_elem = driver.select(selector)
                    if contact_elem:
                        contact_text = contact_elem.text.strip()
                        if contact_text and len(contact_text) > 10:
                            contact_info = contact_text
                            break
                except:
                    continue
            
            detail_data['contact_info'] = contact_info if contact_info else 'N/A'
        except:
            detail_data['contact_info'] = 'N/A'
        
        # Extract business type/category from detail page
        try:
            category_selectors = [
                '.business-category',
                '.listing-category',
                '.category',
                '.business-type',
                '.property-type',
                '.industry',
                '.sector',
                '[class*="category"]',
                '[class*="type"]',
                '[class*="industry"]'
            ]
            
            business_type = ""
            for selector in category_selectors:
                try:
                    cat_elem = driver.select(selector)
                    if cat_elem:
                        cat_text = cat_elem.text.strip()
                        if cat_text and len(cat_text) > 5:
                            business_type = cat_text
                            break
                except:
                    continue
            
            detail_data['detailed_business_type'] = business_type if business_type else 'N/A'
        except:
            detail_data['detailed_business_type'] = 'N/A'
        
        logger.info(f"✅ Detail page scraped: {len(full_description)} chars description")
        return detail_data
        
    except Exception as e:
        logger.error(f"❌ Error scraping detail page {listing_url}: {e}")
        return {'full_description': 'N/A', 'detailed_financials': 'N/A', 'contact_info': 'N/A', 'detailed_business_type': 'N/A'}

def scrape_with_browser() -> List[Dict]:
    """Browser-based scraping with full descriptions from individual listing pages."""
    if not BOTASAURUS_AVAILABLE:
        logger.error("❌ Botasaurus not available - install botasaurus")
        return []
    
    logger.info("🌐 Starting browser mode with FULL DESCRIPTIONS - visiting each listing page...")
    
    from botasaurus.browser import browser, Driver
    
    @browser(headless=True)
    def scrape_all_pages_browser(driver: Driver, _):
        """Browser-based scraping with individual page visits for full descriptions."""
        all_businesses = []
        
        for page_num in range(1, MAX_PAGES_TO_SCRAPE + 1):
            try:
                url = f"{BASE_URL}&page={page_num}"
                logger.info(f"🌐 Browser: Processing page {page_num}")
                
                driver.get(url)
                driver.sleep(3)  # Reduced wait for faster scraping
                
                # Check for Cloudflare but don't skip if we can still find listings
                page_title = driver.title
                logger.info(f"Page {page_num}: Title = '{page_title}'")
                
                # Check if driver is still responsive
                try:
                    current_title = driver.title
                    if not current_title:
                        logger.error(f"Page {page_num}: Driver not responsive, stopping")
                        break
                except Exception as e:
                    logger.error(f"Page {page_num}: Driver connection lost: {e}")
                    break
                
                listings = driver.select_all('.result')
                page_businesses = []
                for i in range(len(listings)):
                    try:
                        # Re-fetch listings after each individual page visit to avoid DOM disconnection
                        current_listings = driver.select_all('.result')
                        if i >= len(current_listings):
                            logger.warning(f"❌ Listing {i+1}: Not enough listings found")
                            continue
                            
                        listing = current_listings[i]
                        business_data = {}
                        
                        # 1. Title and URL
                        title_elem = listing.select('h2 a')
                        if title_elem:
                            business_data['title'] = title_elem.text.strip()
                            business_data['url'] = title_elem.get_attribute('href')
                        else:
                            business_data['title'] = 'N/A'
                            business_data['url'] = 'N/A'
                        
                        # 2. Location
                        loc_elem = listing.select('tr.t-loc td')
                        business_data['location'] = loc_elem.text.strip() if loc_elem else 'N/A'
                        
                        # 3. Summary Description (from search results)
                        desc_elem = listing.select('tr.t-desc p')
                        business_data['summary_description'] = desc_elem.text.strip() if desc_elem else 'N/A'
                        
                        # 4. ALL Financial Information
                        finance_row = listing.select('tr.t-finance')
                        if finance_row:
                            nested_table = finance_row.select('table')
                            if nested_table:
                                rows = nested_table.select_all('tr')
                                if not rows:
                                    tbody = nested_table.select('tbody')
                                    if tbody:
                                        rows = tbody.select_all('tr')
                                
                                # Extract all financial data
                                for row in rows:
                                    try:
                                        header_el = row.select('th')
                                        value_el = row.select('td')
                                        
                                        if header_el and value_el:
                                            header = header_el.text.strip().lower().replace(':', '').replace(' ', '_')
                                            value = value_el.text.strip()
                                            
                                            if header and value:
                                                business_data[header] = value
                                                # Create numeric version
                                                numeric_value = clean_and_convert_to_float(value)
                                                if numeric_value is not None:
                                                    business_data[f"{header}_numeric"] = numeric_value
                                    except Exception:
                                        continue
                        
                        # 5. Business type tags
                        tags_container = listing.select('.t-tags')
                        if tags_container:
                            tag_elements = tags_container.select_all('li')
                            tags = []
                            for tag_element in tag_elements:
                                try:
                                    tag_text = tag_element.text.strip()
                                    # Clean up tag text (remove icon names)
                                    tag_text = re.sub(r'location_on|gavel|flash_on|share', '', tag_text).strip()
                                    if tag_text:
                                        tags.append(tag_text)
                                except:
                                    continue
                            business_data['business_type_tags'] = ', '.join(tags) if tags else 'N/A'
                        else:
                            business_data['business_type_tags'] = 'N/A'
                        
                        # 6. Contact URL
                        contact_elem = listing.select('.contact-seller, .contact-franchise')
                        business_data['contact_url'] = contact_elem.get_attribute('href') if contact_elem else 'N/A'
                        
                        # 7. Listing ID
                        save_elem = listing.select('.shortlist-ajax')
                        if save_elem:
                            save_url = save_elem.get_attribute('href')
                            if save_url and 'addListingId=' in save_url:
                                listing_id = save_url.split('addListingId=')[1].split('&')[0]
                                business_data['listing_id'] = listing_id
                            else:
                                business_data['listing_id'] = 'N/A'
                        else:
                            business_data['listing_id'] = 'N/A'
                        
                        # 8. Thumbnail URL
                        thumb_elem = listing.select('.t-thumb img')
                        business_data['thumbnail_url'] = thumb_elem.get_attribute('src') if thumb_elem else 'N/A'
                        
                        # 9. Metadata
                        business_data['scraped_page'] = page_num
                        business_data['scraped_method'] = 'browser_with_full_descriptions'
                        
                        # 10. SCRAPE INDIVIDUAL LISTING PAGE FOR FULL DESCRIPTION (if enabled)
                        if SCRAPE_FULL_DESCRIPTIONS and business_data.get('url') and business_data['url'] != 'N/A':
                            detail_data = scrape_individual_listing_page(driver, business_data['url'])
                            business_data.update(detail_data)
                            
                            # Navigate back to search results page
                            driver.get(url)
                            driver.sleep(2)  # Wait for page to load
                        else:
                            # Use summary description as full description for fast scraping
                            business_data['full_description'] = business_data.get('summary_description', 'N/A')
                            business_data['detailed_financials'] = 'N/A'
                            business_data['contact_info'] = 'N/A'
                            business_data['detailed_business_type'] = 'N/A'
                        
                        # Only add if we have at least a title
                        if business_data.get('title') != 'N/A':
                            page_businesses.append(business_data)
                            
                    except Exception as e:
                        logger.warning(f"❌ Error processing listing {i+1}: {e}")
                        continue
                
                all_businesses.extend(page_businesses)
                logger.info(f"🌐 Browser Page {page_num}: {len(page_businesses)} businesses. Total: {len(all_businesses)}")
                
                # Check if we've reached our target
                if len(all_businesses) >= MAX_BUSINESSES_TO_SCRAPE:
                    logger.info(f"🎯 Target reached! Stopping at {len(all_businesses)} businesses")
                    break
                
                # Random delay between pages to avoid detection
                if page_num < MAX_PAGES_TO_SCRAPE:
                    delay = random.uniform(1, 2)  # Faster delay between pages
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before next page...")
                    driver.sleep(delay)
                    
            except Exception as e:
                error_msg = str(e).strip()
                if not error_msg:  # Empty error message indicates connection lost
                    logger.error(f"Browser Page {page_num}: Connection lost, stopping scraper")
                    break
                else:
                    logger.error(f"Browser Page {page_num} error: {e}")
                    continue
        
        return all_businesses
    
    # Run the browser scraper
    try:
        businesses = scrape_all_pages_browser()
        return businesses
    except Exception as e:
        logger.error(f"Browser mode failed: {e}")
        return []

# Initialize FastAPI app
if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="Business Scraper API",
        description="API for scraping high-value business listings",
        version="1.0.0"
    )

    @app.get("/health")
    async def health():
        """Health check endpoint for Render monitoring"""
        return {"status": "ok", "service": "business-scraper-api"}
    
    @app.get("/")
    async def root():
        """Root endpoint with API information."""
        return {
            "message": "Business Scraper API",
            "version": "1.0.0",
            "endpoints": {
                "/scrape": "Start fast scraping (POST) - gets all 165 businesses quickly",
                "/scrape/details": "Get full descriptions for specific businesses (POST)",
                "/data": "Get scraped data (GET)",
                "/status": "Get scraping status (GET)",
                "/health": "Health check (GET)",
                "/data/search": "Search businesses (GET)",
                "/data/{business_id}": "Get specific business (GET)"
            },
            "usage": {
                "fast_scraping": "POST /scrape - Gets all 165 businesses in ~5-10 minutes",
                "detailed_scraping": "POST /scrape/details - Gets full descriptions for specific businesses",
                "search": "GET /data/search?q=keyword&location=city&min_price=1000000"
            }
        }
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "botasaurus_available": BOTASAURUS_AVAILABLE,
            "fastapi_available": FASTAPI_AVAILABLE,
            "last_scrape_time": last_scrape_time,
            "data_count": len(scraped_data)
        }
    
    @app.get("/status")
    async def get_status():
        """Get current scraping status."""
        return {
            "scraping_in_progress": scraping_in_progress,
            "last_scrape_time": last_scrape_time,
            "data_count": len(scraped_data),
            "target_businesses": MAX_BUSINESSES_TO_SCRAPE
        }
    
    @app.get("/data")
    async def get_data():
        """Get all scraped business data."""
        if not scraped_data:
            raise HTTPException(status_code=404, detail="No data available. Start scraping first.")
        
        return {
            "count": len(scraped_data),
            "last_scrape_time": last_scrape_time,
            "businesses": scraped_data
        }
    
    @app.get("/data/{business_id}")
    async def get_business(business_id: str):
        """Get specific business by ID."""
        if not scraped_data:
            raise HTTPException(status_code=404, detail="No data available. Start scraping first.")
        
        # Find business by listing_id or title
        for business in scraped_data:
            if (business.get('listing_id') == business_id or 
                business.get('title', '').lower().replace(' ', '-') == business_id.lower()):
                return business
        
        raise HTTPException(status_code=404, detail="Business not found")
    
    @app.post("/scrape")
    async def scrape_and_return_data():
        """Scrape and return data directly."""
        global scraping_in_progress, scraped_data, last_scrape_time
        
        if scraping_in_progress:
            raise HTTPException(status_code=409, detail="Scraping already in progress")
        
        if not BOTASAURUS_AVAILABLE:
            raise HTTPException(status_code=503, detail="Botasaurus not available")
        
        # Use semaphore to limit concurrent browser instances
        with BROWSER_SEMAPHORE:
            scraping_in_progress = True
            start_time = time.time()
            
            try:
                logger.info("🎯 Starting direct scraping...")
                businesses = fast_scrape_with_browser()
                
                if businesses:
                    scraped_data = businesses
                    last_scrape_time = time.time()
                    total_time = time.time() - start_time
                    
                    logger.info(f"🏆 Scraping completed! {len(businesses)} businesses in {total_time:.2f}s")
                    
                    return {
                        "message": "Scraping completed successfully",
                        "status": "completed",
                        "count": len(businesses),
                        "scraping_time_seconds": round(total_time, 2),
                        "last_scrape_time": last_scrape_time,
                        "businesses": businesses
                    }
                else:
                    logger.error("💥 Scraping failed - no data retrieved")
                    return {
                        "message": "Scraping failed - no data retrieved",
                        "status": "failed",
                        "count": 0,
                        "businesses": []
                    }
                    
            except Exception as e:
                logger.error(f"💥 Scraping error: {e}")
                return {
                    "message": f"Scraping error: {str(e)}",
                    "status": "error",
                    "count": 0,
                    "businesses": []
                }
            finally:
                scraping_in_progress = False
    
    
    @app.get("/data/search")
    async def search_businesses(
        q: Optional[str] = None,
        location: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        limit: int = 50
    ):
        """Search businesses with filters."""
        if not scraped_data:
            raise HTTPException(status_code=404, detail="No data available. Start scraping first.")
        
        filtered_businesses = scraped_data.copy()
        
        # Apply filters
        if q:
            filtered_businesses = [
                b for b in filtered_businesses 
                if q.lower() in b.get('title', '').lower() or 
                   q.lower() in b.get('summary_description', '').lower()
            ]
        
        if location:
            filtered_businesses = [
                b for b in filtered_businesses 
                if location.lower() in b.get('location', '').lower()
            ]
        
        if min_price:
            filtered_businesses = [
                b for b in filtered_businesses 
                if b.get('asking_price_numeric', 0) >= min_price
            ]
        
        if max_price:
            filtered_businesses = [
                b for b in filtered_businesses 
                if b.get('asking_price_numeric', float('inf')) <= max_price
            ]
        
        # Apply limit
        filtered_businesses = filtered_businesses[:limit]
        
        return {
            "count": len(filtered_businesses),
            "total_available": len(scraped_data),
            "filters_applied": {
                "query": q,
                "location": location,
                "min_price": min_price,
                "max_price": max_price,
                "limit": limit
            },
            "businesses": filtered_businesses
        }
    
    @app.post("/scrape/details")
    async def scrape_details_for_businesses(
        business_ids: List[str] = None,
        limit: int = 10
    ):
        """Get full descriptions for specific businesses or a random sample."""
        global scraping_in_progress, scraped_data
        
        if scraping_in_progress:
            raise HTTPException(status_code=409, detail="Scraping already in progress")
        
        if not scraped_data:
            raise HTTPException(status_code=404, detail="No data available. Run /scrape first.")
        
        if not BOTASAURUS_AVAILABLE:
            raise HTTPException(status_code=503, detail="Botasaurus not available")
        
        scraping_in_progress = True
        start_time = time.time()
        
        try:
            # Select businesses to get details for
            if business_ids:
                selected_businesses = [
                    b for b in scraped_data 
                    if b.get('listing_id') in business_ids or b.get('title', '').lower().replace(' ', '-') in [bid.lower() for bid in business_ids]
                ]
            else:
                # Get a random sample
                import random
                selected_businesses = random.sample(scraped_data, min(limit, len(scraped_data)))
            
            logger.info(f"🎯 Getting details for {len(selected_businesses)} businesses...")
            
            from botasaurus.browser import browser, Driver
            
            @browser(headless=True)
            def get_details_for_businesses(driver: Driver, _):
                detailed_businesses = []
                
                for i, business in enumerate(selected_businesses, 1):
                    try:
                        logger.info(f"📄 Getting details {i}/{len(selected_businesses)}: {business.get('title', 'N/A')}")
                        
                        if business.get('url') and business['url'] != 'N/A':
                            detail_data = scrape_individual_listing_page(driver, business['url'])
                            business.update(detail_data)
                        
                        detailed_businesses.append(business)
                        
                        # Small delay between requests
                        if i < len(selected_businesses):
                            driver.sleep(3)
                            
                    except Exception as e:
                        logger.error(f"Error getting details for {business.get('title', 'N/A')}: {e}")
                        detailed_businesses.append(business)  # Add without details
                        continue
                
                return detailed_businesses
            
            detailed_businesses = get_details_for_businesses()
            
            # Update the global data with detailed information
            for detailed_business in detailed_businesses:
                for i, original_business in enumerate(scraped_data):
                    if original_business.get('listing_id') == detailed_business.get('listing_id'):
                        scraped_data[i] = detailed_business
                        break
            
            total_time = time.time() - start_time
            
            return {
                "message": f"Details scraped for {len(detailed_businesses)} businesses",
                "status": "completed",
                "count": len(detailed_businesses),
                "scraping_time_seconds": round(total_time, 2),
                "businesses": detailed_businesses
            }
            
        except Exception as e:
            logger.error(f"💥 Details scraping error: {e}")
            return {
                "message": f"Details scraping error: {str(e)}",
                "status": "error",
                "count": 0,
                "businesses": []
            }
        finally:
            scraping_in_progress = False

else:
    # Fallback if FastAPI not available
    app = None
    print("❌ FastAPI not available - cannot create API endpoints")

if __name__ == "__main__":
    if app:
        import uvicorn
        print("🚀 Starting Business Scraper API...")
        print("📡 API will be available at: http://localhost:8000")
        print("📚 API docs at: http://localhost:8000/docs")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        print("❌ Cannot start API - FastAPI not available")
