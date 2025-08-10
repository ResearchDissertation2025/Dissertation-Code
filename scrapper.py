from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
import pandas as pd
import os
import re
import zipfile

# Logging setup
logging.basicConfig(
    filename='google_maps_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def search_query(driver, query):
    logging.info(f"Searching for: {query}")
    
    # Extract category and county from query
    try:
        # Pattern to match "{category} in {county}, UK"
        match = re.match(r"^(.*?) in (.*?), UK$", query, re.IGNORECASE)
        if match:
            category = match.group(1).strip().lower()
            county = match.group(2).strip().lower()
        else:
            category = "unknown"
            county = "unknown"
    except Exception as e:
        logging.warning(f"Could not parse query '{query}': {e}")
        category = "unknown"
        county = "unknown"
    
    driver.get("https://www.google.com/maps")
    time.sleep(5)
    
    try:
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="No thanks"]'))
            ).click()
        except:
            pass
        
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "searchboxinput"))
        )
        search_box.clear()
        search_box.send_keys(query)
        search_box.send_keys(Keys.ENTER)
        time.sleep(8)
    except Exception as e:
        logging.error(f"Error during search: {e}")
        raise
    
    # Scrape the business data
    business_data = scrape_all_businesses(driver)
    
    if business_data:

        # Step 2: Extract emails by visiting websites
        business_data = extract_emails_from_websites(driver, business_data)
        
        # Step 3: Extract tech stack and payment methods
        business_data = extract_advanced_info(driver, business_data)

        # Add category and county to each business record
        for business in business_data:
            business['category'] = category
            business['county'] = county
        
        # Create filename from query
        filename = query.lower().replace(",", "").replace(" ", "-").replace("'", "")
        filename = re.sub(r"[^\w-]", "", filename) + ".csv"  # Remove special chars
        
        # Convert to DataFrame and save
        df = pd.DataFrame(business_data)
        
        # Reorder columns to have category and county first
        columns = ['category', 'county'] + [col for col in df.columns if col not in ['category', 'county']]
        df = df[columns]
        
        df.to_csv(filename, index=False)
        logging.info(f"Saved {len(business_data)} businesses to {filename}")
    else:
        logging.warning(f"No businesses found for query: {query}")
    
    return business_data

    
def scrape_all_businesses(driver):
    logging.info("Scraping ALL business information...")
    business_data = []
    processed_names = set()
    max_retries = 3
    scroll_attempts = 0
    max_scroll_attempts = 100
    last_count = 0
    stale_retries = 0
    max_stale_retries = 5

    try:
        # Wait for results container
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//div[contains(@aria-label, "Results for")]'))
        )

        while scroll_attempts < max_scroll_attempts and stale_retries < max_stale_retries:
            scroll_attempts += 1
            
            # Get fresh references to listings
            try:
                listings = driver.find_elements(By.XPATH, '//div[contains(@aria-label, "Results for")]/div/div[./a]')
                current_count = len(listings)
                
                # Check if we've loaded new businesses
                if current_count == last_count:
                    stale_retries += 1
                    if stale_retries >= max_stale_retries:
                        break
                else:
                    stale_retries = 0
                    last_count = current_count
                
                # Process new businesses
                for index in range(len(business_data), current_count):
                    retry_count = 0
                    success = False
                    
                    while not success and retry_count < max_retries:
                        try:
                            # Refresh listings reference
                            listings = driver.find_elements(By.XPATH, '//div[contains(@aria-label, "Results for")]/div/div[./a]')
                            if index >= len(listings):
                                break
                                
                            listing = listings[index]
                            name = get_business_name(listing)
                            
                            if not name or name in processed_names:
                                break
                                
                            processed_names.add(name)
                            logging.info(f"Processing business #{len(business_data) + 1}: {name}")
                            
                            data = process_business_listing(driver, listing, name)
                            if data:
                                business_data.append(data)
                                success = True
                            
                        except StaleElementReferenceException:
                            retry_count += 1
                            logging.warning(f"Stale element (retry {retry_count} for business #{index + 1})")
                            time.sleep(1)
                        except Exception as e:
                            logging.error(f"Error processing business #{index + 1}: {e}")
                            break
                            
                # Scroll to load more results
                scrollable_div = driver.find_element(By.XPATH, '//div[contains(@aria-label, "Results for")]')
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
                time.sleep(2)  # Wait for new results to load
                
                # Random slight scroll up then down to trigger loading
                if scroll_attempts % 5 == 0:
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop - 200", scrollable_div)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_div)
                    time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error during scrolling/processing: {e}")
                stale_retries += 1
                if stale_retries >= max_stale_retries:
                    break
                time.sleep(2)
                continue

    except Exception as e:
        logging.error(f"Error in main scraping loop: {e}")
        raise

    logging.info(f"Successfully processed {len(business_data)} businesses")
    return business_data

def get_business_name(listing_element):
    """Safely get business name with multiple fallbacks"""
    for selector in [
        (By.CSS_SELECTOR, '.qBF1Pd'),
        (By.XPATH, './/div[@role="heading"]'),
        (By.XPATH, './/h3'),
        (By.XPATH, './/div[contains(@class, "fontHeadlineSmall")]')
    ]:
        try:
            name = listing_element.find_element(*selector).text.strip()
            if name:
                return name
        except:
            continue
    return None

def process_business_listing(driver, listing_element, business_name):
    """Process individual business listing"""
    data = {
        "Name": business_name,
        "Rating": None,
        "Address": None,
        "Phone": None,
        "Website": None,
        "Email": None,
        "TechStack": None,
        "PaymentMethods": None
    }

    try:
        # Click to open details panel
        link = listing_element.find_element(By.XPATH, './/a[contains(@class, "hfpxzc")]')
        driver.execute_script("arguments[0].click();", link)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[contains(@aria-label, "Information for")]'))
        )
        time.sleep(1)  # Wait for panel to stabilize

        # Extract details from panel
        panel = driver.find_element(By.XPATH, '//div[contains(@aria-label, "Information for")]')
        
        # Rating
        try:
            data["Rating"] = panel.find_element(
                By.XPATH, './/div[contains(@class, "F7nice")]//span[1]').text
        except NoSuchElementException:
            pass

        # Address
        try:
            data["Address"] = panel.find_element(
                By.XPATH, './/button[contains(@data-item-id, "address")]//div[contains(@class, "fontBodyMedium")]').text
        except NoSuchElementException:
            pass

        # Phone
        try:
            data["Phone"] = panel.find_element(
                By.XPATH, './/button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]').text
        except NoSuchElementException:
            pass

        # Website
        try:
            data["Website"] = panel.find_element(
                By.XPATH, './/a[contains(@data-item-id, "authority")]').get_attribute('href')
        except NoSuchElementException:
            pass

    except Exception as e:
        logging.warning(f"Error processing details for {business_name}: {e}")
    finally:
        # Always return to listings
        try:
            driver.back()
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[contains(@aria-label, "Results for")]'))
            )
            time.sleep(1.5)  # Slightly longer wait for stability
        except Exception as e:
            logging.warning(f"Error returning to listings: {e}")
            # Fallback reload
            current_url = driver.current_url
            if '@' in current_url:
                driver.get(current_url.split('@')[0])
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//div[contains(@aria-label, "Results for")]'))
                )

    return data

def extract_emails_from_websites(driver, business_data):
    """Extract emails by visiting websites after all basic info is collected"""
    logging.info("Starting email extraction from websites...")
    
    for index, business in enumerate(business_data):
        if business['Website']:
            try:
                logging.info(f"Checking website for {business['Name']} ({index+1}/{len(business_data)})")
                
                # Open website in new tab
                driver.execute_script(f"window.open('{business['Website']}');")
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(5)  # Wait for page to load
                
                # Try to find emails on the page
                page_text = driver.find_element(By.TAG_NAME, "body").text
                emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', page_text)
                
                # Also check for mailto links
                mailto_links = driver.find_elements(By.XPATH, '//a[contains(@href, "mailto:")]')
                for link in mailto_links:
                    href = link.get_attribute('href')
                    if href:
                        email = href.replace('mailto:', '').split('?')[0].strip()
                        if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
                            emails.append(email)
                
                # If no emails found, try to find contact/about pages
                if not emails:
                    try:
                        contact_links = driver.find_elements(By.XPATH, '//a[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "contact") or contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "about")]')
                        for link in contact_links[:2]:  # Check first 2 contact/about links
                            try:
                                contact_url = link.get_attribute('href')
                                if contact_url and 'http' in contact_url:
                                    driver.get(contact_url)
                                    time.sleep(3)
                                    contact_text = driver.find_element(By.TAG_NAME, "body").text
                                    found_emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', contact_text)
                                    emails.extend(found_emails)
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if emails:
                    business['Email'] = emails[0]  # Take the first found email
                    logging.info(f"Found email for {business['Name']}: {emails[0]}")
                
                # Close the tab and switch back to main window
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error checking website for {business['Name']}: {e}")
                # Make sure we're back to the main window
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
    
    return business_data

def detect_tech_stack(driver):
    """Detect the technology stack of the current website"""
    tech_stack = {
        'CMS': None,
        'EcommercePlatform': None,
        'ProgrammingLanguage': None,
        'WebServer': None,
        'JavaScriptFramework': None,
        'Analytics': None,
        'PaymentGateway': None
    }
    
    try:
        # Get page source and HTML for analysis
        html = driver.page_source
        html_lower = html.lower()
        
        # Check for common CMS platforms
        if 'wp-content' in html or 'wordpress' in html_lower:
            tech_stack['CMS'] = 'WordPress'
        elif 'shopify' in html_lower:
            tech_stack['CMS'] = 'Shopify'
            tech_stack['EcommercePlatform'] = 'Shopify'
        elif 'magento' in html_lower:
            tech_stack['CMS'] = 'Magento'
            tech_stack['EcommercePlatform'] = 'Magento'
        elif 'woocommerce' in html_lower:
            tech_stack['EcommercePlatform'] = 'WooCommerce'
        elif 'prestashop' in html_lower:
            tech_stack['EcommercePlatform'] = 'PrestaShop'
        elif 'bigcommerce' in html_lower:
            tech_stack['EcommercePlatform'] = 'BigCommerce'
        
        # Check for JavaScript frameworks
        if 'react' in html_lower or 'react-dom' in html_lower:
            tech_stack['JavaScriptFramework'] = 'React'
        elif 'vue' in html_lower:
            tech_stack['JavaScriptFramework'] = 'Vue.js'
        elif 'angular' in html_lower:
            tech_stack['JavaScriptFramework'] = 'Angular'
        
        # Check for common payment gateways
        if 'stripe' in html_lower:
            tech_stack['PaymentGateway'] = 'Stripe'
        elif 'paypal' in html_lower:
            tech_stack['PaymentGateway'] = 'PayPal'
        elif 'braintree' in html_lower:
            tech_stack['PaymentGateway'] = 'Braintree'
        elif 'authorize.net' in html_lower:
            tech_stack['PaymentGateway'] = 'Authorize.net'
        
        # Check for analytics tools
        if 'google-analytics' in html_lower or 'ga.js' in html_lower:
            tech_stack['Analytics'] = 'Google Analytics'
        elif 'gtag.js' in html_lower:
            tech_stack['Analytics'] = 'Google Analytics (gtag)'
        elif 'facebook-pixel' in html_lower:
            tech_stack['Analytics'] = 'Facebook Pixel'
        
        # Clean up None values
        tech_stack = {k: v for k, v in tech_stack.items() if v is not None}
        
        return tech_stack if tech_stack else None
    
    except Exception as e:
        logging.warning(f"Error detecting tech stack: {e}")
        return None

def detect_payment_methods(driver):
    """Detect payment methods on e-commerce sites"""
    payment_methods = []
    payment_icons = {
        'visa': ['visa', 'cc-visa'],
        'mastercard': ['mastercard', 'cc-mastercard'],
        'amex': ['american express', 'amex', 'cc-amex'],
        'discover': ['discover', 'cc-discover'],
        'paypal': ['paypal'],
        'apple pay': ['apple pay'],
        'google pay': ['google pay'],
        'amazon pay': ['amazon pay'],
        'klarna': ['klarna'],
        'afterpay': ['afterpay'],
        'bitcoin': ['bitcoin', 'crypto'],
        'bank transfer': ['bank transfer', 'wire transfer'],
        'cash on delivery': ['cash on delivery', 'cod']
    }
    
    try:
        # Get all text content from the page
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        
        # Check for payment method mentions in text
        for method, keywords in payment_icons.items():
            if any(keyword in page_text for keyword in keywords):
                payment_methods.append(method)
        
        # Check for payment icons/images
        img_alt_texts = [img.get_attribute('alt').lower() for img in driver.find_elements(By.TAG_NAME, "img") if img.get_attribute('alt')]
        img_srcs = [img.get_attribute('src').lower() for img in driver.find_elements(By.TAG_NAME, "img") if img.get_attribute('src')]
        
        for method, keywords in payment_icons.items():
            # Check in alt texts
            if any(any(keyword in alt for keyword in keywords) for alt in img_alt_texts):
                if method not in payment_methods:
                    payment_methods.append(method)
            # Check in image sources
            if any(any(keyword in src for keyword in keywords) for src in img_srcs):
                if method not in payment_methods:
                    payment_methods.append(method)
        
        # Check footer specifically
        try:
            footer = driver.find_element(By.TAG_NAME, "footer").text.lower()
            for method, keywords in payment_icons.items():
                if any(keyword in footer for keyword in keywords):
                    if method not in payment_methods:
                        payment_methods.append(method)
        except:
            pass
        
        # Check checkout page if we can find it
        try:
            checkout_links = [a.get_attribute('href') for a in driver.find_elements(By.TAG_NAME, "a") 
                            if a.get_attribute('href') and 'checkout' in a.get_attribute('href').lower()]
            
            if checkout_links:
                original_window = driver.current_window_handle
                driver.execute_script(f"window.open('{checkout_links[0]}');")
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(3)
                
                checkout_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                for method, keywords in payment_icons.items():
                    if any(keyword in checkout_text for keyword in keywords):
                        if method not in payment_methods:
                            payment_methods.append(method)
                
                driver.close()
                driver.switch_to.window(original_window)
        except:
            pass
        
        return payment_methods if payment_methods else None
    
    except Exception as e:
        logging.warning(f"Error detecting payment methods: {e}")
        return None

def extract_advanced_info(driver, business_data):
    """Extract tech stack and payment methods by visiting websites"""
    logging.info("Starting advanced info extraction from websites...")
    
    for index, business in enumerate(business_data):
        if business['Website']:
            try:
                logging.info(f"Checking website for {business['Name']} ({index+1}/{len(business_data)})")
                
                # Open website in new tab
                driver.execute_script(f"window.open('{business['Website']}');")
                driver.switch_to.window(driver.window_handles[-1])
                time.sleep(5)  # Wait for page to load
                
                # Extract tech stack
                tech_stack = detect_tech_stack(driver)
                if tech_stack:
                    business['TechStack'] = tech_stack
                
                # Detect payment methods if it's an e-commerce site
                if tech_stack and tech_stack.get('EcommercePlatform'):
                    payment_methods = detect_payment_methods(driver)
                    if payment_methods:
                        business['PaymentMethods'] = payment_methods
                
                # Close the tab and switch back to main window
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error checking website for {business['Name']}: {e}")
                # Make sure we're back to the main window
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
    
    return business_data

def generate_search_queries(counties, categories):
    """Generate all combinations of county × category search queries"""
    return [f"{category} in {county}, UK" for county in counties for category in categories]

def scrape_all_combinations(driver, counties, categories, scrape_function):
    """Iterate through all county×category combinations and scrape"""
    search_queries = generate_search_queries(counties, categories)
    total_queries = len(search_queries)
    
    for i, query in enumerate(search_queries, 1):
        try:
            logging.info(f"Processing query {i}/{total_queries}: {query}")
            scrape_function(driver, query)
        except Exception as e:
            logging.error(f"Error processing query '{query}': {str(e)}")
            continue

import os
import zipfile
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def create_proxy_extension(proxy_host, proxy_port, proxy_user, proxy_pass):
    """Create a Chrome proxy extension ZIP file"""
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Oxylabs Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
        mode: "fixed_servers",
        rules: {
            singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
            },
            bypassList: ["localhost"]
        }
    };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
        callbackFn,
        {urls: ["<all_urls>"]},
        ['blocking']
    );
    """ % (proxy_host, proxy_port, proxy_user, proxy_pass)

    extension_dir = 'proxy_extension'
    if not os.path.exists(extension_dir):
        os.makedirs(extension_dir)

    with open(os.path.join(extension_dir, "manifest.json"), "w") as f:
        f.write(manifest_json)
    with open(os.path.join(extension_dir, "background.js"), "w") as f:
        f.write(background_js)

    proxy_extension_path = 'oxylabs_proxy_auth.zip'
    with zipfile.ZipFile(proxy_extension_path, 'w') as zp:
        zp.write(os.path.join(extension_dir, "manifest.json"), "manifest.json")
        zp.write(os.path.join(extension_dir, "background.js"), "background.js")

    return proxy_extension_path

import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging

def init_driver_with_proxy():
    options = Options()
    
    # Essential for UTM/Windows on Mac
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    
    # Disable problematic features
    options.add_argument('--disable-features=VoiceTranscription')
    options.add_argument('--disable-component-update')
    options.add_argument('--disable-logging')
    
    # Proxy configuration
    proxy_extension = create_proxy_extension(
        proxy_host, proxy_port, proxy_user, proxy_pass
    )
    options.add_extension(proxy_extension)
    
    # Headless configuration
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    
    try:
        service = Service(executable_path='/usr/local/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(5)
        return driver
    except Exception as e:
        logging.error(f"Driver initialization failed: {e}")
        raise

uk_counties = [
    "bedfordshire", "berkshire", "bristol", "buckinghamshire", "cambridgeshire",
    "cheshire", "cornwall", "county durham", "cumbria", "derbyshire",
    "devon", "dorset", "east riding of yorkshire", "east sussex", "essex",
    "gloucestershire", "greater manchester", "hampshire", "herefordshire",
    "hertfordshire", "isle of wight", "kent", "lancashire", "leicestershire",
    "lincolnshire", "merseyside", "norfolk", "north yorkshire",
    "northamptonshire", "northumberland", "nottinghamshire", "oxfordshire",
    "rutland", "shropshire", "somerset", "south yorkshire", "staffordshire",
    "suffolk", "surrey", "tyne and wear", "warwickshire", "west midlands",
    "west sussex", "west yorkshire", "wiltshire", "worcestershire",
    "flintshire", "gwynedd", "anglesey", "conwy", "denbighshire",
    "wrexham", "ceredigion", "pembrokeshire", "carmarthenshire",
    "powys", "monmouthshire", "blaenau gwent", "bridgend", "caerphilly",
    "merthyr tydfil", "neath port talbot", "newport", "rhondda cynon taf",
    "swansea", "torfaen", "vale of glamorgan", "aberdeenshire",
    "angus", "argyll and bute", "ayrshire", "clackmannanshire",
    "dumfries and galloway", "dunbartonshire", "east lothian", "fife",
    "inverness-shire", "kincardineshire", "lanarkshire", "midlothian",
    "moray", "nairnshire", "orkney", "perthshire", "renfrewshire",
    "ross-shire", "roxburghshire", "shetland", "stirlingshire",
    "sutherland", "west lothian", "wigtownshire"
]

categories = [
    "general clothing", "mens wear", "women's wear", "children's wear", 
    "bridal wear", "luxury brands", "outdoor clothing", "boutique", 
    "vintage", "cosmetics store", "perfume store", "health supplies", 
    "beauty supplies", "food drink retailer", "specialty retailers", 
    "pet store", "souvenir gift store", "jewellery store", "florist", 
    "art gallery", "antique store", "adult store", "kids store", 
    "alcohol tobacco store", "lighting", "designer home store", 
    "department store", "home decorations", "furniture store", 
    "appliance store", "stationery store", "home supplies", 
    "garden supplies", "hardware supplies", "bookstore", 
    "magazine store", "toy and game store", "music store", 
    "arts and crafts store", "sporting goods store", "sportswear store", 
    "gym equipment store", "bicycle store", "mobile phone store", 
    "computer store", "video game store"
]

def main():
    # Initialize driver with proxy
    driver = init_driver_with_proxy()
    
    try:
        # Scrape all county×category combinations
        scrape_all_combinations(
            driver=driver,
            counties=uk_counties,
            categories=categories,
            scrape_function=search_query  # Your existing function
        )
        
    except Exception as e:
        logging.error(f"Error in main function: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
