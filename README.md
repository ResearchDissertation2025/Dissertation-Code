# 🗺️ Google Maps Business Scraper

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Selenium](https://img.shields.io/badge/selenium-4.0+-green.svg)
![License](https://img.shields.io/badge/license-MIT-orange.svg)

A powerful Python scraper that extracts business listings from Google Maps across all UK counties and retail categories.

## 🌟 Features

- **Comprehensive UK Coverage**  
  Scrapes businesses across all UK counties (excluding Greater London)
- **45+ Retail Categories**  
  Fashion, electronics, homeware, sporting goods, and more
- **Advanced Data Extraction**:
  - Basic contact info (name, address, phone)
  - Website/email extraction
  - Technology stack detection
  - Payment method analysis
- **Residential Proxy Support**  
  Built-in Oxylabs integration with authentication
- **Smart Organization**  
  Auto-generated CSV files per search query

## ⚙️ Installation

### Prerequisites
- Python 3.8+
- Chrome browser
- ChromeDriver (matching your Chrome version)

```bash
# Clone repository
git clone https://github.com/yourusername/google-maps-scraper.git
```


# Install dependencies
```bash
pip install -r requirements.txt
```

```bash
PROXY_CONFIG = {
    "host": "pr.oxylabs.io",
    "port": "7777",
    "username": "your-username",
    "password": "your-password"
}
```
🚀 Usage Instructions
Basic Execution
```bash
python scrapper.py
```
📂 Output Structure
The scraper generates organized CSV files with this naming convention:
```bash
{category}-in-{county}-uk.csv
```
## 📊 Complete Data Schema

| Column             | Type        | Description                                  |
|--------------------|-------------|----------------------------------------------|
| `business_name`    | String      | Official business name                       |
| `google_rating`    | Float       | Rating (1-5 stars)                           |
| `review_count`     | Integer     | Number of reviews                            |
| `full_address`     | String      | Complete business address                    |
| `primary_phone`    | String      | Formatted phone number (+44 format)          |
| `website_url`      | String      | Business website URL                         |
| `contact_email`    | String      | Extracted from website                       |
| `tech_stack`       | JSON        | Detected technologies (CMS, eCommerce etc.)  |
| `payment_methods`  | JSON        | Available payment options                    |
| `search_category`  | String      | Original search category used                |
| `search_county`    | String      | Original search county used                  |
