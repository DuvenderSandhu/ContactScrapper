# scraper.py

import json
from typing import List
from pydantic import BaseModel, create_model
from assets import (OPENAI_MODEL_FULLNAME,GEMINI_MODEL_FULLNAME,SYSTEM_MESSAGE)
from llm_calls import (call_llm_model,clean_html_from_string)
from markdown import read_raw_data
from api_management import get_supabase_client
from utils import  generate_unique_name
import re

from bs4 import BeautifulSoup
supabase = get_supabase_client()

def create_dynamic_listing_model(field_names: List[str]):
    field_definitions = {field: (str, ...) for field in field_names}
    return create_model('DynamicListingModel', **field_definitions)

def create_listings_container_model(listing_model: BaseModel):
    return create_model('DynamicListingsContainer', listings=(List[listing_model], ...))

def generate_system_message(listing_model: BaseModel) -> str:
    # same logic as your code
    schema_info = listing_model.model_json_schema()
    field_descriptions = []
    for field_name, field_info in schema_info["properties"].items():
        field_type = field_info["type"]
        field_descriptions.append(f'"{field_name}": "{field_type}"')

    schema_structure = ",\n".join(field_descriptions)

    final_prompt= SYSTEM_MESSAGE+"\n"+f"""strictly follows this schema:
    {{
       "listings": [
         {{
           {schema_structure}
         }}
       ]
    }}
    """

    return final_prompt


def save_formatted_data(unique_name: str, formatted_data):
    if isinstance(formatted_data, str):
        try:
            data_json = json.loads(formatted_data)
        except json.JSONDecodeError:
            data_json = {"raw_text": formatted_data}
    elif hasattr(formatted_data, "dict"):
        data_json = formatted_data.dict()
    else:
        data_json = formatted_data

    supabase.table("scraped_data").update({
        "formatted_data": data_json
    }).eq("unique_name", unique_name).execute()
    MAGENTA = "\033[35m"
    RESET = "\033[0m"  # Reset color to default
    print(f"{MAGENTA}INFO:Scraped data saved for {unique_name}{RESET}")

def scrape_urls(unique_names: List[str], fields: List[str], selected_model: str):
    """
    For each unique_name:
      1) read raw_data from supabase
      2) parse with selected LLM
      3) save formatted_data
      4) accumulate cost
    Return total usage + list of final parsed data
    """
    total_input_tokens = 0
    total_output_tokens = 0
    total_cost = 0
    parsed_results = []

    DynamicListingModel = create_dynamic_listing_model(fields)
    DynamicListingsContainer = create_listings_container_model(DynamicListingModel)

    for uniq in unique_names:
        raw_data = read_raw_data(uniq)
        if not raw_data:
            BLUE = "\033[34m"
            RESET = "\033[0m"
            print(f"{BLUE}No raw_data found for {uniq}, skipping.{RESET}")
            continue
        print("Selected Model",selected_model,"System Message is ",SYSTEM_MESSAGE)
        parsed, token_counts, cost = call_llm_model(raw_data, DynamicListingsContainer, selected_model, SYSTEM_MESSAGE)

        # store
        save_formatted_data(uniq, parsed)

        total_input_tokens += token_counts["input_tokens"]
        total_output_tokens += token_counts["output_tokens"]
        total_cost += cost
        parsed_results.append({"unique_name": uniq,"parsed_data": parsed})

    return total_input_tokens, total_output_tokens, total_cost, parsed_results


import re
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

# Predefined regex patterns
regex_patterns = {
    'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    'mobile number': r'(\+?\d{1,4}[\s.-]?)?(\(?\d{1,4}\)?[\s.-]?)?[\d\s.-]{7,15}',
    'dob': r'(0[1-9]|1[0-9]|2[0-9]|3[01])[-/](0[1-9]|1[0-2])[-/](19|20)\d\d',
    'name': r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\b',
    'password': r'(?i)(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[!@#$%^&*()_+])[A-Za-z\d!@#$%^&*()_+]{8,}',
}

from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import re

def extract_data_from_html(
    html_content: str,
    fields: List[str],
    css_selectors: Optional[Dict[str, str]] = None
) -> List[Dict[str, object]]:
    """
    Extracts data from HTML content using CSS selectors and regex fallbacks, returning a structured list of data.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    extracted_data: Dict[str, List[str]] = {}
    
    # Predefined regex patterns for common fields
    default_regex = {
        'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        'mobile number': r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        'phone': r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        'dob': r'\b\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}\b',
        'date of birth': r'\b\d{1,2}[\/-]\d{1,2}[\/-]\d{2,4}\b',
        'name': r'(?i)\b[A-Z][a-z]+(?: [A-Z][a-z]+)+\b'  # Basic name pattern
    }
    
    for field in fields:
        values = []
        if css_selectors and field in css_selectors:
            # Extract using CSS selector
            elements = soup.select(css_selectors[field])
            values = [
                element.get_text(strip=True)
                for element in elements
                if element.get_text(strip=True)
            ]
        else:
            # Attempt regex extraction based on field name
            regex_key = next(
                (key for key in default_regex.keys() if key.lower() == field.lower()),
                None
            )
            if regex_key:
                text = soup.get_text()
                matches = re.findall(default_regex[regex_key], text, flags=re.IGNORECASE)
                values = [match.strip() for match in matches if match.strip()]
        
        # Ensure at least one entry (empty string if no data)
        extracted_data[field] = values if values else [""]
    
    # Determine max number of rows
    max_rows = max(len(v) for v in extracted_data.values()) if extracted_data else 0
    
    # Combine into rows
    combined_rows = []
    for i in range(max_rows):
        row = {field: extracted_data[field][i] if i < len(extracted_data[field]) else "" for field in fields}
        combined_rows.append(row)
    
    # Build final structure
    final_data = [{
        "unique_name": "combined_data",
        "parsed_data": {
            "listings": combined_rows
        }
    }]
    
    return final_data

def scrape_urls_manually(
    unique_names: List[str], 
    fields: List[str], 
    css_selectors: Optional[Dict[str, str]] = None
) -> List[Dict[str, object]]:
    """
    Scrapes and extracts data manually from stored HTML content.

    :param unique_names: List of unique identifiers for stored HTML.
    :param fields: List of fields to extract.
    :param css_selectors: Dictionary mapping fields to CSS selectors.
    :return: A combined list of extracted data.
    """
    complete_data = []
    
    for uniq in unique_names:
        raw_data = read_raw_data(uniq)  # Function to read raw HTML data
        if not raw_data:
            print(f"Skipping {uniq}, no raw data found.")
            continue

        # Extract data
        # raw_data= clean_html_from_string(raw_data)
        data = extract_data_from_html(raw_data, fields, css_selectors)
        complete_data.extend(data)  

    return complete_data  # Fixed return statement
