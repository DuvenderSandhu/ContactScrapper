# scraper.py

import json
from typing import List
from pydantic import BaseModel, create_model
from assets import (OPENAI_MODEL_FULLNAME,GEMINI_MODEL_FULLNAME,SYSTEM_MESSAGE)
from llm_calls import (call_llm_model)
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

def extract_data_from_html(
    html_content: str,
    fields: List[str],
    css_selectors: Optional[Dict[str, str]] = None
) -> List[Dict[str, object]]:
    """
    This function extracts data from HTML content using either CSS selectors or regular expressions.
    It groups multiple matches under a "listings" key.
    
    :param html_content: The HTML content to extract data from.
    :param fields: A list of fields to extract (either predefined or custom fields).
    :param css_selectors: A dictionary where keys are field names and values are CSS selectors.
    :return: A list of dictionaries containing extracted data for each field.
    """
    # Allow a single field as a string
    if isinstance(fields, str):
        fields = [fields]

    # This dictionary will group results by field name.
    grouped_data: Dict[str, List[object]] = {}
    soup = BeautifulSoup(html_content, 'html.parser')

    for field in fields:
        extracted_values = []  # Temporarily store all matches for this field

        # Case 1: Use CSS selector if provided for this field
        if css_selectors and field in css_selectors:
            css_selector = css_selectors.get(field)
            elements = soup.select(css_selector)
            for element in elements:
                if element:
                    # Extract text content
                    data_item = element.get_text(strip=True)
                    extracted_values.append(data_item)

        # Case 2: Use regex pattern if defined for the field
        elif field in regex_patterns:
            matched_data = re.findall(regex_patterns[field], soup.get_text())
            for data_item in matched_data:
                # Attempt to parse JSON-like strings if applicable
                if isinstance(data_item, str):
                    try:
                        parsed = json.loads(data_item)
                        extracted_values.append(parsed)
                    except json.JSONDecodeError:
                        extracted_values.append(data_item)
                else:
                    extracted_values.append(data_item)

        # Only add the field if we found at least one value.
        if extracted_values:
            grouped_data[field] = extracted_values

    # Now, convert the grouped data into the desired list of dictionaries.
    all_data = []
    for field, values in grouped_data.items():
        if len(values) > 1:
            data_entry = {
                "unique_name": field,
                "parsed_data": {
                    "listings": [{field: value} for value in values]  # Fixing the structure
                }
            }
        else:
            data_entry = {"unique_name": field, "parsed_data": {field: values[0]}}  # Ensuring consistency for single items

    all_data.append(data_entry)


    return all_data


def scrape_urls_manually(unique_names: List[str], field: List[str], css_selectors: Optional[Dict[str, str]] = None) -> List[Dict[str, object]]:
    completeData = []
    for uniq in unique_names:
        raw_data = read_raw_data(uniq)  # Function to read raw HTML data
        if not raw_data:
            print(f"Skipping {uniq}, no raw data found.")
            continue

        # Extract data using the extract_data_from_html function
        data = extract_data_from_html(raw_data, field, css_selectors)
        completeData.extend(data)  # Use extend to add each dict to the list
    return completeData
