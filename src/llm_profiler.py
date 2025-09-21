"""
Cibus: LLM-based Data Profiler

This module leverages a large language model (LLM) to infer data patterns
from statistical profiles. It is the "reasoning engine" of the Cibus agentic
pipeline.
"""

import json
import os
from typing import Dict, Any, List
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# --- API Configuration ---
API_KEY = os.getenv('GEMINI_API_KEY')
if API_KEY:
    genai.configure(api_key=API_KEY)

def infer_data_pattern(column_name: str, column_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infers the data pattern of a column using an LLM.

    Args:
        column_name: The name of the column.
        column_profile: A dictionary containing the statistical profile of a column.

    Returns:
        A dictionary with the 'column_name', inferred 'pattern', and 'reasoning'.
    """
    if not API_KEY:
        print("\n--- WARNING: API Key not found in environment. Using mock response. ---")
        # Fallback to mock response if API key is not set
        if column_name.lower() == "id" or (column_profile.get("unique_count") == column_profile.get("total_count") and column_profile.get("min_length") == 8 and column_profile.get("max_length") == 8):
            mock_response = {
                "column_name": column_name,
                "pattern": "SEQUENCE",
                "reasoning": "The column has a unique value for every record, suggesting a progressive ID or sequence."
            }
        elif column_name.lower() in ["product_type", "currency", "status"] or (column_profile.get("unique_count") < 5 and "value_counts" in column_profile):
            mock_response = {
                "column_name": column_name,
                "pattern": "ENUM",
                "reasoning": "The column has a very low cardinality with a few unique values and a clear value distribution."
            }
        elif column_name.lower() in ["price", "quantity", "zip_code"]:
            mock_response = {
                "column_name": column_name,
                "pattern": "NUMBER",
                "reasoning": "The values are strictly numerical.",
                "generation_guidelines": {
                    "number_type": "DECIMAL" if column_name.lower() == "price" else "INTEGER",
                    "min_value": 37.00 if column_name.lower() == "price" else 1,
                    "max_value": 12036.50 if column_name.lower() == "price" else 1000,
                    "decimal_places": 2 if column_name.lower() == "price" else 0
                }
            }
        elif column_name.lower() in ["order_date"]:
            mock_response = {
                "column_name": column_name,
                "pattern": "DATE",
                "reasoning": "The column contains a single, fixed-length date.",
                "generation_guidelines": {
                    "date_format": "MMDDYYYY",
                    "min_date": "01012023",
                    "max_date": "12312023"
                }
            }
        else:
            mock_response = {
                "column_name": column_name,
                "pattern": "UNCLASSIFIED",
                "reasoning": "The column's properties do not fit a well-defined pattern category."
            }
            
        print("\nLLM Mock Response:")
        print(json.dumps(mock_response, indent=2))
        return mock_response

    # --- LLM Integration ---
    # The system prompt is updated to request more detail for 'DATE' patterns.
    system_prompt = """
    You are an expert data analyst. Your task is to analyze a statistical profile of a data column and infer its underlying data pattern. Your response MUST be a single JSON object with three keys: "column_name", "pattern", and "reasoning".
    
    The "pattern" must be one of the following categories: "SEQUENCE" (e.g., progressive IDs), "ENUM" (limited, repeated choices), "RANDOM_STRING", "DATE", "NUMBER", "BOOLEAN", "UNSTRUCTURED_TEXT", or "UNCLASSIFIED".
    The "reasoning" should be a concise, single-sentence explanation for your choice.
    
    If the pattern is "NUMBER", you MUST also include a JSON object with the key "generation_guidelines". This object must contain:
    - "number_type": one of "DECIMAL", "INTEGER", or "STRING_OF_DIGITS"
    - "min_value": the minimum value observed.
    - "max_value": the maximum value observed.
    - "decimal_places": required for "DECIMAL" type, the number of digits after the decimal point.

    If the pattern is "DATE", you MUST also include a JSON object with the key "generation_guidelines". This object must contain:
    - "date_format": a Python strftime format string (e.g., "%Y-%m-%d").
    - "min_date": the earliest date observed, in the same format.
    - "max_date": the latest date observed, in the same format.
    
    Example output for a NUMBER column:
    {
      "column_name": "price",
      "pattern": "NUMBER",
      "reasoning": "The values are all numeric and formatted as decimal numbers, consistent with price values.",
      "generation_guidelines": {
        "number_type": "DECIMAL",
        "min_value": 1.00,
        "max_value": 9999.99,
        "decimal_places": 2
      }
    }
    
    Example output for a DATE column:
    {
      "column_name": "order_date",
      "pattern": "DATE",
      "reasoning": "The column contains a limited set of fixed-length numeric values that appear to be dates.",
      "generation_guidelines": {
        "date_format": "%m%d%Y",
        "min_date": "01012023",
        "max_date": "03312023"
      }
    }
    """
    
    # The user prompt contains the actual data to be analyzed, now including the column name.
    user_prompt = f"""
    Please analyze the following column named '{column_name}' with the provided profile and provide your inference:
    {json.dumps(column_profile, indent=2)}
    """
    
    print(f"\n--- Calling Gemini API for pattern inference for column: {column_name} ---")
    try:
        # Use the requested model
        model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')
        
        response = model.generate_content(
            contents=[
                {"role": "user", "parts": [{"text": system_prompt}]},
                {"role": "user", "parts": [{"text": user_prompt}]}
            ],
            generation_config={"response_mime_type": "application/json"}
        )
        
        # The LLM responds with a JSON string, which we need to parse
        response_json = json.loads(response.text)
        print("\nGemini Response:")
        print(json.dumps(response_json, indent=2))
        return response_json
        
    except Exception as e:
        print(f"\n--- ERROR: Failed to get response from Gemini API. Error: {e} ---")
        return {
            "column_name": column_name,
            "pattern": "UNCLASSIFIED",
            "reasoning": "Failed to call LLM due to an API error."
        }


if __name__ == '__main__':
    # --- Instructions for running this test ---
    print("--- Testing `llm_profiler.py` in isolation ---")
    print("Please follow these steps to run the test:")
    print("1. Create a file named '.env' in your project root.")
    print("2. Add your Gemini API key to the .env file like this: GEMINI_API_KEY='your_api_key_here'")
    print("3. Ensure you have the python-dotenv library installed: pip install python-dotenv")

    # A dummy profile for an ENUM-like column (e.g., 'product_type')
    product_type_profile = {
        'total_count': 5,
        'unique_count': 3,
        'cardinality': 0.6,
        'value_counts': {'FURNITURE': 2, 'APPLIANCE': 2, 'ELECTRONIC': 1},
        'min_length': 9,
        'max_length': 10
    }
    
    # A dummy profile for a SEQUENCE-like column (e.g., 'id')
    id_profile = {
        'total_count': 5,
        'unique_count': 5,
        'cardinality': 1.0,
        'value_counts': {'00001234': 1, '00001235': 1, '00001236': 1, '00001237': 1, '00001238': 1},
        'min_length': 8,
        'max_length': 8
    }

    # A dummy profile for a DECIMAL-like NUMBER column (e.g., 'price')
    price_profile = {
        'total_count': 5,
        'unique_count': 5,
        'cardinality': 1.0,
        'value_counts': {'120.00': 1, '2500.50': 1, '999.99': 1, '550.00': 1, '150.00': 1},
        'min_length': 6,
        'max_length': 7
    }

    # A dummy profile for a DATE column
    date_profile = {
        'total_count': 5,
        'unique_count': 5,
        'cardinality': 1.0,
        'value_counts': {'22022024': 1, '21022024': 1, '13122023': 1, '18112023': 1, '02092023': 1},
        'min_length': 8,
        'max_length': 8
    }

    # Test the function with the ENUM profile
    inferred_pattern_enum = infer_data_pattern(column_name="product_type", column_profile=product_type_profile)
    print("\nInferred Pattern for 'product_type':")
    print(inferred_pattern_enum)
    
    # Test the function with the SEQUENCE profile
    inferred_pattern_seq = infer_data_pattern(column_name="id", column_profile=id_profile)
    print("\nInferred Pattern for 'id':")
    print(inferred_pattern_seq)

    # Test the function with the NUMBER profile
    inferred_pattern_num = infer_data_pattern(column_name="price", column_profile=price_profile)
    print("\nInferred Pattern for 'price':")
    print(inferred_pattern_num)
    
    # Test the function with the DATE profile
    inferred_pattern_date = infer_data_pattern(column_name="order_date", column_profile=date_profile)
    print("\nInferred Pattern for 'order_date':")
    print(inferred_pattern_date)