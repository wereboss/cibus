"""
Cibus: LLM-based Data Profiler

This module leverages a large language model (LLM) to infer data patterns
from statistical profiles. It is the "reasoning engine" of the Cibus agentic
pipeline.
"""

import json
from typing import Dict, Any, List

# To be used in the real implementation
# import google.generativeai as genai

# For isolated testing, we will use a mock response object
class MockGeminiResponse:
    def __init__(self, text_content: str):
        self.text = text_content

def infer_data_pattern(column_name: str, column_profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infers the data pattern of a column using an LLM.

    Args:
        column_name: The name of the column.
        column_profile: A dictionary containing the statistical profile of a column.

    Returns:
        A dictionary with the 'column_name', inferred 'pattern', and 'reasoning'.
    """
    # The system prompt instructs the LLM on its persona and role.
    system_prompt = """
    You are an expert data analyst. Your task is to analyze a statistical profile of a data column and infer its underlying data pattern. Your response MUST be a single JSON object with three keys: "column_name", "pattern" and "reasoning".
    
    The "pattern" must be one of the following categories: "SEQUENCE" (e.g., progressive IDs), "ENUM" (limited, repeated choices), "RANDOM_STRING", "DATE", "NUMBER", "BOOLEAN", "UNSTRUCTURED_TEXT", or "UNCLASSIFIED".
    The "reasoning" should be a concise, single-sentence explanation for your choice.
    
    Example output:
    {
      "column_name": "product_type",
      "pattern": "ENUM",
      "reasoning": "The column has a very low cardinality with a clear distribution of a few unique values."
    }
    """
    
    # The user prompt contains the actual data to be analyzed, now including the column name.
    user_prompt = f"""
    Please analyze the following column named '{column_name}' with the provided profile and provide your inference:
    {json.dumps(column_profile, indent=2)}
    """
    
    # In a real implementation, this would be the API call to the LLM.
    # We will simulate the call for this isolated test.
    # response = genai.generate_content(
    #     contents=[{"parts": [{"text": user_prompt}]}],
    #     system_instruction={"parts": [{"text": system_prompt}]},
    #     model="gemini-2.5-flash-preview-05-20"
    # )
    # mock_response = json.loads(response.text)
    
    # --- For Isolated Testing ---
    print(f"\n--- Simulating LLM Call for pattern inference for column: {column_name} ---")
    print("Sending the following profile to the LLM:")
    print(json.dumps(column_profile, indent=2))
    
    # Mocking the LLM's response based on the input
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
    else:
        mock_response = {
            "column_name": column_name,
            "pattern": "UNCLASSIFIED",
            "reasoning": "The column's properties do not fit a well-defined pattern category."
        }
        
    print("\nLLM Mock Response:")
    print(json.dumps(mock_response, indent=2))
    return mock_response

if __name__ == '__main__':
    # --- Dummy data for isolated testing ---
    print("--- Testing `llm_profiler.py` in isolation ---")
    
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

    # Test the function with the ENUM profile
    inferred_pattern_enum = infer_data_pattern(column_name="product_type", column_profile=product_type_profile)
    print("\nInferred Pattern for 'product_type':")
    print(inferred_pattern_enum)
    
    # Test the function with the SEQUENCE profile
    inferred_pattern_seq = infer_data_pattern(column_name="id", column_profile=id_profile)
    print("\nInferred Pattern for 'id':")
    print(inferred_pattern_seq)