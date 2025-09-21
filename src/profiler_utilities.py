"""
Cibus: Data Profiling Utilities

This module provides functions to perform statistical analysis on structured
data, calculating metrics such as unique value counts, frequency distributions,
and string lengths. This information serves as the input for the LLM-based
pattern inference agent.
"""

import pandas as pd
from typing import List, Dict, Any

def profile_data(data: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Performs a simple statistical profile on structured data.

    Args:
        data: A list of dictionaries, where each dictionary is a data record.

    Returns:
        A dictionary where each key is a column name and the value is a
        dictionary containing the profile statistics for that column.
    """
    if not data:
        return {}
        
    # Convert list of dicts to pandas DataFrame for easy profiling
    df = pd.DataFrame(data)

    profile = {}
    for col in df.columns:
        series = df[col]
        
        # Calculate statistical properties
        total_count = series.count()
        unique_values = series.unique()
        unique_count = len(unique_values)
        
        # Avoid division by zero if there are no records
        cardinality = unique_count / total_count if total_count > 0 else 0
        
        # Calculate value counts for distribution, useful for ENUM/limited choice fields
        value_counts = series.value_counts().to_dict()
        
        # Calculate min/max length if it's a string column
        if series.dtype == 'object':
            str_lengths = series.astype(str).str.len()
            min_length = str_lengths.min() if not str_lengths.empty else 0
            max_length = str_lengths.max() if not str_lengths.empty else 0
        else:
            min_length, max_length = None, None
            
        profile[col] = {
            'total_count': total_count,
            'unique_count': unique_count,
            'cardinality': cardinality,
            'value_counts': value_counts,
            'min_length': min_length,
            'max_length': max_length,
        }
        
    return profile

if __name__ == '__main__':
    # --- Dummy data for isolated testing ---
    print("--- Testing `profiler_utilities.py` in isolation ---")
    
    # This dummy data should match the output format of data_parser.py
    sample_data = [
        {'id': '00001234', 'product_code': 'PROD-A001', 'product_type': 'FURNITURE', 'price': '120.00', 'currency': 'USD', 'status': 'STATUS-A', 'order_date': '02022023', 'customer_id': 'CUST000000', 'zip_code': '01234', 'quantity': '5678'},
        {'id': '00001235', 'product_code': 'PROD-B002', 'product_type': 'APPLIANCE', 'price': '2500.50', 'currency': 'EUR', 'status': 'STATUS-B', 'order_date': '02022023', 'customer_id': 'CUST000000', 'zip_code': '02345', 'quantity': '6789'},
        {'id': '00001236', 'product_code': 'PROD-A001', 'product_type': 'ELECTRONIC', 'price': '999.99', 'currency': 'USD', 'status': 'STATUS-C', 'order_date': '02022023', 'customer_id': 'CUST000000', 'zip_code': '03456', 'quantity': '7890'},
        {'id': '00001237', 'product_code': 'PROD-C003', 'product_type': 'APPLIANCE', 'price': '550.00', 'currency': 'EUR', 'status': 'STATUS-B', 'order_date': '02022023', 'customer_id': 'CUST000000', 'zip_code': '04567', 'quantity': '8901'},
        {'id': '00001238', 'product_code': 'PROD-A001', 'product_type': 'FURNITURE', 'price': '150.00', 'currency': 'USD', 'status': 'STATUS-A', 'order_date': '02022023', 'customer_id': 'CUST000000', 'zip_code': '05678', 'quantity': '9012'}
    ]

    # Profile the sample data
    profiled_data = profile_data(sample_data)

    # Print the results
    print("\nData Profile:")
    for column, stats in profiled_data.items():
        print(f"\nColumn: {column}")
        for key, value in stats.items():
            print(f"  - {key}: {value}")