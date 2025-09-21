"""
Cibus: Synthetic Data Generator

This module provides a function to generate synthetic data based on a learned
data profile. It is a key component of the Cibus MVP's second phase.
"""

import random
import string
from typing import Dict, Any, List

def generate_synthetic_data(profile: Dict[str, Any], volume: int) -> List[Dict[str, str]]:
    """
    Generates synthetic data records based on a given profile.

    Args:
        profile: A dictionary containing the profile of each column, including
                 inferred patterns and statistical properties.
        volume: The number of synthetic records to generate.

    Returns:
        A list of dictionaries, where each dictionary represents a synthetic record.
    """
    generated_data = []

    # Get column order from the profile keys
    columns = list(profile.keys())
    
    # Store state for sequence generation
    sequence_counters = {col: 0 for col, stats in profile.items() if stats.get('pattern') == 'SEQUENCE'}
    
    # Pre-calculate unique values for ENUMs to sample from
    enum_values = {
        col: list(stats.get('value_counts', {}).keys())
        for col, stats in profile.items()
        if stats.get('pattern') == 'ENUM'
    }

    for i in range(volume):
        new_record = {}
        for col in columns:
            stats = profile.get(col, {})
            pattern = stats.get('pattern')

            # Handle different patterns
            if pattern == 'ENUM':
                # Randomly choose from unique values based on distribution
                choices = enum_values.get(col, [''])
                new_record[col] = random.choice(choices)
                
            elif pattern == 'SEQUENCE':
                # Generate a new sequential value based on the original format
                current_count = sequence_counters[col] + 1
                length = stats.get('max_length', 0)
                # This simple logic assumes a numeric sequence.
                # In a more advanced version, we'd handle prefixes/suffixes.
                new_record[col] = str(current_count).zfill(length)
                sequence_counters[col] = current_count
                
            elif pattern == 'NUMBER':
                # Generate a random number within the observed length range
                min_len = stats.get('min_length', 1)
                max_len = stats.get('max_length', 10)
                length = random.randint(min_len, max_len)
                new_record[col] = ''.join(random.choices(string.digits, k=length))
                
            elif pattern == 'DATE':
                # Generate a random date (simplified for this example)
                year = random.randint(2000, 2025)
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                new_record[col] = f"{month:02d}{day:02d}{year}"
                
            else: # Covers 'UNCLASSIFIED' and 'RANDOM_STRING'
                # Generate a random string of a fixed or variable length
                min_len = stats.get('min_length', 1)
                max_len = stats.get('max_length', 10)
                length = random.randint(min_len, max_len)
                new_record[col] = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length)).strip()

        generated_data.append(new_record)

    return generated_data

if __name__ == '__main__':
    # --- Dummy data and isolated testing ---
    print("--- Testing `data_generator.py` in isolation ---")

    # A dummy profile that mimics the output of our agentic pipeline
    dummy_profile = {
        'id': {'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 8, 'max_length': 8, 'column_name': 'id', 'pattern': 'SEQUENCE', 'reasoning': 'Sequential identifier.'},
        'product_code': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 10, 'max_length': 10, 'column_name': 'product_code', 'pattern': 'ENUM', 'value_counts': {'PROD-A0001': 3, 'PROD-B0002': 1, 'PROD-C0003': 1}},
        'product_type': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 9, 'max_length': 10, 'column_name': 'product_type', 'pattern': 'ENUM', 'value_counts': {'FURNITURE': 2, 'APPLIANCE': 2, 'ELECTRONIC': 1}},
        'price': {'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 6, 'max_length': 7, 'column_name': 'price', 'pattern': 'NUMBER'},
        'currency': {'total_count': 5, 'unique_count': 2, 'cardinality': 0.4, 'min_length': 3, 'max_length': 3, 'column_name': 'currency', 'pattern': 'ENUM', 'value_counts': {'USD': 3, 'EUR': 2}},
        'status': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 8, 'max_length': 8, 'column_name': 'status', 'pattern': 'ENUM', 'value_counts': {'STATUS-A': 2, 'STATUS-B': 2, 'STATUS-C': 1}},
        'order_date': {'total_count': 5, 'unique_count': 1, 'cardinality': 0.2, 'min_length': 8, 'max_length': 8, 'column_name': 'order_date', 'pattern': 'DATE'},
        'customer_id': {'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 12, 'max_length': 12, 'column_name': 'customer_id', 'pattern': 'SEQUENCE'},
        'zip_code': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 5, 'max_length': 5, 'column_name': 'zip_code', 'pattern': 'NUMBER'},
        'quantity': {'total_count': 5, 'unique_count': 4, 'cardinality': 0.8, 'min_length': 4, 'max_length': 4, 'column_name': 'quantity', 'pattern': 'NUMBER'}
    }

    # Generate a sample of 10 synthetic records
    generated_records = generate_synthetic_data(dummy_profile, 10)

    # Print a sample of the generated data for verification
    print("\nSample of 10 Generated Records:")
    for record in generated_records:
        print(record)

    # Verify the count
    print(f"\nTotal records generated: {len(generated_records)}")