"""
Cibus: Synthetic Data Generator

This module provides a function to generate synthetic data based on a learned
data profile. It is a key component of the Cibus MVP's second phase.
"""

import random
import string
from typing import Dict, Any, List
from datetime import datetime, timedelta

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
                guidelines = stats.get('generation_guidelines', {})
                number_type = guidelines.get('number_type', 'INTEGER')
                min_value = guidelines.get('min_value', 0)
                max_value = guidelines.get('max_value', 100)
                
                if number_type == 'DECIMAL':
                    decimal_places = guidelines.get('decimal_places', 2)
                    # Generate a random decimal number
                    generated_value = random.uniform(min_value, max_value)
                    new_record[col] = f"{generated_value:.{decimal_places}f}"
                else: # Covers 'INTEGER' and 'STRING_OF_DIGITS'
                    # Generate a random integer
                    generated_value = random.randint(int(min_value), int(max_value))
                    
                    if number_type == 'STRING_OF_DIGITS':
                        # Pad with leading zeros to match original length
                        length = stats.get('max_length', len(str(generated_value)))
                        new_record[col] = str(generated_value).zfill(length)
                    else:
                        new_record[col] = str(generated_value)

            elif pattern == 'DATE':
                guidelines = stats.get('generation_guidelines', {})
                date_format = guidelines.get('date_format', "%Y%m%d")
                min_date_str = guidelines.get('min_date', '20000101')
                max_date_str = guidelines.get('max_date', '20251231')

                try:
                    min_date = datetime.strptime(min_date_str, date_format.replace('%Y', '%Y').replace('%m', '%m').replace('%d', '%d'))
                    max_date = datetime.strptime(max_date_str, date_format.replace('%Y', '%Y').replace('%m', '%m').replace('%d', '%d'))
                    
                    # Calculate the difference in days between the min and max date
                    days_diff = (max_date - min_date).days
                    
                    # Generate a random number of days to add to the min date
                    random_days = random.randint(0, days_diff)
                    
                    # Generate the new random date
                    random_date = min_date + timedelta(days=random_days)
                    
                    new_record[col] = random_date.strftime(date_format)
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not generate date for column '{col}'. Error: {e}. Falling back to default.")
                    new_record[col] = "00000000" # Fallback value
                
            else: # Covers 'UNCLASSIFIED' and 'RANDOM_STRING'
                # Generate a random string of a fixed or variable length
                min_len = stats.get('min_length', 1)
                max_len = stats.get('max_length', 10)
                length = random.randint(min_len, max_len)
                new_record[col] = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length)).strip()

        generated_data.append(new_record)

    return generated_data

if __name__ == '__main__':
    # --- Dummy data and isolated testing with rich NUMBER profile---
    print("--- Testing `data_generator.py` in isolation ---")

    # A dummy profile with a new 'generation_guidelines' for NUMBER and DATE columns
    dummy_profile = {
        'id': {'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 8, 'max_length': 8, 'column_name': 'id', 'pattern': 'SEQUENCE', 'reasoning': 'Sequential identifier.'},
        'product_code': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 10, 'max_length': 10, 'column_name': 'product_code', 'pattern': 'ENUM', 'value_counts': {'PROD-A0001': 3, 'PROD-B0002': 1, 'PROD-C0003': 1}},
        'product_type': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 9, 'max_length': 10, 'column_name': 'product_type', 'pattern': 'ENUM', 'value_counts': {'FURNITURE': 2, 'APPLIANCE': 2, 'ELECTRONIC': 1}},
        'price': {
            'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 6, 'max_length': 7, 
            'column_name': 'price', 'pattern': 'NUMBER',
            'generation_guidelines': {
                'number_type': 'DECIMAL',
                'min_value': 100.0,
                'max_value': 10000.0,
                'decimal_places': 2
            }
        },
        'currency': {'total_count': 5, 'unique_count': 2, 'cardinality': 0.4, 'min_length': 3, 'max_length': 3, 'column_name': 'currency', 'pattern': 'ENUM', 'value_counts': {'USD': 3, 'EUR': 2}},
        'status': {'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 8, 'max_length': 8, 'column_name': 'status', 'pattern': 'ENUM', 'value_counts': {'STATUS-A': 2, 'STATUS-B': 2, 'STATUS-C': 1}},
        'order_date': {
            'total_count': 5, 'unique_count': 1, 'cardinality': 0.2, 'min_length': 8, 'max_length': 8,
            'column_name': 'order_date', 'pattern': 'DATE',
            'generation_guidelines': {
                'date_format': "%m%d%Y",
                'min_date': "01012023",
                'max_date': "12312023"
            }
        },
        'customer_id': {'total_count': 5, 'unique_count': 5, 'cardinality': 1.0, 'min_length': 12, 'max_length': 12, 'column_name': 'customer_id', 'pattern': 'SEQUENCE'},
        'zip_code': {
            'total_count': 5, 'unique_count': 3, 'cardinality': 0.6, 'min_length': 5, 'max_length': 5,
            'column_name': 'zip_code', 'pattern': 'NUMBER',
            'generation_guidelines': {
                'number_type': 'STRING_OF_DIGITS',
                'min_value': 10000,
                'max_value': 99999,
                'decimal_places': 0
            }
        },
        'quantity': {
            'total_count': 5, 'unique_count': 4, 'cardinality': 0.8, 'min_length': 4, 'max_length': 4,
            'column_name': 'quantity', 'pattern': 'NUMBER',
            'generation_guidelines': {
                'number_type': 'INTEGER',
                'min_value': 1,
                'max_value': 100,
                'decimal_places': 0
            }
        }
    }

    # Generate a sample of 10 synthetic records
    generated_records = generate_synthetic_data(dummy_profile, 10)

    # Print a sample of the generated data for verification
    print("\nSample of 10 Generated Records:")
    for record in generated_records:
        print(record)

    # Verify the count
    print(f"\nTotal records generated: {len(generated_records)}")