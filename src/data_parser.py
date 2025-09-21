"""
Cibus: Fixed-Length Data Parser

This module provides a function to parse a fixed-length data file
based on a given layout. It is a critical component for converting
raw mainframe handoffs into a structured, workable format.
"""

from typing import List, Dict, Tuple

def parse_fixed_length_data(data_content: str, layout: List[Tuple[str, int]]) -> List[Dict[str, str]]:
    """
    Parses a fixed-length data string based on a provided layout.

    Args:
        data_content: A string containing the entire fixed-length data file content.
        layout: A list of tuples, where each tuple contains the field name (str)
                and its length (int).

    Returns:
        A list of dictionaries, where each dictionary represents a row
        with column names as keys and field values as strings.
    """
    parsed_data = []
    
    # Calculate the total line length based on the layout
    record_length = sum(length for _, length in layout)
    
    # Split the data content into individual lines, handling potential
    # empty lines at the start or end of the file.
    lines = [line for line in data_content.split('\n') if line.strip()]
    
    for line in lines:
        if len(line) != record_length:
            print(f"Warning: Skipping line due to incorrect length. Expected {record_length}, got {len(line)}.")
            continue

        row = {}
        current_pos = 0
        
        # Iterate through the layout to extract each field's value
        for field_name, field_length in layout:
            # Strip whitespace from the extracted value
            field_value = line[current_pos : current_pos + field_length].strip()
            row[field_name] = field_value
            current_pos += field_length
            
        parsed_data.append(row)
        
    return parsed_data

if __name__ == '__main__':
    # --- Dummy data and layout for isolated testing ---
    print("--- Testing `data_parser.py` in isolation ---")
    
    # Sample layout (matching the MVP's 10-column, 2-variance-column scope)
    # The layout is a list of tuples: (field_name, field_length)
    sample_layout = [
        ("id", 8),
        ("product_code", 10),
        ("product_type", 15),
        ("price", 7),
        ("currency", 3),
        ("status", 10),
        ("order_date", 8),
        ("customer_id", 12),
        ("zip_code", 5),
        ("quantity", 4)
    ]
    
    # Sample fixed-length data as a list of strings for reliability.
    # Each line is now exactly 82 characters long, matching the layout.
    sample_data_lines = [
        "00001234PROD-A001 FURNITURE     120.00 USDSTATUS-A  02022023CUST0000000001234567890123",
        "00001235PROD-B002 APPLIANCE     2500.50EURSTATUS-B  02022023CUST000000000234567890045",
        "00001236PROD-A001 ELECTRONIC    999.99 USDSTATUS-C  02022023CUST000000000345678901267",
        "00001237PROD-C003 APPLIANCE     550.00 EURSTATUS-B  02022023CUST000000000456789012015",
        "00001238PROD-A001 FURNITURE     150.00 USDSTATUS-A  02022023CUST000000000567890123101"
    ]
    sample_data = "\n".join(sample_data_lines)

    # Parse the sample data using the function
    parsed_output = parse_fixed_length_data(sample_data, sample_layout)

    # Print the results for verification
    print("\nParsed Data (List of Dictionaries):")
    for row in parsed_output:
        print(row)
        
    # Example of accessing a specific value for a record
    if len(parsed_output) >= 3:
        print(f"\nExample: 'product_type' for the third record is: {parsed_output[2]['product_type']}")
    else:
        print("\nCould not access the third record, parsed output has fewer than 3 records.")