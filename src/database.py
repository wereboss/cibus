"""
Cibus: Profile Persistence Layer

This module handles all interactions with the SQLite database to save and
load data profiles and their inferred patterns.
"""

import sqlite3
import json
import os
from typing import Dict, Any, Optional

DB_PATH = 'cibus_profiles.db'

def init_db():
    """Initializes the SQLite database and creates the profiles table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create the profiles table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            layout_name TEXT PRIMARY KEY,
            profile_data TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def save_profile(layout_name: str, profile: Dict[str, Any]) -> None:
    """
    Saves a data profile to the database.

    Args:
        layout_name: A unique identifier for the layout.
        profile: The dictionary containing the full data profile.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Serialize the profile dictionary to a JSON string
    profile_json = json.dumps(profile)

    # Use a replace-or-insert to update the profile if it already exists
    cursor.execute("""
        INSERT OR REPLACE INTO profiles (layout_name, profile_data)
        VALUES (?, ?)
    """, (layout_name, profile_json))
    
    conn.commit()
    conn.close()
    print(f"Profile for layout '{layout_name}' saved to the database.")

def load_profile(layout_name: str) -> Optional[Dict[str, Any]]:
    """
    Loads a data profile from the database.

    Args:
        layout_name: The unique identifier for the layout.

    Returns:
        The data profile dictionary or None if not found.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT profile_data FROM profiles WHERE layout_name = ?", (layout_name,))
    row = cursor.fetchone()
    
    conn.close()
    
    if row:
        # Deserialize the JSON string back into a dictionary
        profile = json.loads(row[0])
        print(f"Profile for layout '{layout_name}' loaded from the database.")
        return profile
    
    print(f"Profile for layout '{layout_name}' not found.")
    return None

if __name__ == '__main__':
    # --- Dummy data and isolated testing ---
    print("--- Testing `database.py` in isolation ---")
    
    # 1. Initialize the database
    init_db()
    
    # 2. Create a dummy profile
    dummy_profile = {
        'id': {
            'total_count': 5,
            'unique_count': 5,
            'pattern': 'SEQUENCE',
            'reasoning': 'The column has a unique value for every record, suggesting a progressive ID or sequence.'
        },
        'product_type': {
            'total_count': 5,
            'unique_count': 3,
            'pattern': 'ENUM',
            'reasoning': 'The column has a very low cardinality with a few unique values and a clear value distribution.'
        }
    }
    dummy_layout_name = "test_layout_v1"
    
    try:
        # 3. Save the dummy profile
        print("\nAttempting to save profile...")
        save_profile(dummy_layout_name, dummy_profile)
        
        # 4. Load the profile to verify
        print("\nAttempting to load the saved profile...")
        loaded_profile = load_profile(dummy_layout_name)
        
        # 5. Verify the data
        if loaded_profile == dummy_profile:
            print("\nTest successful: Saved and loaded profiles match!")
        else:
            print("\nTest failed: Loaded profile does not match the saved profile.")
            print("Loaded:", loaded_profile)
            print("Original:", dummy_profile)
            
        # 6. Test a load for a non-existent profile
        print("\nAttempting to load a non-existent profile...")
        non_existent_profile = load_profile("non_existent_layout")
        assert non_existent_profile is None
        print("Test for non-existent profile successful.")

    finally:
        # 7. Clean up the database file
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"\nCleaned up database file: {DB_PATH}")