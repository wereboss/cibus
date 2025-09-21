"""
Cibus: Agentic Pipeline for Data Profiling

This module contains the core LangGraph-based agent pipeline for the Cibus MVP.
It orchestrates the data ingestion, parsing, and profiling steps.
"""

from typing import List, Dict, Tuple, Any, TypedDict
from langgraph.graph import StateGraph, START, END

# Import our working utility functions
import os
import openpyxl
from layout_parser import parse_xls_layout
from data_parser import parse_fixed_length_data
from profiler_utilities import profile_data
from llm_profiler import infer_data_pattern
from database import init_db, save_profile
import numpy as np

# Define the state of our graph as a TypedDict
class AgentState(TypedDict):
    """Represents the state of the agent's workflow."""
    inputs: Dict[str, Any]
    layout: List[Tuple[str, int]]
    parsed_data: List[Dict[str, str]]
    profile: Dict[str, Any]
    
# Define the nodes (functions) for our graph
def parse_layout_node(state: AgentState):
    """Node 1: Parses the XLS layout file."""
    print("--- DEBUG: Entering 'parse_layout_node' ---")
    filepath = state['inputs'].get("layout_path")
    layout = parse_xls_layout(filepath)
    print(f"--- DEBUG: 'parse_layout_node' returning layout: {layout[:1]}")
    return {"layout": layout}

def parse_data_node(state: AgentState):
    """Node 2: Parses the fixed-length data file."""
    print("--- DEBUG: Entering 'parse_data_node' ---")
    data_content = state['inputs'].get("data_content")
    layout = state['layout']
    parsed_data = parse_fixed_length_data(data_content, layout)
    print(f"--- DEBUG: 'parse_data_node' returning {len(parsed_data)} records")
    return {"parsed_data": parsed_data}

def profile_data_node(state: AgentState):
    """Node 3: Profiles the structured data."""
    print("--- DEBUG: Entering 'profile_data_node' ---")
    profile = profile_data(state['parsed_data'])
    
    # Cast numpy types to native Python types for clean output
    for col, stats in profile.items():
        for key, value in stats.items():
            if isinstance(value, np.integer):
                profile[col][key] = int(value)
            elif isinstance(value, np.floating):
                profile[col][key] = float(value)

    print("--- DEBUG: 'profile_data_node' returning profile data")
    return {"profile": profile}

def infer_patterns_node(state: AgentState):
    """Node 4: Uses the LLM to infer data patterns for each column."""
    print("--- DEBUG: Entering 'infer_patterns_node' ---")
    profile = state['profile']
    
    inferred_profile = {}
    for column, stats in profile.items():
        inferred_pattern = infer_data_pattern(column, stats)
        inferred_profile[column] = {**stats, **inferred_pattern}
    
    print("--- DEBUG: 'infer_patterns_node' returning inferred patterns")
    return {"profile": inferred_profile}

def persist_profile_node(state: AgentState):
    """Node 5: Saves the final profile to the database."""
    print("--- DEBUG: Entering 'persist_profile_node' ---")
    layout_name = state['inputs'].get("layout_name")
    profile = state['profile']
    save_profile(layout_name, profile)
    print("--- DEBUG: 'persist_profile_node' finished")
    return {}

def finish_node(state: AgentState):
    """Final Node: Prints the results."""
    print("--- DEBUG: Entering 'finish_node' ---")
    print("\n--- Agent Workflow Complete ---")
    print("Final Profile:")
    print(state['profile'])
    return {}

if __name__ == "__main__":
    # --- Full Integrated Test ---
    print("--- Starting Full Integrated Test ---")
    
    # 1. Create dummy files to simulate inputs
    # Dummy layout for a 10-column file, now with COBOL formats
    sample_layout_cobol = [
        ("id", "9(8)"),
        ("product_code", "X(10)"),
        ("product_type", "X(15)"),
        ("price", "9(7)"),
        ("currency", "X(3)"),
        ("status", "X(10)"),
        ("order_date", "9(8)"),
        ("customer_id", "X(12)"),
        ("zip_code", "9(5)"),
        ("quantity", "9(4)")
    ]
    dummy_layout_filepath = "dummy_layout.xlsx"
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["Field Name", "Field Length"])
    for name, length in sample_layout_cobol:
        sheet.append([name, length])
    workbook.save(dummy_layout_filepath)

    # Corrected dummy fixed-length data string (82 characters per line)
    dummy_data_content = """\
00001234PROD-A0001FURNITURE       120.00USDSTATUS-A  02022023CUST00000001234560034
00001235PROD-B0002APPLIANCE      2500.50EURSTATUS-B  02022023CUST00000002234560056
00001236PROD-A0001ELECTRONIC      999.99USDSTATUS-C  02022023CUST00000003456780078
00001237PROD-C0003APPLIANCE       550.00EURSTATUS-B  02022023CUST00000004567890056
00001238PROD-A0001FURNITURE       150.00USDSTATUS-A  02022023CUST00000005234560012"""
    dummy_data_filepath = "dummy_data.txt"
    with open(dummy_data_filepath, "w") as f:
        f.write(dummy_data_content)
        
    # 2. Define and compile the LangGraph workflow
    workflow = StateGraph(AgentState)
    workflow.add_node("parse_layout", parse_layout_node)
    workflow.add_node("parse_data", parse_data_node)
    workflow.add_node("profile_data", profile_data_node)
    workflow.add_node("infer_patterns", infer_patterns_node)
    workflow.add_node("persist_profile", persist_profile_node)
    workflow.add_node("finish", finish_node)
    workflow.add_edge(START, "parse_layout")
    workflow.add_edge("parse_layout", "parse_data")
    workflow.add_edge("parse_data", "profile_data")
    workflow.add_edge("profile_data", "infer_patterns")
    workflow.add_edge("infer_patterns", "persist_profile")
    workflow.add_edge("persist_profile", "finish")
    workflow.add_edge("finish", END)
    app = workflow.compile()
    
    # Initialize the database and define a layout name
    init_db()
    layout_name_for_test = "test_mainframe_layout_v1"
    
    # 3. Run the graph with our dummy file paths and data
    inputs = {
        "inputs": {
            "layout_name": layout_name_for_test,
            "layout_path": dummy_layout_filepath,
            "data_content": dummy_data_content
        }
    }
    
    try:
        for state in app.stream(inputs):
            pass
    finally:
        # 4. Clean up the dummy files
        if os.path.exists(dummy_layout_filepath):
            os.remove(dummy_layout_filepath)
        if os.path.exists(dummy_data_filepath):
            os.remove(dummy_data_filepath)
        print("\n--- Test complete, dummy files cleaned up ---")