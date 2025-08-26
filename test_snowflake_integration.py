#!/usr/bin/env python3
"""
Test script to demonstrate the Snowflake integration functionality
without requiring actual Snowflake credentials.

This simulates the workflow by creating a mock DataFrame with ELEMENT_NUMBER values
that match facts in facts.yaml, then shows how the semantic model is built.
"""

import pandas as pd
from main import extract_fact_names_from_dataframe, get_facts_by_names, generate_dynamic_semantic_model

def test_snowflake_workflow_simulation():
    """
    Simulate the Snowflake workflow using a mock DataFrame.
    """
    print("=" * 60)
    print("TESTING SNOWFLAKE INTEGRATION WORKFLOW")
    print("=" * 60)
    
    # Step 1: Create mock DataFrame simulating Snowflake query results
    print("\n1️⃣ Simulating Snowflake query results...")
    
    # This simulates what pd.read_sql(query, conn) would return
    mock_snowflake_data = {
        'ELEMENT_NUMBER': [
            'LOAN_AMOUNT',
            'INCOME', 
            'MORTGAGERESPONSE',
            'HIGH_INCOME_FLAG',
            'NONEXISTENT_FACT'  # This one won't be found in facts.yaml
        ]
    }
    
    df = pd.DataFrame(mock_snowflake_data)
    print(f"Mock Snowflake result DataFrame:")
    print(df)
    
    # Step 2: Extract fact names from DataFrame
    print(f"\n2️⃣ Extracting fact names from DataFrame...")
    fact_names = extract_fact_names_from_dataframe(df)
    print(f"Extracted fact names: {fact_names}")
    
    # Step 3: Look up facts in facts.yaml
    print(f"\n3️⃣ Looking up facts in facts.yaml...")
    facts = get_facts_by_names(fact_names)
    print(f"Found {len(facts)} matching facts:")
    for fact in facts:
        print(f"  ✅ {fact['name']}: {fact.get('description', 'No description')[:50]}...")
    
    # Step 4: Generate semantic model
    print(f"\n4️⃣ Generating semantic model...")
    semantic_model = generate_dynamic_semantic_model(
        fact_names=fact_names,
        output_path='test_snowflake_model.yaml'
    )
    
    print(f"✅ Generated semantic model with {len(semantic_model['tables'][0]['facts'])} facts")
    
    # Step 5: Show what would happen with real Snowflake
    print(f"\n5️⃣ Real Snowflake workflow would be:")
    print("""
# Your actual usage:
query = "SELECT ELEMENT_NUMBER FROM data_dictionary WHERE ..."
df = pd.read_sql(query, snowflake_conn)  # Returns DataFrame like our mock
fact_names = extract_fact_names_from_dataframe(df)  # Extract ELEMENT_NUMBER values
facts = get_facts_by_names(fact_names)  # Look up in facts.yaml
# Build semantic model with base.yaml + selected facts
    """)
    
    return semantic_model


def show_facts_in_yaml():
    """Show what facts are available in facts.yaml."""
    
    print("\n" + "=" * 60)
    print("AVAILABLE FACTS IN FACTS.YAML")
    print("=" * 60)
    
    from main import list_available_facts
    facts = list_available_facts()
    
    print(f"Total facts available: {len(facts)}")
    print("\nFact names that can be used as ELEMENT_NUMBER values:")
    for i, fact in enumerate(facts, 1):
        print(f"{i:2d}. {fact}")
    
    print(f"\nYour Snowflake query should return ELEMENT_NUMBER values")
    print(f"that match these fact names to include them in the semantic model.")


if __name__ == "__main__":
    print("Snowflake Integration Test")
    
    # Show available facts
    show_facts_in_yaml()
    
    # Test the workflow
    semantic_model = test_snowflake_workflow_simulation()
    
    print("\n" + "=" * 60)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print("Generated file: test_snowflake_model.yaml")
    print("\nThis demonstrates how the Snowflake integration works:")
    print("1. Execute query → Get ELEMENT_NUMBER values")
    print("2. Look up matching facts in facts.yaml")
    print("3. Combine with base.yaml → Complete semantic model")
