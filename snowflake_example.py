#!/usr/bin/env python3
"""
Example usage of Snowflake integration for dynamic semantic model generation.

This script demonstrates how to:
1. Connect to Snowflake 
2. Execute a data dictionary query
3. Parse results into semantic model facts
4. Generate a complete semantic model YAML file
"""

from main import generate_semantic_model_from_snowflake, generate_semantic_model_from_env_and_query
import os

# Example data dictionary query
# This query only needs to return ELEMENT_NUMBER that matches facts in facts.yaml
SAMPLE_QUERY = """
SELECT ELEMENT_NUMBER
FROM your_database.your_schema.data_dictionary_table
WHERE table_name = 'your_target_table'
ORDER BY ELEMENT_NUMBER
"""


def example_with_direct_credentials():
    """Example using direct credential parameters."""

    print("=== Example 1: Direct Credentials ===")

    # Replace these with your actual Snowflake credentials
    snowflake_params = {
        'account': 'your_account.region',        # e.g., 'abc12345.us-east-1'
        'user': 'your_username',
        'password': 'your_password',
        'warehouse': 'your_warehouse',           # e.g., 'COMPUTE_WH'
        'database': 'your_database',
        'schema': 'your_schema',
        'role': 'your_role'                      # Optional
    }

    try:
        # Example 1a: Save locally only
        model = generate_semantic_model_from_snowflake(
            query=SAMPLE_QUERY,
            **snowflake_params,
            output_path='snowflake_generated_model.yaml'
        )

        # Example 1b: Save locally AND upload to stage with timestamp
        model = generate_semantic_model_from_snowflake(
            query=SAMPLE_QUERY,
            **snowflake_params,
            output_path='local_model.yaml',
            stage_name='@my_stage/semantic_models/',
            stage_filename_base='data_dict_model'
        )

        print(
            f"✅ Successfully generated semantic model with {len(model.get('tables', [{}])[0].get('facts', []))} facts")

    except Exception as e:
        print(f"❌ Error: {e}")


def example_with_environment_variables():
    """Example using environment variables (recommended for security)."""

    print("\n=== Example 2: Environment Variables ===")

    # Check if environment variables are set
    required_env_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]

    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"⚠️ Missing environment variables: {', '.join(missing_vars)}")
        print("\nTo use this method, set these environment variables:")
        for var in required_env_vars:
            print(f"export {var}=your_value")
        print("export SNOWFLAKE_ROLE=your_role  # Optional")
        return

    try:
        # Example with stage upload using environment variables
        model = generate_semantic_model_from_env_and_query(
            query=SAMPLE_QUERY,
            output_path='env_generated_model.yaml',
            stage_name='@my_models_stage/dynamic/',
            stage_filename_base='env_semantic_model'
        )

        print(
            f"✅ Successfully generated semantic model with {len(model.get('tables', [{}])[0].get('facts', []))} facts")

    except Exception as e:
        print(f"❌ Error: {e}")


def show_sample_data_dictionary_format():
    """Show the expected format of the data dictionary table."""

    print("\n=== Expected Data Dictionary Format ===")
    print("""
Your Snowflake data dictionary query only needs to return the ELEMENT_NUMBER column:
    
┌─────────────────┐
│ ELEMENT_NUMBER  │
├─────────────────┤
│ CUSTOMER_ID     │
├─────────────────┤
│ PURCHASE_AMOUNT │
├─────────────────┤
│ LOAN_AMOUNT     │
├─────────────────┤
│ INCOME          │
└─────────────────┘

Key points:
- ELEMENT_NUMBER values must match fact names in your facts.yaml file
- The function will look up each ELEMENT_NUMBER in facts.yaml to get the full fact definition
- Facts not found in facts.yaml will be skipped with a warning
- The semantic model will be built using only the matching pre-existing facts

Example facts.yaml structure:
- name: CUSTOMER_ID
  expr: CUSTOMER_ID
  data_type: NUMBER(10,0)
  description: Unique customer identifier
  sample_values: ['12345', '67890']
  synonyms: ['customer_id', 'cust_id']
    """)


if __name__ == "__main__":
    print("Snowflake Dynamic Semantic Model Generator Examples")
    print("=" * 55)

    # Show expected data format
    show_sample_data_dictionary_format()

    # Run examples
    example_with_direct_credentials()
    example_with_environment_variables()

    print("\n=== Next Steps ===")
    print("1. Ensure your facts.yaml contains all the facts you want to use")
    print("2. Update the SAMPLE_QUERY with your actual data dictionary table details")
    print("3. Make sure the ELEMENT_NUMBER values in your query match fact names in facts.yaml")
    print("4. Replace placeholder credentials with your actual Snowflake connection info")
    print("5. Create a Snowflake stage if you want to upload files (e.g., CREATE STAGE my_stage)")
    print("6. Run this script to generate your semantic model!")
    print("\nGenerated files will be saved as:")
    print("- local_model.yaml (local file from direct credentials)")
    print("- data_dict_model_YYYYMMDD_HHMMSS.yaml (uploaded to @my_stage/semantic_models/)")
    print("- env_generated_model.yaml (local file from environment variables)")
    print("- env_semantic_model_YYYYMMDD_HHMMSS.yaml (uploaded to @my_models_stage/dynamic/)")
    print("\nThe workflow:")
    print("Query → Extract ELEMENT_NUMBER → Lookup in facts.yaml → Build semantic model → Upload to stage")
    print("\nStage upload features:")
    print("- Automatic unique timestamp-based filenames")
    print("- Configurable stage paths and filename prefixes")
    print("- Works with both direct credentials and environment variables")
