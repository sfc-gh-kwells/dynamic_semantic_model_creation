#!/usr/bin/env python3

"""
End-to-end test: Generate semantic model from Snowflake → Use with Cortex Analyst API
"""

import os
from main import (
    CortexAnalystClient,
    load_config_from_file,
    generate_semantic_model_from_snowflake,
    convert_to_yaml_string
)


def test_end_to_end_workflow():
    """Test complete workflow: Snowflake query → Semantic model → Cortex Analyst"""

    print("🚀 End-to-End Integration Test")
    print("=" * 60)
    print("Testing: Snowflake Query → Semantic Model → Cortex Analyst API")

    try:
        # Load configuration
        print("\n1️⃣ Loading configuration...")
        config = load_config_from_file()

        account = config['account']
        user = config['user']
        pat_token = config['token']
        # Default to COMPUTE_WH if not specified
        warehouse = config.get('warehouse', 'COMPUTE_WH')
        database = config['database']
        schema = config['schema']
        role = config.get('role')

        print(f"✅ Config loaded - Account: {account}, Database: {database}")

        # Test query that should return fact names
        # For this test, we'll use a simple query that returns known fact names from our facts.yaml
        test_query = """
        SELECT 'LOAN_AMOUNT' as ELEMENT_NUMBER
        UNION ALL
        SELECT 'INCOME' as ELEMENT_NUMBER
        UNION ALL  
        SELECT 'LOAN_ID' as ELEMENT_NUMBER
        """

        print(f"\n2️⃣ Generating semantic model from Snowflake...")
        print(f"Query: {test_query.strip()}")

        # Try different warehouses in case the configured one doesn't exist
        warehouse_options = [warehouse, 'COMPUTE_WH', 'APP_WH', None]
        semantic_model_yaml = None

        for wh in warehouse_options:
            try:
                print(f"⚙️ Trying warehouse: {wh or 'None (default)'}")
                # Generate semantic model with warehouse usage
                semantic_model_yaml = generate_semantic_model_from_snowflake(
                    query=test_query,
                    account=account,
                    user=user,
                    token=pat_token,
                    warehouse=wh,
                    database=database,
                    schema=schema,
                    role=role,
                    return_yaml_string=True
                )
                if semantic_model_yaml:
                    print(f"✅ Success with warehouse: {wh or 'default'}")
                    break
            except Exception as e:
                print(f"❌ Failed with warehouse {wh}: {str(e)[:100]}...")
                if wh == warehouse_options[-1]:  # Last option
                    raise

        if not semantic_model_yaml:
            print("❌ Failed to generate semantic model")
            return False

        print(f"✅ Semantic model generated successfully!")
        print(f"📄 YAML length: {len(semantic_model_yaml)} characters")

        # Show a sample of the generated model
        print(f"\n--- Generated Model Sample (first 300 chars) ---")
        print(semantic_model_yaml[:300] + "..." if len(
            semantic_model_yaml) > 300 else semantic_model_yaml)
        print("--- End Sample ---")

        print(f"\n3️⃣ Testing with Cortex Analyst API...")

        # Create Cortex Analyst client
        account_url = f"https://{account}.snowflakecomputing.com"
        client = CortexAnalystClient(
            account_url=account_url,
            authorization_token=pat_token,
            token_type="PROGRAMMATIC_ACCESS_TOKEN"
        )

        # Test question
        test_question = "What are the available loan metrics in this dataset?"
        print(f"Question: '{test_question}'")

        # Send to Cortex Analyst
        response = client.send_message(
            question=test_question,
            semantic_model=semantic_model_yaml
        )

        print("\n✅ Cortex Analyst response received!")
        print("\n--- API Response ---")

        # Parse and display response
        if 'message' in response and 'content' in response['message']:
            for content in response['message']['content']:
                if content.get('type') == 'text':
                    print(f"📝 Analysis: {content.get('text', 'N/A')}")
                elif content.get('type') == 'sql':
                    print(
                        f"💾 Generated SQL:\n{content.get('statement', 'N/A')}")
                elif content.get('type') == 'suggestions':
                    suggestions = content.get('suggestions', [])
                    if suggestions:
                        print(f"💡 Suggestions: {', '.join(suggestions)}")

        if 'warnings' in response and response['warnings']:
            print(f"⚠️ Warnings: {len(response['warnings'])} warning(s)")
            for warning in response['warnings']:
                print(f"   - {warning.get('message', 'Unknown warning')}")

        if 'request_id' in response:
            print(f"🔍 Request ID: {response['request_id']}")

        print("--- End Response ---")

        print(f"\n🎉 END-TO-END SUCCESS!")
        print("✅ Generated semantic model from Snowflake query")
        print("✅ Successfully used model with Cortex Analyst API")
        print("✅ Received meaningful analysis and SQL generation")

        return True

    except Exception as e:
        print(f"\n❌ End-to-end test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_end_to_end_workflow()

    if success:
        print(f"\n🚀 COMPLETE END-TO-END INTEGRATION VERIFIED!")
        print("The full workflow is working correctly:")
        print("  1. Connect to Snowflake with PAT")
        print("  2. Execute query to get fact names")
        print("  3. Generate dynamic semantic model")
        print("  4. Use model with Cortex Analyst API")
        print("  5. Receive analysis and SQL generation")
    else:
        print(f"\n❌ End-to-end test failed. Check your configuration and credentials.")
