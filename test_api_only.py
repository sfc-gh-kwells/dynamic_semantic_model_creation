#!/usr/bin/env python3

"""
Test Cortex Analyst API directly with PAT token, bypassing Snowflake connection.
"""

import os
from main import CortexAnalystClient, load_yaml, load_config_from_file


def test_cortex_analyst_api_only():
    """Test Cortex Analyst API with a pre-built semantic model."""

    print("🧪 Testing Cortex Analyst API Only (No Snowflake Connection)")
    print("=" * 70)

    # Try to load configuration from config file first, then environment variables
    try:
        print("📋 Loading configuration...")
        config = load_config_from_file()
        pat_token = config.get('token')
        account = config.get('account')

        if not pat_token:
            pat_token = os.getenv('SNOWFLAKE_PAT')

        if not account:
            account = os.getenv('SNOWFLAKE_ACCOUNT')

        if not pat_token:
            pat_token = input(
                "Enter your Personal Access Token (PAT): ").strip()

        if not account:
            account = input(
                "Enter your Snowflake account identifier (e.g., abc123.us-east-1): ").strip()

        account_url = f"https://{account}.snowflakecomputing.com"

    except Exception as e:
        print(f"⚠️ Could not load config file: {e}")
        print("📝 Please provide configuration manually or via environment variables:")

        pat_token = os.getenv('SNOWFLAKE_PAT') or input(
            "Enter your Personal Access Token (PAT): ").strip()
        account = os.getenv('SNOWFLAKE_ACCOUNT') or input(
            "Enter your Snowflake account identifier (e.g., abc123.us-east-1): ").strip()
        account_url = f"https://{account}.snowflakecomputing.com"

    try:
        print("1️⃣ Loading pre-built semantic model...")
        # Use relative path to semantic model file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        model_file = os.path.join(script_dir, "generated_model_example1.yaml")

        if not os.path.exists(model_file):
            # Fallback to other model files if the example doesn't exist
            alternative_models = [
                "generated_model_loan_focus.yaml", "base.yaml"]
            for alt_model in alternative_models:
                alt_path = os.path.join(script_dir, alt_model)
                if os.path.exists(alt_path):
                    model_file = alt_path
                    print(f"📄 Using alternative model file: {alt_model}")
                    break
            else:
                raise FileNotFoundError(
                    f"No semantic model files found in {script_dir}")

        semantic_model_dict = load_yaml(model_file)

        if not semantic_model_dict:
            print("❌ Failed to load semantic model")
            return False

        print(
            f"✅ Loaded semantic model: '{semantic_model_dict.get('name', 'Unknown')}'")

        print("\n2️⃣ Converting to YAML string...")
        from main import convert_to_yaml_string
        semantic_model_yaml = convert_to_yaml_string(semantic_model_dict)

        if not semantic_model_yaml:
            print("❌ Failed to convert to YAML string")
            return False

        print(f"✅ YAML string ready ({len(semantic_model_yaml)} characters)")

        print("\n3️⃣ Creating Cortex Analyst client...")
        client = CortexAnalystClient(
            account_url=account_url,
            authorization_token=pat_token,
            token_type="PROGRAMMATIC_ACCESS_TOKEN"
        )
        print("✅ Client created")

        print("\n4️⃣ Sending test question to Cortex Analyst...")
        test_question = "What is the total loan amount in the dataset?"

        print(f"Question: '{test_question}'")

        response = client.send_message(
            question=test_question,
            semantic_model=semantic_model_yaml,
            stream=False
        )

        print("✅ Response received!")
        print("\n--- Cortex Analyst Response ---")

        # Parse and display response
        if 'message' in response and 'content' in response['message']:
            for i, content in enumerate(response['message']['content']):
                if content.get('type') == 'text':
                    print(f"📝 Analysis: {content.get('text', 'N/A')}")
                elif content.get('type') == 'sql':
                    print(
                        f"💾 Generated SQL: {content.get('statement', 'N/A')}")
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

        print("\n🎉 SUCCESS! Cortex Analyst API test completed successfully!")
        return True

    except Exception as e:
        print(f"\n❌ API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_cortex_analyst_api_only()
    if success:
        print("\n✅ All tests passed! The Cortex Analyst integration is working.")
    else:
        print("\n❌ Test failed. Check your PAT token and configuration.")
