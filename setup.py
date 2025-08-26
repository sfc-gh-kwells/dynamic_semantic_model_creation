#!/usr/bin/env python3
"""
Setup script for Dynamic Semantic Model Generator.

This script helps new users set up their environment and verify everything is working.
"""

import subprocess
import sys
import os


def install_requirements():
    """Install required packages."""
    print("📦 Installing required packages...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False


def check_environment():
    """Check if Snowflake environment variables are set."""
    print("\n🔍 Checking Snowflake environment variables...")

    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA'
    ]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print("⚠️  Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 To set these, add to your shell profile:")
        for var in missing_vars:
            print(f"   export {var}=your_value")
        print("\n   Or create a .env file with these values")
        return False
    else:
        print("✅ All Snowflake environment variables are set!")
        return True


def test_imports():
    """Test if all required modules can be imported."""
    print("\n🧪 Testing imports...")

    try:
        import snowflake.connector
        print("✅ snowflake-connector-python imported successfully")
    except ImportError:
        print("❌ Failed to import snowflake-connector-python")
        return False

    try:
        import pandas as pd
        print("✅ pandas imported successfully")
    except ImportError:
        print("❌ Failed to import pandas")
        return False

    try:
        import yaml
        print("✅ PyYAML imported successfully")
    except ImportError:
        print("❌ Failed to import PyYAML")
        return False

    return True


def test_core_functionality():
    """Test core functionality without Snowflake connection."""
    print("\n🚀 Testing core functionality...")

    try:
        from main import list_available_facts, generate_dynamic_semantic_model

        # Test fact listing
        facts = list_available_facts()
        print(f"✅ Found {len(facts)} facts in facts.yaml")

        # Test model generation with a few facts
        test_facts = facts[:3] if len(facts) >= 3 else facts
        model = generate_dynamic_semantic_model(
            fact_names=test_facts,
            output_path="test_setup_model.yaml"
        )

        # Clean up test file
        if os.path.exists("test_setup_model.yaml"):
            os.remove("test_setup_model.yaml")

        print("✅ Dynamic model generation working correctly")
        return True

    except Exception as e:
        print(f"❌ Core functionality test failed: {e}")
        return False


def create_example_env_file():
    """Create an example .env file."""
    print("\n📝 Creating example .env file...")

    env_content = """# Snowflake Configuration
# Copy this file to .env and fill in your actual values

SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role  # Optional

# Example usage after setting these variables:
# python -c "from main import generate_semantic_model_from_env_and_query; print('Ready to use!')"
"""

    try:
        with open('.env.example', 'w') as f:
            f.write(env_content)
        print("✅ Created .env.example file")
        print("   Copy this to .env and fill in your Snowflake credentials")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env.example: {e}")
        return False


def main():
    """Main setup function."""
    print("🎯 Dynamic Semantic Model Generator Setup")
    print("=" * 50)

    success = True

    # Install requirements
    if not install_requirements():
        success = False

    # Test imports
    if not test_imports():
        success = False

    # Test core functionality
    if not test_core_functionality():
        success = False

    # Check environment (optional for basic functionality)
    env_ready = check_environment()

    # Create example env file
    create_example_env_file()

    print("\n" + "=" * 50)
    if success:
        print("🎉 Setup completed successfully!")
        print("\n📋 Next steps:")
        print("1. Review the README.md file")
        print("2. Set up Snowflake environment variables (if using Snowflake)")
        print("3. Run: python main.py (to see available functionality)")
        print("4. Try: python snowflake_example.py (for usage examples)")

        if not env_ready:
            print("\n⚠️  Note: Snowflake functionality requires environment variables")
            print("   But you can still use local fact selection features!")
    else:
        print("❌ Setup encountered some issues")
        print("   Please check the error messages above and try again")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
