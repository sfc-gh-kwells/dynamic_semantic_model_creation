#!/usr/bin/env python3
"""
Demo script showing the timestamp-based filename generation for Snowflake stage uploads.

This script demonstrates the new stage upload functionality without requiring 
actual Snowflake credentials.
"""

from main import generate_unique_filename
from datetime import datetime
import time


def demo_unique_filenames():
    """Demonstrate unique filename generation with timestamps."""

    print("=" * 60)
    print("DEMO: Timestamp-based Unique Filename Generation")
    print("=" * 60)

    print("\n🕒 Generating unique filenames with timestamps...")

    # Generate several filenames to show uniqueness
    filenames = []
    for i in range(3):
        filename = generate_unique_filename("semantic_model", "yaml")
        filenames.append(filename)
        print(f"  {i+1}. {filename}")
        time.sleep(1)  # Wait 1 second to show timestamp difference

    print(f"\n📋 All filenames are unique and include timestamp:")
    for filename in filenames:
        print(f"  ✅ {filename}")

    print(f"\n🎯 Customizable base names:")
    custom_examples = [
        generate_unique_filename("mortgage_model", "yaml"),
        generate_unique_filename("customer_facts", "yaml"),
        generate_unique_filename("sales_semantic", "yaml"),
        generate_unique_filename("data_dict_output", "yaml")
    ]

    for example in custom_examples:
        print(f"  📁 {example}")

    return filenames


def show_stage_upload_workflow():
    """Show how the stage upload workflow would work."""

    print("\n" + "=" * 60)
    print("SNOWFLAKE STAGE UPLOAD WORKFLOW")
    print("=" * 60)

    print("""
🔄 The complete workflow when using stage uploads:

1️⃣ Execute your Snowflake query:
   SELECT ELEMENT_NUMBER FROM data_dictionary WHERE ...
   
2️⃣ Extract fact names from query results:
   ['LOAN_AMOUNT', 'INCOME', 'MORTGAGERESPONSE', ...]
   
3️⃣ Look up facts in facts.yaml:
   - LOAN_AMOUNT: Found ✅
   - INCOME: Found ✅  
   - MORTGAGERESPONSE: Found ✅
   
4️⃣ Generate semantic model:
   Combine base.yaml + selected facts → Complete semantic model
   
5️⃣ Save locally (if output_path provided):
   💾 Saved to: local_model.yaml
   
6️⃣ Upload to Snowflake stage (if stage_name provided):
   🔄 Generate unique filename: mortgage_model_20241215_143532.yaml
   📤 Execute: PUT 'file://local_model.yaml' @my_stage/semantic_models/
   ✅ File uploaded to stage successfully
   
🎯 Result: Your semantic model is now available in Snowflake stage with a unique timestamp!
    """)


def show_stage_examples():
    """Show different stage name examples."""

    print("\n" + "=" * 60)
    print("SNOWFLAKE STAGE NAME EXAMPLES")
    print("=" * 60)

    examples = [
        {
            "stage": "@my_stage",
            "description": "Root of named stage",
            "result": "semantic_model_YYYYMMDD_HHMMSS.yaml uploaded to @my_stage"
        },
        {
            "stage": "@my_stage/models/",
            "description": "Subfolder in named stage",
            "result": "semantic_model_YYYYMMDD_HHMMSS.yaml uploaded to @my_stage/models/"
        },
        {
            "stage": "my_models_stage/dynamic/",
            "description": "Subfolder (@ added automatically)",
            "result": "semantic_model_YYYYMMDD_HHMMSS.yaml uploaded to @my_models_stage/dynamic/"
        },
        {
            "stage": "@%temp_stage",
            "description": "User stage",
            "result": "semantic_model_YYYYMMDD_HHMMSS.yaml uploaded to user's temp stage"
        }
    ]

    print("Stage configuration examples:")
    print()

    for i, example in enumerate(examples, 1):
        print(f"{i}. Stage: {example['stage']}")
        print(f"   Description: {example['description']}")
        print(f"   Result: {example['result']}")
        print()


if __name__ == "__main__":
    print("Snowflake Stage Upload Demo")

    # Demo unique filename generation
    unique_files = demo_unique_filenames()

    # Show workflow
    show_stage_upload_workflow()

    # Show stage examples
    show_stage_examples()

    print("=" * 60)
    print("DEMO COMPLETED!")
    print("=" * 60)
    print("Key features:")
    print("✅ Automatic timestamp-based unique filenames")
    print("✅ Configurable base filename and stage paths")
    print("✅ Support for both named stages and user stages")
    print("✅ Flexible stage folder organization")
    print("✅ Works with existing local file saving")
    print()
    print("Ready to use with your Snowflake data dictionary queries!")
