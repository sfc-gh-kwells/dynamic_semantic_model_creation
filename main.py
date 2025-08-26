import yaml
import json
from pathlib import Path
from typing import Dict, Any, List
import copy
import os
from datetime import datetime
import tempfile
import snowflake.connector
import pandas as pd


BASE_SEMANTIC_MODEL = "/Users/kwells/Desktop/repos/ca_dynamic_semantic_model/base.yaml"
FACTS_FILE = "/Users/kwells/Desktop/repos/ca_dynamic_semantic_model/facts.yaml"


def create_snowflake_connection(account, user, password, warehouse, database, schema, role=None):
    """
    Create a Snowflake connection.

    Returns: snowflake connection object
    """
    conn_params = {
        'account': account,
        'user': user,
        'password': password,
        'warehouse': warehouse,
        'database': database,
        'schema': schema,
    }

    if role:
        conn_params['role'] = role

    try:
        conn = snowflake.connector.connect(**conn_params)
        print(f"✅ Connected to Snowflake: {database}.{schema}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        raise


def extract_fact_names_from_dataframe(df: pd.DataFrame) -> List[str]:
    """
    Extract ELEMENT_NUMBER values from data dictionary DataFrame.

    Expected columns in DataFrame:
    - ELEMENT_NUMBER: Fact name/identifier that matches facts in facts.yaml

    Returns: List of fact names (ELEMENT_NUMBER values)
    """
    fact_names = []

    for index, row in df.iterrows():
        element_number = str(row.get('ELEMENT_NUMBER', '')).strip()

        if not element_number:
            print(f"⚠️ Skipping row {index} with missing ELEMENT_NUMBER")
            continue

        fact_names.append(element_number)
        print(f"📋 Found fact name: {element_number}")

    print(f"📊 Total fact names extracted: {len(fact_names)}")
    return fact_names


def generate_unique_filename(base_name: str = "semantic_model", extension: str = "yaml") -> str:
    """
    Generate a unique filename with timestamp.

    base_name: Base name for the file
    extension: File extension (without dot)

    Returns: Unique filename with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{timestamp}.{extension}"


def upload_to_snowflake_stage(
    conn,
    local_file_path: str,
    stage_name: str,
    remote_file_name: str = None
) -> bool:
    """
    Upload a file to Snowflake stage.

    conn: Active Snowflake connection
    local_file_path: Path to local file to upload
    stage_name: Name of the Snowflake stage (e.g., '@my_stage' or '@my_stage/subfolder/')
    remote_file_name: Optional custom name for the file in stage

    Returns: True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()

        # Ensure stage name starts with @
        if not stage_name.startswith('@'):
            stage_name = f'@{stage_name}'

        # Build PUT command
        if remote_file_name:
            # Upload with custom name
            put_command = f"PUT 'file://{local_file_path}' {stage_name}"
            print(
                f"🔄 Uploading {local_file_path} to {stage_name} as {remote_file_name}")
        else:
            # Upload with original name
            put_command = f"PUT 'file://{local_file_path}' {stage_name}"
            print(f"🔄 Uploading {local_file_path} to {stage_name}")

        # Execute PUT command
        cursor.execute(put_command)
        result = cursor.fetchone()

        if result and result[6] == 'UPLOADED':  # Status column
            print(f"✅ Successfully uploaded to stage: {stage_name}")
            return True
        else:
            print(
                f"❌ Upload failed. Status: {result[6] if result else 'Unknown'}")
            return False

    except Exception as e:
        print(f"❌ Error uploading to stage: {e}")
        return False
    finally:
        if cursor:
            cursor.close()


def save_and_upload_to_stage(
    data: Dict[str, Any],
    conn,
    stage_name: str,
    base_filename: str = "semantic_model",
    local_output_path: str = None
) -> str:
    """
    Save YAML data locally and upload to Snowflake stage with unique timestamp name.

    data: Dictionary data to save as YAML
    conn: Active Snowflake connection
    stage_name: Snowflake stage name
    base_filename: Base name for the file (timestamp will be added)
    local_output_path: Optional local path to save file (if None, uses temp file)

    Returns: Remote filename in stage
    """
    # Generate unique filename
    unique_filename = generate_unique_filename(base_filename, "yaml")

    # Determine local file path
    if local_output_path:
        local_file_path = local_output_path
    else:
        # Use temporary file
        temp_dir = tempfile.gettempdir()
        local_file_path = os.path.join(temp_dir, unique_filename)

    try:
        # Save YAML locally
        print(f"💾 Saving YAML to local file: {local_file_path}")
        save_yaml_file(data, local_file_path)

        # Upload to stage
        success = upload_to_snowflake_stage(
            conn=conn,
            local_file_path=local_file_path,
            stage_name=stage_name,
            remote_file_name=unique_filename
        )

        if success:
            print(f"🎯 File available in stage as: {unique_filename}")
            return unique_filename
        else:
            raise Exception("Failed to upload to stage")

    except Exception as e:
        print(f"❌ Error in save_and_upload_to_stage: {e}")
        raise


def generate_semantic_model_from_snowflake(
    query: str,
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str = None,
    base_path: str = BASE_SEMANTIC_MODEL,
    facts_path: str = FACTS_FILE,
    output_path: str = None,
    stage_name: str = None,
    stage_filename_base: str = "semantic_model"
) -> Dict[str, Any]:
    """
    Execute Snowflake query to get fact names and generate semantic model using existing facts.

    query: SQL query string that returns ELEMENT_NUMBER column with fact names
    account, user, password, warehouse, database, schema: Snowflake connection parameters
    role: Optional Snowflake role
    base_path: Path to base semantic model YAML
    facts_path: Path to facts YAML file containing pre-created facts
    output_path: Optional path to save generated model locally
    stage_name: Optional Snowflake stage name to upload file (e.g., '@my_stage' or 'my_stage/folder/')
    stage_filename_base: Base name for stage file (timestamp will be added)

    Returns: Generated semantic model dictionary
    """
    # Create connection
    conn = create_snowflake_connection(
        account, user, password, warehouse, database, schema, role)

    try:
        # Execute query using pandas
        print("🔍 Executing data dictionary query...")
        df = pd.read_sql(query, conn)
        print(f"✅ Query executed successfully. Retrieved {len(df)} rows.")

        if df.empty:
            print("⚠️ No results returned from data dictionary query")
            return {}

        # Extract fact names from DataFrame
        print("🔄 Extracting fact names from query results...")
        fact_names = extract_fact_names_from_dataframe(df)

        if not fact_names:
            print("⚠️ No valid fact names could be extracted from results")
            return {}

        # Get matching facts from facts.yaml
        print("📋 Looking up facts in facts.yaml...")
        facts = get_facts_by_names(fact_names, facts_path)

        if not facts:
            print("⚠️ No matching facts found in facts.yaml")
            return {}

        # Load base model
        print("📖 Loading base semantic model...")
        base_model = load_yaml(base_path)
        if not base_model:
            raise ValueError(f"Could not load base model from {base_path}")

        # Create dynamic model
        dynamic_model = copy.deepcopy(base_model)

        # Add facts to the first table
        if 'tables' in dynamic_model and len(dynamic_model['tables']) > 0:
            dynamic_model['tables'][0]['facts'] = facts
            print(f"✅ Added {len(facts)} facts to semantic model")
        else:
            print("⚠️ No tables found in base model")

        # Save to local file if output path provided
        if output_path:
            save_yaml_file(dynamic_model, output_path)

        # Upload to Snowflake stage if stage name provided
        stage_filename = None
        if stage_name:
            try:
                stage_filename = save_and_upload_to_stage(
                    data=dynamic_model,
                    conn=conn,
                    stage_name=stage_name,
                    base_filename=stage_filename_base,
                    local_output_path=output_path  # Use same local file if provided
                )
                print(f"📤 Uploaded to stage {stage_name} as: {stage_filename}")
            except Exception as e:
                print(f"⚠️ Failed to upload to stage: {e}")

        return dynamic_model

    finally:
        # Always close the connection
        if conn:
            conn.close()
            print("🔌 Snowflake connection closed.")


def generate_semantic_model_from_env_and_query(
    query: str,
    facts_path: str = FACTS_FILE,
    output_path: str = None,
    stage_name: str = None,
    stage_filename_base: str = "semantic_model"
) -> Dict[str, Any]:
    """
    Convenience function that uses environment variables for Snowflake connection.

    Expected environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER  
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    - SNOWFLAKE_ROLE (optional)

    query: SQL query string that returns ELEMENT_NUMBER column
    facts_path: Path to facts YAML file containing pre-created facts
    output_path: Optional path to save generated model locally
    stage_name: Optional Snowflake stage name to upload file
    stage_filename_base: Base name for stage file (timestamp will be added)

    Returns: Generated semantic model dictionary
    """
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')

    # Check required environment variables
    required_vars = [account, user, password, warehouse, database, schema]
    if not all(required_vars):
        missing_vars = []
        for var_name, var_value in [
            ('SNOWFLAKE_ACCOUNT', account),
            ('SNOWFLAKE_USER', user),
            ('SNOWFLAKE_PASSWORD', password),
            ('SNOWFLAKE_WAREHOUSE', warehouse),
            ('SNOWFLAKE_DATABASE', database),
            ('SNOWFLAKE_SCHEMA', schema)
        ]:
            if not var_value:
                missing_vars.append(var_name)

        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}")

    return generate_semantic_model_from_snowflake(
        query=query,
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        facts_path=facts_path,
        output_path=output_path,
        stage_name=stage_name,
        stage_filename_base=stage_filename_base
    )


def load_yaml(yaml_path: str) -> Dict[str, Any]:
    """
    Load local yaml file and parse it.

    yaml_path: str The absolute path to the location of your yaml file. Something like path/to/your/file.yaml.
    """
    try:
        with open(yaml_path, 'r') as file:
            data = yaml.safe_load(file)
        return data
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        return {}
    except FileNotFoundError:
        print(f"File not found: {yaml_path}")
        return {}


def load_facts_from_yaml(facts_path: str) -> List[Dict[str, Any]]:
    """
    Load facts from facts.yaml.

    facts_path: Path to the facts YAML file
    Returns: List of fact dictionaries
    """
    try:
        facts_data = load_yaml(facts_path)

        # If it's a list, return it directly
        if isinstance(facts_data, list):
            return facts_data

        # If it's a dict with facts key, return the facts
        if isinstance(facts_data, dict) and 'facts' in facts_data:
            facts_list = facts_data['facts']
            if isinstance(facts_list, list):
                return facts_list
            else:
                return [facts_list]  # Single fact wrapped in list

        return []

    except Exception as e:
        print(f"Error loading facts: {e}")
        return []


def get_facts_by_names(fact_names: List[str], facts_path: str = FACTS_FILE) -> List[Dict[str, Any]]:
    """
    Get specific facts from facts.yaml by their names.

    fact_names: List of fact names to retrieve
    facts_path: Path to the facts YAML file
    Returns: List of matching fact dictionaries
    """
    all_facts = load_facts_from_yaml(facts_path)
    selected_facts = []

    for fact_name in fact_names:
        # Find fact with matching name
        matching_fact = None
        for fact in all_facts:
            if isinstance(fact, dict) and fact.get('name') == fact_name:
                matching_fact = fact
                break

        if matching_fact:
            selected_facts.append(matching_fact)
        else:
            print(f"Warning: Fact '{fact_name}' not found in facts file")

    return selected_facts


def generate_dynamic_semantic_model(fact_names: List[str],
                                    base_path: str = BASE_SEMANTIC_MODEL,
                                    facts_path: str = FACTS_FILE,
                                    output_path: str = None) -> Dict[str, Any]:
    """
    Generate a dynamic semantic model by combining base.yaml with selected facts.

    fact_names: List of fact names to include in the model
    base_path: Path to the base YAML file
    facts_path: Path to the facts YAML file  
    output_path: Optional path to save the generated YAML file
    Returns: Combined semantic model dictionary
    """
    # Load base model
    base_model = load_yaml(base_path)
    if not base_model:
        raise ValueError(f"Could not load base model from {base_path}")

    # Create a deep copy to avoid modifying the original
    dynamic_model = copy.deepcopy(base_model)

    # Get selected facts
    selected_facts = get_facts_by_names(fact_names, facts_path)

    if not selected_facts:
        print("No facts were selected or found")
        return dynamic_model

    # Replace or add facts to the first table
    if 'tables' in dynamic_model and len(dynamic_model['tables']) > 0:
        # Replace the facts in the first table with selected facts
        dynamic_model['tables'][0]['facts'] = selected_facts
        print(f"Added {len(selected_facts)} facts to the semantic model")
    else:
        print("Warning: No tables found in base model")

    # Save to file if output path is provided
    if output_path:
        save_yaml_file(dynamic_model, output_path)

    return dynamic_model


def save_yaml_file(data: Dict[str, Any], output_path: str):
    """
    Save dictionary data to a YAML file.

    data: Dictionary to save
    output_path: Path where to save the YAML file
    """
    try:
        with open(output_path, 'w') as file:
            yaml.dump(data, file, default_flow_style=False,
                      sort_keys=False, indent=2)
        print(f"Generated YAML saved to: {output_path}")
    except Exception as e:
        print(f"Error saving YAML file: {e}")


def list_available_facts(facts_path: str = FACTS_FILE) -> List[str]:
    """
    List all available fact names from the facts file.

    facts_path: Path to the facts YAML file
    Returns: List of fact names
    """
    all_facts = load_facts_from_yaml(facts_path)
    fact_names = []

    for fact in all_facts:
        if isinstance(fact, dict) and 'name' in fact:
            fact_names.append(fact['name'])

    return fact_names


def demonstrate_dynamic_generation():
    """Demonstrate dynamic YAML generation with examples."""

    print("=== Available Facts ===")
    available_facts = list_available_facts()
    print(f"Total facts available: {len(available_facts)}")
    print(f"First 5 facts: {available_facts[:5]}")

    print("\n=== Example 1: Generate model with specific facts ===")
    selected_facts = ["LOAN_AMOUNT", "INCOME", "MORTGAGERESPONSE"]
    dynamic_model = generate_dynamic_semantic_model(
        fact_names=selected_facts,
        output_path="generated_model_example1.yaml"
    )

    print(
        f"Generated model with {len(dynamic_model['tables'][0]['facts'])} facts")
    for fact in dynamic_model['tables'][0]['facts']:
        print(
            f"  - {fact['name']}: {fact.get('description', 'No description')[:50]}...")

    print("\n=== Example 2: Generate model with loan-related facts ===")
    loan_facts = [fact for fact in available_facts if 'LOAN' in fact]
    print(f"Found {len(loan_facts)} loan-related facts: {loan_facts}")

    loan_model = generate_dynamic_semantic_model(
        fact_names=loan_facts[:3],  # Take first 3 loan facts
        output_path="generated_model_loan_focus.yaml"
    )

    return dynamic_model, loan_model


def quick_validation(data):
    """Quick validation of parsed YAML data."""
    print("=== YAML Validation ===")
    print(f"Model name: {data.get('name', 'Not found')}")
    print(f"Description: {data.get('description', 'Not found')[:100]}...")
    print(f"Number of tables: {len(data.get('tables', []))}")

    if data.get('tables'):
        first_table = data['tables'][0]
        print(f"First table name: {first_table.get('name', 'Not found')}")
        print(
            f"Number of facts in first table: {len(first_table.get('facts', []))}")


if __name__ == "__main__":
    print("Dynamic Semantic Model Generator")
    print("=" * 40)

    # Demonstrate the new dynamic generation functionality
    demonstrate_dynamic_generation()

    print("\n=== Testing base model loading ===")
    base_model = load_yaml(BASE_SEMANTIC_MODEL)
    quick_validation(base_model)

    print("\n=== Snowflake Integration Available ===")
    print("✅ Snowflake functionality is available!")
    print("\nExample usage:")
    print("""
# Method 1: Direct connection parameters
# Query just needs to return ELEMENT_NUMBER column that matches fact names in facts.yaml
query = '''
SELECT ELEMENT_NUMBER
FROM your_database.your_schema.data_dictionary_table
WHERE condition = 'your_filter'
'''

# Save locally only
model = generate_semantic_model_from_snowflake(
    query=query,
    account='your_account.region',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',
    output_path='snowflake_generated_model.yaml'
)

# Save locally AND upload to Snowflake stage with timestamp
model = generate_semantic_model_from_snowflake(
    query=query,
    account='your_account.region',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',
    output_path='local_model.yaml',
    stage_name='@my_stage/semantic_models/',
    stage_filename_base='mortgage_model'
)
# This creates a file like: mortgage_model_20241215_143022.yaml in the stage

# Method 2: Using environment variables (recommended)
# Set these environment variables first:
# SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
# SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

model = generate_semantic_model_from_env_and_query(
    query=query,
    output_path='env_generated_model.yaml',
    stage_name='@my_models_stage',
    stage_filename_base='dynamic_semantic_model'
)

# The function will:
# 1. Execute your query to get ELEMENT_NUMBER values
# 2. Look up matching facts in facts.yaml 
# 3. Combine with base.yaml to create the semantic model
# 4. Save locally (if output_path provided)
# 5. Upload to Snowflake stage with unique timestamp name (if stage_name provided)
        """)
