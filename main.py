import yaml
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import copy
import os
from datetime import datetime
import tempfile
import snowflake.connector
import pandas as pd
import requests
from io import StringIO
import configparser
import sys


# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Use relative paths based on script location
BASE_SEMANTIC_MODEL = os.path.join(SCRIPT_DIR, "base.yaml")
FACTS_FILE = os.path.join(SCRIPT_DIR, "facts.yaml")
CONFIG_FILE = os.path.join(SCRIPT_DIR, "config.env")


def load_config_from_file(config_path: str = CONFIG_FILE, connection_name: str = "my_example_connection") -> Dict[str, str]:
    """
    Load Snowflake connection details from config.env file.

    config_path: Path to the config.env file
    connection_name: Name of the connection section to load

    Returns: Dictionary with connection parameters
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_path)

        section_name = f"connections.{connection_name}"
        if section_name not in config:
            raise ValueError(
                f"Connection '{connection_name}' not found in config file")

        connection_config = dict(config[section_name])
        print(f"✅ Loaded config for connection: {connection_name}")

        return connection_config

    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        raise


def create_snowflake_connection(**kwargs):
    """
    Create a Snowflake connection.

    Supports both password and Personal Access Token (PAT) authentication.

    Returns: snowflake connection object
    """
    # Get required parameters
    account = kwargs.get('account')
    user = kwargs.get('user')

    if not account or not user:
        raise ValueError("account and user are required parameters")

    # Prepare connection parameters - PAT as password replacement
    conn_params = {k: v for k, v in kwargs.items() if k not in ['token']}

    # Handle authentication - PAT as password replacement
    if 'token' in kwargs:
        # Use PAT as password (Snowflake drivers accept PAT as password replacement)
        conn_params['password'] = kwargs['token']
        print(f"🔐 Using Personal Access Token (PAT) as password replacement")
    elif 'password' not in kwargs:
        raise ValueError("Either password or token (PAT) must be provided")
    else:
        print(f"🔐 Using traditional password authentication")

    try:
        print(f"🔗 Connecting to Snowflake account: {account}")
        conn = snowflake.connector.connect(**conn_params)
        database = kwargs.get('database', 'N/A')
        schema = kwargs.get('schema', 'N/A')
        print(f"✅ Connected to Snowflake: {database}.{schema}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Snowflake: {e}")
        safe_params = {k: v for k, v in conn_params.items() if k not in [
            'password']}
        print(f"Connection params (excluding auth): {safe_params}")
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
    password: str = None,
    warehouse: str = None,
    database: str = None,
    schema: str = None,
    role: str = None,
    token: str = None,
    base_path: str = BASE_SEMANTIC_MODEL,
    facts_path: str = FACTS_FILE,
    output_path: str = None,
    stage_name: str = None,
    stage_filename_base: str = "semantic_model",
    return_yaml_string: bool = False
) -> Union[Dict[str, Any], str]:
    """
    Execute Snowflake query to get fact names and generate semantic model using existing facts.

    query: SQL query string that returns ELEMENT_NUMBER column with fact names
    account, user: Snowflake connection parameters
    password: Snowflake password (optional if token provided)
    warehouse, database, schema: Snowflake connection parameters
    role: Optional Snowflake role
    token: Personal Access Token (optional, alternative to password)
    base_path: Path to base semantic model YAML
    facts_path: Path to facts YAML file containing pre-created facts
    output_path: Optional path to save generated model locally
    stage_name: Optional Snowflake stage name to upload file (e.g., '@my_stage' or 'my_stage/folder/')
    stage_filename_base: Base name for stage file (timestamp will be added)
    return_yaml_string: If True, return YAML string instead of dictionary (for direct API use)

    Returns: Generated semantic model dictionary or YAML string (if return_yaml_string=True)
    """
    # Create connection - only pass non-None parameters
    conn_params = {
        'account': account,
        'user': user
    }

    # Add authentication - PAT can be used as password replacement
    if token:
        # Use PAT as password (Snowflake drivers accept PAT as password replacement)
        conn_params['password'] = token
        print("🔐 Using Personal Access Token (PAT) as password replacement")
    elif password:
        conn_params['password'] = password
        print("🔐 Using traditional password authentication")
    else:
        raise ValueError("Either password or token (PAT) must be provided")

    # Add optional parameters
    if warehouse:
        conn_params['warehouse'] = warehouse
    if database:
        conn_params['database'] = database
    if schema:
        conn_params['schema'] = schema
    if role:
        conn_params['role'] = role

    conn = create_snowflake_connection(**conn_params)

    try:
        # Set warehouse if provided
        if warehouse:
            cursor = conn.cursor()
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            print(f"🏭 Using warehouse: {warehouse}")
            cursor.close()

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

        # Return YAML string if requested, otherwise return dictionary
        if return_yaml_string:
            return convert_to_yaml_string(dynamic_model)
        else:
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
    stage_filename_base: str = "semantic_model",
    return_yaml_string: bool = False
) -> Union[Dict[str, Any], str]:
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
    return_yaml_string: If True, return YAML string instead of dictionary (for direct API use)

    Returns: Generated semantic model dictionary or YAML string (if return_yaml_string=True)
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
        stage_filename_base=stage_filename_base,
        return_yaml_string=return_yaml_string
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


def convert_to_yaml_string(data: Dict[str, Any]) -> str:
    """
    Convert dictionary data to a YAML string.

    data: Dictionary to convert
    Returns: YAML string representation
    """
    try:
        yaml_string = yaml.dump(data, default_flow_style=False,
                                sort_keys=False, indent=2)
        return yaml_string
    except Exception as e:
        print(f"Error converting to YAML string: {e}")
        return ""


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


class CortexAnalystClient:
    """
    Client for interacting with Snowflake Cortex Analyst REST API.
    """

    def __init__(self, account_url: str, authorization_token: str,
                 token_type: str = "PROGRAMMATIC_ACCESS_TOKEN"):
        """
        Initialize the Cortex Analyst client.

        account_url: Your Snowflake account URL (e.g., 'https://myaccount.snowflakecomputing.com')
        authorization_token: PAT (Personal Access Token) or OAuth token
        token_type: Type of authorization token (default: "PROGRAMMATIC_ACCESS_TOKEN" for PAT)
        """
        self.account_url = account_url.rstrip('/')
        self.authorization_token = authorization_token
        self.token_type = token_type
        self.base_url = f"{self.account_url}/api/v2/cortex/analyst"

    def send_message(self,
                     question: str,
                     semantic_model: str = None,
                     semantic_model_file: str = None,
                     stream: bool = False,
                     conversation_history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Send a question to Cortex Analyst with a semantic model.

        question: The user's question
        semantic_model: YAML string of the semantic model (alternative to semantic_model_file)
        semantic_model_file: Path to semantic model file in stage (alternative to semantic_model)
        stream: Whether to use streaming response (default: False)
        conversation_history: Optional previous messages for multi-turn conversation

        Returns: API response dictionary
        """
        # Set up headers based on token type
        if self.token_type == "PROGRAMMATIC_ACCESS_TOKEN":
            headers = {
                "Authorization": f"Bearer {self.authorization_token}",
                "Content-Type": "application/json",
                "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN"
            }
        else:
            headers = {
                "Authorization": f"Bearer {self.authorization_token}",
                "Content-Type": "application/json",
                "X-Snowflake-Authorization-Token-Type": self.token_type
            }

        # Build messages array
        messages = []
        if conversation_history:
            messages.extend(conversation_history)

        # Add current user message
        messages.append({
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": question
                }
            ]
        })

        # Build payload - use either semantic_model or semantic_model_file
        payload = {
            "messages": messages,
            "stream": stream
        }

        if semantic_model_file:
            payload["semantic_model_file"] = semantic_model_file
        elif semantic_model:
            payload["semantic_model"] = semantic_model
        else:
            raise ValueError(
                "Either semantic_model or semantic_model_file must be provided")

        try:
            response = requests.post(
                f"{self.base_url}/message",
                headers=headers,
                json=payload,
                timeout=60
            )
            response.raise_for_status()

            if stream:
                return self._handle_streaming_response(response)
            else:
                return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Error calling Cortex Analyst API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response body: {e.response.text}")
            raise

    def _handle_streaming_response(self, response) -> Dict[str, Any]:
        """
        Handle streaming server-sent events response.

        response: Response object from requests
        Returns: Assembled response dictionary
        """
        # This is a simplified streaming handler
        # In practice, you'd want more sophisticated handling of server-sent events
        full_response = {
            "message": {
                "role": "analyst",
                "content": []
            },
            "warnings": [],
            "response_metadata": {}
        }

        try:
            for line in response.iter_lines(decode_unicode=True):
                if line.startswith('data: '):
                    event_data = line[6:]  # Remove 'data: ' prefix
                    try:
                        event_json = json.loads(event_data)
                        # Handle different event types as needed
                        # This is a simplified implementation
                        print(f"📡 Streaming event: {event_json}")
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"⚠️ Error processing streaming response: {e}")

        return full_response

    def send_feedback(self, request_id: str, positive: bool,
                      feedback_message: Optional[str] = None) -> bool:
        """
        Send feedback for a previous request.

        request_id: The request ID from a previous message call
        positive: True for positive feedback, False for negative
        feedback_message: Optional feedback text

        Returns: True if successful, False otherwise
        """
        headers = {
            "Authorization": f"Bearer {self.authorization_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "request_id": request_id,
            "positive": positive
        }

        if feedback_message:
            payload["feedback_message"] = feedback_message

        try:
            response = requests.post(
                f"{self.base_url}/feedback",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ Error sending feedback: {e}")
            return False


def generate_and_query_with_cortex_analyst(
    query: str,
    user_question: str,
    client: CortexAnalystClient,
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str = None,
    base_path: str = BASE_SEMANTIC_MODEL,
    facts_path: str = FACTS_FILE,
    stream: bool = False
) -> Dict[str, Any]:
    """
    Generate semantic model from Snowflake query and immediately use it with Cortex Analyst.

    query: SQL query to get fact names
    user_question: Question to ask Cortex Analyst
    client: Initialized CortexAnalystClient
    account, user, password, warehouse, database, schema: Snowflake connection parameters
    role: Optional Snowflake role
    base_path: Path to base semantic model YAML
    facts_path: Path to facts YAML file
    stream: Whether to use streaming response

    Returns: Cortex Analyst API response
    """
    # Generate semantic model
    print("🔄 Generating semantic model from Snowflake query...")
    semantic_model_dict = generate_semantic_model_from_snowflake(
        query=query,
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
        base_path=base_path,
        facts_path=facts_path,
        output_path=None,  # Don't save locally
        stage_name=None    # Don't upload to stage
    )

    if not semantic_model_dict:
        raise ValueError("Failed to generate semantic model")

    # Convert to YAML string
    print("📝 Converting semantic model to YAML string...")
    semantic_model_yaml = convert_to_yaml_string(semantic_model_dict)

    if not semantic_model_yaml:
        raise ValueError("Failed to convert semantic model to YAML string")

    # Query Cortex Analyst
    print("🤖 Sending question to Cortex Analyst...")
    response = client.send_message(
        question=user_question,
        semantic_model=semantic_model_yaml,
        stream=stream
    )

    return response


def test_with_config_file(
    config_path: str = CONFIG_FILE,
    connection_name: str = "my_example_connection",
    test_query: str = None,
    test_question: str = "What data is available in this model?",
    oauth_token: str = None,
    snowflake_pat: str = None
) -> None:
    """
    Test the semantic model generation and Cortex Analyst API using config file.

    config_path: Path to config.env file
    connection_name: Connection name in config file
    test_query: SQL query to generate semantic model (if None, uses sample tables query)
    test_question: Question to ask Cortex Analyst
    oauth_token: OAuth token for Cortex Analyst API (required for API test)
    """
    print("🧪 Testing Cortex Analyst integration with config file...")

    try:
        # Load configuration
        print("\n1️⃣ Loading configuration...")
        config = load_config_from_file(config_path, connection_name)

        # Test Snowflake connection
        print("\n2️⃣ Testing Snowflake connection...")

        # Build connection parameters - ONLY include the auth method we need
        conn_params = {
            'account': config['account'],
            'user': config['user'],
            'warehouse': config['warehouse'],
            'database': config['database'],
            'schema': config['schema'],
        }

        if config.get('role'):
            conn_params['role'] = config.get('role')

        # Add ONLY the authentication method we're using
        if snowflake_pat or config.get('token'):
            # Will be converted to password in create_snowflake_connection
            conn_params['token'] = snowflake_pat or config['token']
            print("🔐 Using Personal Access Token from config")
        elif config.get('password'):
            conn_params['password'] = config['password']
            print("🔐 Using password authentication")
        else:
            raise ValueError(
                "No authentication method found in config (need 'token' or 'password')")

        conn = create_snowflake_connection(**conn_params)

        # If no test query provided, use a sample query to list available tables
        if not test_query:
            test_query = f"""
                SELECT TABLE_NAME as ELEMENT_NUMBER 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{config['schema']}'
                AND TABLE_TYPE = 'BASE TABLE'
                LIMIT 5
            """
            print(
                f"📊 Using sample query to find tables in {config['database']}.{config['schema']}")

        print(f"\n3️⃣ Test Query: {test_query}")

        # Generate semantic model as YAML string
        print("\n4️⃣ Generating semantic model...")
        try:
            # Build connection params for semantic model generation
            gen_params = {
                'query': test_query,
                'account': config['account'],
                'user': config['user'],
                'warehouse': config['warehouse'],
                'database': config['database'],
                'schema': config['schema'],
                'role': config.get('role'),
                'return_yaml_string': True
            }

            # Add authentication method - only one of password or token
            if snowflake_pat or config.get('token'):
                gen_params['token'] = snowflake_pat or config['token']
                # Do NOT pass password when using token
                print("🔐 Using PAT authentication for semantic model generation")
            elif config.get('password'):
                gen_params['password'] = config['password']
                print("🔐 Using password authentication for semantic model generation")
            else:
                raise ValueError("No authentication method found in config")

            semantic_model_yaml = generate_semantic_model_from_snowflake(
                **gen_params)

            if semantic_model_yaml:
                print("✅ Semantic model generated successfully!")
                print(f"📄 Model length: {len(semantic_model_yaml)} characters")
                print("\n--- Generated Semantic Model (first 500 chars) ---")
                print(semantic_model_yaml[:500] + "..." if len(
                    semantic_model_yaml) > 500 else semantic_model_yaml)
                print("--- End Model Sample ---\n")
            else:
                print("❌ No semantic model generated (empty result)")
                return

        except Exception as e:
            print(f"❌ Error generating semantic model: {e}")
            return

        # Test Cortex Analyst API if OAuth token provided
        if oauth_token:
            print("5️⃣ Testing Cortex Analyst REST API...")

            # Build account URL from account identifier
            account_parts = config['account'].split('.')
            if len(account_parts) >= 2:
                account_url = f"https://{config['account']}.snowflakecomputing.com"
            else:
                account_url = f"https://{config['account']}.snowflakecomputing.com"

            try:
                client = CortexAnalystClient(
                    account_url=account_url,
                    authorization_token=oauth_token
                )

                print(f"🤖 Asking question: '{test_question}'")
                response = client.send_message(
                    question=test_question,
                    semantic_model=semantic_model_yaml
                )

                print("✅ Cortex Analyst API response received!")
                print("\n--- API Response ---")

                # Extract and display response content
                if 'message' in response and 'content' in response['message']:
                    for content in response['message']['content']:
                        if content.get('type') == 'text':
                            print(f"📝 Analysis: {content.get('text', 'N/A')}")
                        elif content.get('type') == 'sql':
                            print(
                                f"💾 Generated SQL: {content.get('statement', 'N/A')}")
                        elif content.get('type') == 'suggestions':
                            suggestions = content.get('suggestions', [])
                            if suggestions:
                                print(
                                    f"💡 Suggestions: {', '.join(suggestions)}")

                if 'warnings' in response and response['warnings']:
                    print(
                        f"⚠️ Warnings: {len(response['warnings'])} warning(s)")
                    for warning in response['warnings']:
                        print(
                            f"   - {warning.get('message', 'Unknown warning')}")

                if 'request_id' in response:
                    print(f"🔍 Request ID: {response['request_id']}")

                print("--- End API Response ---\n")

            except Exception as e:
                print(f"❌ Error calling Cortex Analyst API: {e}")
        else:
            print("5️⃣ Skipping Cortex Analyst API test (no OAuth token provided)")
            print("💡 To test the API, provide an OAuth token:")
            print("   test_with_config_file(oauth_token='your_oauth_token')")
            print("\n✅ Semantic model generation test completed successfully!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":

    print("\n🚀 Cortex Analyst API Integration Tool")
    print("=====================================")

    # Check if we should run the test
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # For Cortex Analyst API, use PAT from config instead of separate OAuth token
        print("🎯 Will test both semantic model generation and Cortex Analyst API")

        # Load config to get PAT token for API
        try:
            config = load_config_from_file()
            pat_token = config.get('token')

            if pat_token:
                print("✅ Found PAT token in config for Cortex Analyst API")
                test_with_config_file(oauth_token=pat_token)
            else:
                oauth_token = input(
                    "Enter PAT token for Cortex Analyst API (or press Enter to skip): ")
                test_with_config_file(
                    oauth_token=oauth_token if oauth_token.strip() else None)

        except Exception as e:
            print(f"⚠️ Could not load config: {e}")
            oauth_token = input(
                "Enter PAT token for Cortex Analyst API (or press Enter to skip): ")
            test_with_config_file(
                oauth_token=oauth_token if oauth_token.strip() else None)
    else:
        print("\nQuick Start:")
        print("1. Run semantic model generation test:")
        print("   python main.py test")
        print("\n2. For full API test, set OAuth token:")
        print("   export SNOWFLAKE_OAUTH_TOKEN='your_token'")
        print("   python main.py test")

    print("\nExample usage:")
    print("""
    # Method 1: Direct connection parameters
    # Query just needs to return ELEMENT_NUMBER column that matches fact names in facts.yaml
    query = '''
    SELECT ELEMENT_NUMBER
    FROM your_database.your_schema.data_dictionary_table
    WHERE condition = 'your_filter'
    '''

# Generate semantic model as YAML string for direct API use
semantic_model_yaml = generate_semantic_model_from_snowflake(
    query=query,
    account='your_account.region',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',
    return_yaml_string=True  # Returns YAML string instead of dictionary
)

# Now use with Cortex Analyst REST API
client = CortexAnalystClient(
    account_url='https://your_account.snowflakecomputing.com',
    authorization_token='your_oauth_token'
)

response = client.send_message(
    question="What was the total revenue last quarter?",
    semantic_model=semantic_model_yaml
)

print("Cortex Analyst Response:", response)

# Method 2: Traditional file-based approach
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

# Method 3: Using environment variables (recommended for API integration)
# Set these environment variables first:
# SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, 
# SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

# Get YAML string for API use
semantic_model_yaml = generate_semantic_model_from_env_and_query(
    query=query,
    return_yaml_string=True
)

# Method 4: Complete end-to-end workflow with environment variables
client = CortexAnalystClient(
    account_url='https://your_account.snowflakecomputing.com',
    authorization_token=os.getenv('SNOWFLAKE_OAUTH_TOKEN')
)

# This function does everything: generates model and queries Cortex Analyst
response = generate_and_query_with_cortex_analyst(
    query=query,
    user_question="Which products had the highest sales?",
    client=client,
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    stream=False  # Set to True for streaming responses
)

# Extract SQL and text from response
if 'message' in response and 'content' in response['message']:
    for content in response['message']['content']:
        if content.get('type') == 'sql':
            print("Generated SQL:", content.get('statement'))
        elif content.get('type') == 'text':
            print("Analysis:", content.get('text'))

# Send feedback if desired
if 'request_id' in response:
    client.send_feedback(
        request_id=response['request_id'],
        positive=True,
        feedback_message="Great analysis!"
    )

# The functions will:
# 1. Execute your query to get ELEMENT_NUMBER values
# 2. Look up matching facts in facts.yaml 
# 3. Combine with base.yaml to create the semantic model
# 4. Convert to YAML string (if return_yaml_string=True)
# 5. Send to Cortex Analyst REST API
# 6. Return the analysis results
        """)
