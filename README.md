# Cortex Analyst Dynamic Semantic Model Generator

A Python tool for dynamically generating semantic models and integrating with Snowflake's Cortex Analyst REST API. This tool allows you to create semantic models from Snowflake data dictionary queries and use them directly with Cortex Analyst for natural language data analysis.

## Features

- 🚀 **Dynamic Semantic Model Generation**: Generate semantic models from Snowflake queries
- 🤖 **Cortex Analyst Integration**: Direct integration with Snowflake's Cortex Analyst REST API  
- 🔐 **Multiple Authentication Methods**: Support for Personal Access Tokens (PAT) and password authentication
- 📊 **Flexible Data Sources**: Query your data dictionary to dynamically select facts
- 🎯 **Easy Configuration**: Simple configuration file setup
- ⚡ **API-First Design**: Generate YAML strings for direct API use (no file uploads required)

## Quick Start

### 1. Installation

```bash
pip install -r requirements.txt
```

### 2. Configuration

Copy the configuration template and fill in your Snowflake details:

```bash
cp config.env.template config.env
```

Edit `config.env` with your Snowflake connection details:

```ini
[connections.my_example_connection]
account = your_account_identifier.region
user = your_username
token = your_personal_access_token
role = your_role_name
warehouse = your_warehouse_name  
database = your_database_name
schema = your_schema_name
```

### 3. Get Your Personal Access Token (PAT)

1. Log into Snowsight
2. Go to **Account** > **Security** > **Personal Access Tokens**
3. Create a new token and copy it to your config file

### 4. Test the Integration

Test with a pre-built semantic model:
```bash
python test_api_only.py
```

Test full integration (requires Snowflake connection):
```bash
python main.py test
```

## Usage Examples

### Generate Semantic Model from Query

```python
from main import generate_semantic_model_from_snowflake

# Generate semantic model from a data dictionary query
query = """
SELECT ELEMENT_NUMBER 
FROM your_database.your_schema.data_dictionary_table 
WHERE condition = 'your_filter'
"""

# Option 1: Return as YAML string for direct API use
semantic_model_yaml = generate_semantic_model_from_snowflake(
    query=query,
    account='your_account.region',
    user='your_username',
    token='your_pat_token',
    warehouse='your_warehouse',
    database='your_database', 
    schema='your_schema',
    return_yaml_string=True
)

# Option 2: Use environment variables
model = generate_semantic_model_from_env_and_query(
    query=query,
    return_yaml_string=True
)
```

### Use with Cortex Analyst API

```python
from main import CortexAnalystClient

# Initialize client with PAT
client = CortexAnalystClient(
    account_url='https://your_account.snowflakecomputing.com',
    authorization_token='your_pat_token',
    token_type='PROGRAMMATIC_ACCESS_TOKEN'
)

# Ask questions about your data
response = client.send_message(
    question="What is the total revenue last quarter?",
    semantic_model=semantic_model_yaml
)

# Extract SQL and analysis
for content in response['message']['content']:
    if content['type'] == 'sql':
        print(f"Generated SQL: {content['statement']}")
    elif content['type'] == 'text':
        print(f"Analysis: {content['text']}")
```

### Complete End-to-End Workflow

```python
from main import generate_and_query_with_cortex_analyst, CortexAnalystClient

client = CortexAnalystClient(
    account_url='https://your_account.snowflakecomputing.com',
    authorization_token='your_pat_token'
)

# This does everything: generates model and queries Cortex Analyst
response = generate_and_query_with_cortex_analyst(
    query="SELECT ELEMENT_NUMBER FROM data_dictionary WHERE active=1",
    user_question="Which products had the highest sales?",
    client=client,
    account='your_account',
    user='your_username',
    token='your_pat_token',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'
)
```

## Environment Variables

You can use environment variables instead of the config file:

```bash
export SNOWFLAKE_ACCOUNT=your_account.region
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PAT=your_personal_access_token
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=your_schema
```

## File Structure

```
ca_dynamic_semantic_model/
├── main.py                           # Main library with all functions
├── test_api_only.py                  # Test Cortex Analyst API only
├── config.env.template               # Configuration template
├── config.env                        # Your configuration (do not commit)
├── base.yaml                         # Base semantic model template
├── facts.yaml                        # Predefined facts library
├── generated_model_example1.yaml     # Example generated model
├── generated_model_loan_focus.yaml   # Another example model
└── requirements.txt                  # Python dependencies
```

## Key Functions

- `generate_semantic_model_from_snowflake()`: Generate model from Snowflake query
- `generate_semantic_model_from_env_and_query()`: Generate using environment variables
- `CortexAnalystClient`: REST API client for Cortex Analyst
- `generate_and_query_with_cortex_analyst()`: End-to-end workflow

## Authentication

### Personal Access Token (PAT) - Recommended
- Generate in Snowsight under Account > Security > Personal Access Tokens
- **PATs work as password replacements** in Snowflake drivers, APIs, and libraries
- Best for API integrations and automation
- Use `token` parameter in functions (automatically converted to password replacement)

### Password Authentication
- Traditional username/password
- Use `password` parameter in functions

**Note:** According to Snowflake documentation, PATs can be used as a replacement for passwords in:
- Snowflake drivers (including snowflake-connector-python)
- Third-party applications (Tableau, PowerBI, etc.)
- Snowflake APIs and libraries (Snowpark, Python API)
- Command-line clients (Snowflake CLI, SnowSQL)

## Troubleshooting

### Common Issues

1. **"Invalid OAuth access token"**: Your PAT token may be expired. Generate a new one.
2. **"Password is empty"**: Make sure you're using either `token` OR `password`, not both.
3. **"No semantic model files found"**: Ensure your YAML files are in the same directory as the scripts.

### Debug Mode

Run with debug information:
```bash
python -u test_api_only.py
```

## Requirements

- Python 3.7+
- Snowflake account with Cortex Analyst enabled
- Personal Access Token or username/password
- Required Python packages (see requirements.txt)

## Security Notes

- Never commit your `config.env` file with real credentials
- Use Personal Access Tokens instead of passwords when possible
- Store sensitive information in environment variables in production
- The provided `config.env` contains only placeholder values

## Contributing

1. Fork the repository
2. Create your feature branch
3. Remove any hardcoded credentials before committing
4. Test with the provided test scripts
5. Submit a pull request

## License

This project is provided as-is for educational and development purposes.