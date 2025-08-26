# Dynamic Semantic Model Generator

Customers with over 500 columns in a table run into semantic model file size limits due to LLM context window limits. 

To solve this, we use a Python tool for dynamically generating semantic models by combining base configurations with selected facts from data dictionaries. Includes Snowflake integration for querying data dictionaries and uploading generated models to stages.

## 🚀 Features

- **Dynamic Fact Selection**: Choose specific facts from a pre-defined facts library
- **Snowflake Integration**: Query data dictionaries directly from Snowflake
- **Stage Upload**: Automatically upload generated models to Snowflake stages with unique timestamps
- **Flexible Configuration**: Support for environment variables and direct credentials
- **YAML Management**: Robust YAML parsing, generation, and validation
- **Batch Processing**: Generate multiple models with different fact combinations

## 📁 Project Structure

```
ca_dynamic_semantic_model/
├── main.py                          # Core functionality
├── base.yaml                        # Base semantic model template
├── facts.yaml                       # Library of pre-defined facts
├── snowflake_example.py            # Usage examples with Snowflake
├── demo_stage_upload.py            # Demo of stage upload functionality
├── test_snowflake_integration.py   # Test script for workflow simulation
└── README.md                       # This file
```

## 🛠️ Installation

### Requirements

```bash
pip install snowflake-connector-python pandas pyyaml
```

### Environment Setup

1. Clone this repository:
```bash
git clone <repository-url>
cd ca_dynamic_semantic_model
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Snowflake environment variables (optional but recommended):
```bash
export SNOWFLAKE_ACCOUNT=your_account.region
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=your_schema
export SNOWFLAKE_ROLE=your_role  # Optional
```

## 🎯 Quick Start

### 1. Generate Model with Specific Facts

```python
from main import generate_dynamic_semantic_model

# Select specific facts from facts.yaml
fact_names = ["LOAN_AMOUNT", "INCOME", "MORTGAGERESPONSE"]
model = generate_dynamic_semantic_model(
    fact_names=fact_names,
    output_path="my_semantic_model.yaml"
)
```

### 2. Generate Model from Snowflake Data Dictionary

```python
from main import generate_semantic_model_from_snowflake

# Query data dictionary and generate model
query = """
SELECT ELEMENT_NUMBER
FROM your_database.your_schema.data_dictionary_table
WHERE table_name = 'your_target_table'
"""

model = generate_semantic_model_from_snowflake(
    query=query,
    account='your_account.region',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema',
    output_path='generated_model.yaml'
)
```

### 3. Generate and Upload to Snowflake Stage

```python
from main import generate_semantic_model_from_env_and_query

# Uses environment variables for connection
model = generate_semantic_model_from_env_and_query(
    query=query,
    output_path='local_model.yaml',
    stage_name='@my_models_stage/semantic_models/',
    stage_filename_base='mortgage_model'
)
# Creates: mortgage_model_20241215_143532.yaml in the stage
```

## 📋 Core Components

### Base Configuration (`base.yaml`)

Contains the foundational semantic model structure:
- Model name and description
- Table definitions
- Time dimensions
- Verified queries

### Facts Library (`facts.yaml`)

Pre-defined facts with complete definitions:
- Fact names, expressions, and data types
- Descriptions and sample values
- Synonyms for natural language queries

### Dynamic Generation Process

1. **Query Execution**: Run SQL query against Snowflake data dictionary
2. **Fact Extraction**: Extract `ELEMENT_NUMBER` values from query results
3. **Fact Lookup**: Match against pre-defined facts in `facts.yaml`
4. **Model Assembly**: Combine `base.yaml` with selected facts
5. **Output Generation**: Save locally and/or upload to Snowflake stage

## 🔧 Configuration Options

### Snowflake Connection

#### Method 1: Environment Variables (Recommended)
```bash
export SNOWFLAKE_ACCOUNT=abc123.us-east-1
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=ANALYTICS_DB
export SNOWFLAKE_SCHEMA=DATA_DICTIONARY
```

#### Method 2: Direct Parameters
```python
connection_params = {
    'account': 'abc123.us-east-1',
    'user': 'your_user',
    'password': 'your_password',
    'warehouse': 'COMPUTE_WH',
    'database': 'ANALYTICS_DB',
    'schema': 'DATA_DICTIONARY'
}
```

### Stage Upload Configuration

#### Stage Name Formats
- `@my_stage` - Root of named stage
- `@my_stage/models/` - Subfolder in named stage
- `my_stage/folder/` - Auto-adds @ prefix
- `@%temp_stage` - User stage

#### Filename Generation
Files are automatically timestamped: `{base_name}_{YYYYMMDD_HHMMSS}.yaml`

Examples:
- `semantic_model_20241215_143532.yaml`
- `mortgage_facts_20241215_143532.yaml`

## 📊 Data Dictionary Requirements

Your Snowflake data dictionary query must return an `ELEMENT_NUMBER` column containing fact names that match entries in `facts.yaml`.

### Example Query
```sql
SELECT ELEMENT_NUMBER
FROM analytics_db.data_dictionary.table_metadata
WHERE table_name = 'mortgage_lending'
  AND is_active = true
ORDER BY element_number
```

### Expected Results
```
┌─────────────────┐
│ ELEMENT_NUMBER  │
├─────────────────┤
│ LOAN_AMOUNT     │
│ INCOME          │
│ MORTGAGERESPONSE│
│ HIGH_INCOME_FLAG│
└─────────────────┘
```

## 🎪 Usage Examples

### Example 1: Loan Analysis Model
```python
# Generate model focused on loan analysis
loan_facts = ["LOAN_AMOUNT", "INCOME", "INCOME_LOAN_RATIO", "MORTGAGERESPONSE"]
loan_model = generate_dynamic_semantic_model(
    fact_names=loan_facts,
    output_path="loan_analysis_model.yaml"
)
```

### Example 2: Customer Demographics Model
```python
# Generate model for customer demographics
demo_facts = ["INCOME", "HIGH_INCOME_FLAG", "MEAN_COUNTY_INCOME"]
demo_model = generate_dynamic_semantic_model(
    fact_names=demo_facts,
    output_path="customer_demographics_model.yaml"
)
```

### Example 3: Production Pipeline with Stage Upload
```python
# Production workflow with automatic stage upload
query = """
SELECT ELEMENT_NUMBER 
FROM prod_db.metadata.data_dictionary 
WHERE model_type = 'mortgage_lending'
"""

model = generate_semantic_model_from_env_and_query(
    query=query,
    output_path='production_model.yaml',
    stage_name='@prod_models/mortgage/',
    stage_filename_base='mortgage_semantic_model'
)
```

## 🔍 Available Functions

### Core Functions

| Function | Description |
|----------|-------------|
| `generate_dynamic_semantic_model()` | Generate model from specific fact names |
| `generate_semantic_model_from_snowflake()` | Generate model from Snowflake query with direct credentials |
| `generate_semantic_model_from_env_and_query()` | Generate model from Snowflake query using environment variables |
| `list_available_facts()` | List all facts available in facts.yaml |
| `get_facts_by_names()` | Retrieve specific facts by name |

### Utility Functions

| Function | Description |
|----------|-------------|
| `generate_unique_filename()` | Create timestamp-based unique filenames |
| `upload_to_snowflake_stage()` | Upload files to Snowflake stages |
| `save_and_upload_to_stage()` | Save locally and upload to stage |
| `load_yaml()` | Parse YAML files with error handling |

## 🚦 Error Handling

The system includes comprehensive error handling:

- **Connection Errors**: Graceful handling of Snowflake connection failures
- **Query Errors**: Clear error messages for SQL execution problems
- **File Errors**: Validation for YAML parsing and file operations
- **Stage Upload Errors**: Continues processing if stage upload fails
- **Missing Facts**: Warnings for facts not found in facts.yaml

## 🧪 Testing

### Run Tests
```bash
# Test the workflow without Snowflake connection
python test_snowflake_integration.py

# Demo stage upload functionality
python demo_stage_upload.py

# View usage examples
python snowflake_example.py
```

### Validate Your Setup
```bash
# Check available facts
python -c "from main import list_available_facts; print(list_available_facts())"

# Test YAML parsing
python main.py
```

## 🤝 Contributing

### Adding New Facts

1. Edit `facts.yaml` to add new fact definitions:
```yaml
- name: NEW_FACT_NAME
  expr: NEW_FACT_NAME
  data_type: NUMBER(10,0)
  description: Description of the new fact
  sample_values: ['123', '456', '789']
  synonyms: ['alias1', 'alias2']
```

2. Test the new fact:
```python
from main import get_facts_by_names
facts = get_facts_by_names(['NEW_FACT_NAME'])
print(facts)
```

### Modifying Base Configuration

Edit `base.yaml` to update:
- Model name and description
- Table structure
- Time dimensions
- Verified queries

## 📚 Best Practices

### 1. Fact Naming Convention
- Use consistent naming (e.g., `SNAKE_CASE`)
- Match fact names exactly between data dictionary and facts.yaml
- Include descriptive fact names

### 2. Stage Organization
```
@production_models/
├── mortgage_lending/
├── customer_analytics/
└── risk_assessment/
```

### 3. Environment Management
- Use environment variables for production
- Store credentials securely
- Use different environments for dev/test/prod

### 4. Version Control
- Commit generated models for auditability
- Tag releases with semantic versioning
- Document fact library changes

## 🔧 Troubleshooting

### Common Issues

#### "No facts found in facts.yaml"
- Check that ELEMENT_NUMBER values from your query match fact names in facts.yaml
- Verify facts.yaml format is correct

#### "Snowflake connection failed"
- Verify environment variables are set correctly
- Check network connectivity to Snowflake
- Validate account, warehouse, and database names

#### "Stage upload failed"
- Ensure stage exists in Snowflake
- Check permissions for PUT operations
- Verify stage name format (include @ prefix)

### Debug Mode
```python
# Enable detailed logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Run your generation code
```

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review example scripts in the repository
3. Create an issue in the repository
4. Contact the team

## 🎉 Getting Started Checklist

- [ ] Install required packages
- [ ] Set up Snowflake environment variables  
- [ ] Test connection with `python main.py`
- [ ] Review available facts with `list_available_facts()`
- [ ] Run your first query with sample data
- [ ] Generate your first semantic model
- [ ] Upload to a Snowflake stage
- [ ] Share with your team!

---

**Happy modeling! 🚀**
# dynamic_semantic_model_creation
