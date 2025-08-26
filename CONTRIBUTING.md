# Contributing to Dynamic Semantic Model Generator

Thank you for your interest in contributing to this project! This guide will help you get started.

## 🚀 Quick Start for Contributors

### 1. Clone and Setup
```bash
git clone <repository-url>
cd ca_dynamic_semantic_model
python setup.py  # Installs dependencies and runs tests
```

### 2. Development Environment
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -r requirements.txt
```

### 3. Running Tests
```bash
# Test core functionality
python test_snowflake_integration.py

# Test stage upload demo
python demo_stage_upload.py

# Run main script
python main.py
```

## 📋 How to Contribute

### Adding New Facts

1. **Edit facts.yaml**:
```yaml
- name: NEW_FACT_NAME
  expr: NEW_FACT_NAME
  data_type: NUMBER(10,0)
  description: Clear description of what this fact represents
  sample_values: ['123', '456', '789']
  synonyms: ['alias1', 'alias2', 'alternative_name']
```

2. **Test the new fact**:
```python
from main import get_facts_by_names
facts = get_facts_by_names(['NEW_FACT_NAME'])
assert len(facts) == 1
print("✅ New fact added successfully")
```

3. **Update documentation** if the fact represents a new category or pattern

### Improving Core Functions

1. **Make changes** to `main.py`
2. **Test thoroughly**:
   - Run existing test scripts
   - Test with different fact combinations
   - Test Snowflake integration (if you have access)
3. **Update examples** in `snowflake_example.py` if needed
4. **Update README.md** if adding new functionality

### Adding New Features

1. **Discuss first** - Create an issue to discuss the feature
2. **Follow existing patterns** - Look at how current functions are structured
3. **Add error handling** - Follow the existing error handling patterns
4. **Add documentation** - Update README.md and add code comments
5. **Add examples** - Show how to use the new feature

## 🔧 Code Style Guidelines

### Python Code Style
- Follow PEP 8 guidelines
- Use descriptive variable names
- Add docstrings to all functions
- Include type hints where helpful

### Example Function Structure
```python
def new_function(param1: str, param2: int = None) -> Dict[str, Any]:
    """
    Brief description of what the function does.
    
    param1: Description of first parameter
    param2: Optional parameter description
    
    Returns: Description of return value
    """
    try:
        # Implementation here
        result = {}
        print(f"✅ Success message")
        return result
        
    except Exception as e:
        print(f"❌ Error in new_function: {e}")
        raise
```

### YAML Style
- Use consistent indentation (2 spaces)
- Quote string values that might be interpreted as other types
- Keep descriptions clear and concise
- Use meaningful fact names (UPPER_SNAKE_CASE)

## 🧪 Testing Guidelines

### Before Submitting
1. **Run setup script**: `python setup.py`
2. **Test fact operations**: Verify fact lookup and model generation
3. **Test edge cases**: Empty results, missing facts, connection failures
4. **Test examples**: Ensure example scripts work correctly

### Test Data
- Use realistic but not sensitive fact names
- Include edge cases in test scenarios
- Test with different data types (NUMBER, VARCHAR, FLOAT, etc.)

## 📝 Documentation Standards

### Code Comments
- Explain **why**, not just what
- Document any complex logic or business rules
- Include examples in docstrings for complex functions

### README Updates
- Keep examples current and working
- Update function tables when adding new functions
- Include new features in the feature list
- Update troubleshooting section for new error cases

## 🚦 Pull Request Process

### 1. Preparation
- [ ] Fork the repository
- [ ] Create a feature branch: `git checkout -b feature/your-feature-name`
- [ ] Make your changes
- [ ] Test thoroughly

### 2. Submission
- [ ] Commit with clear messages: `git commit -m "Add: description of changes"`
- [ ] Push to your fork: `git push origin feature/your-feature-name`
- [ ] Create a pull request with:
  - Clear title and description
  - List of changes made
  - Test results
  - Any breaking changes

### 3. Review Process
- Code will be reviewed by maintainers
- Address any feedback promptly
- Be open to suggestions and improvements
- Maintain a positive, collaborative attitude

## 🎯 Specific Contribution Areas

### High Priority
- **Error handling improvements**: Better error messages and recovery
- **Performance optimization**: Faster YAML processing, efficient fact lookup
- **Additional data sources**: Support for other data dictionary formats
- **Testing framework**: Automated tests for all functionality

### Medium Priority
- **UI/CLI interface**: Command-line interface for easier usage
- **Configuration management**: Better config file support
- **Logging improvements**: Structured logging with different levels
- **Documentation**: Video tutorials, more examples

### Low Priority
- **Additional output formats**: JSON, XML output options
- **Integration examples**: Integration with other tools
- **Advanced features**: Conditional fact inclusion, fact transformations

## 🆘 Getting Help

### Questions or Issues
1. **Check existing issues** in the repository
2. **Review README.md** and existing documentation
3. **Run the setup script** to verify your environment
4. **Create a new issue** with:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Your environment details

### Discussion Areas
- **General questions**: Use repository discussions
- **Bug reports**: Create an issue with the bug template
- **Feature requests**: Create an issue with the feature template
- **Code questions**: Comment on specific lines in pull requests

## 🎉 Recognition

Contributors will be:
- Listed in the repository contributors
- Mentioned in release notes for significant contributions
- Given credit in documentation for major features

Thank you for helping make this project better! 🚀
