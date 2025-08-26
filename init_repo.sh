#!/bin/bash

# Dynamic Semantic Model Generator - Repository Initialization Script
# This script helps initialize the Git repository and prepare it for sharing

echo "🚀 Initializing Dynamic Semantic Model Generator Repository"
echo "============================================================"

# Check if Git is installed
if ! command -v git &> /dev/null; then
    echo "❌ Git is not installed. Please install Git first."
    exit 1
fi

# Initialize Git repository if not already initialized
if [ ! -d ".git" ]; then
    echo "📁 Initializing Git repository..."
    git init
    echo "✅ Git repository initialized"
else
    echo "📁 Git repository already exists"
fi

# Add all files to Git
echo "📋 Adding files to Git..."
git add .

# Create initial commit
echo "💾 Creating initial commit..."
if git diff --staged --quiet; then
    echo "⚠️  No changes to commit"
else
    git commit -m "Initial commit: Dynamic Semantic Model Generator

Features:
- Dynamic semantic model generation from fact libraries
- Snowflake integration for data dictionary queries
- Stage upload with timestamp-based unique filenames
- Environment variable configuration support
- Comprehensive examples and documentation

Components:
- main.py: Core functionality
- base.yaml: Base semantic model template
- facts.yaml: Pre-defined facts library
- README.md: Complete documentation
- requirements.txt: Python dependencies
- examples/: Usage examples and demos"
    
    echo "✅ Initial commit created"
fi

# Show repository status
echo ""
echo "📊 Repository Status:"
git status --short

# Show available branches
echo ""
echo "🌿 Available branches:"
git branch

# Display next steps
echo ""
echo "🎯 Next Steps:"
echo "=============="
echo ""
echo "1. 📤 Add remote repository:"
echo "   git remote add origin <your-repository-url>"
echo ""
echo "2. 🔄 Push to remote:"
echo "   git push -u origin main"
echo ""
echo "3. 👥 Share with teammates:"
echo "   Send them the repository URL and they can run:"
echo "   git clone <repository-url>"
echo "   cd ca_dynamic_semantic_model"
echo "   python setup.py"
echo ""
echo "4. 🔧 Set up Snowflake credentials:"
echo "   cp .env.example .env"
echo "   # Edit .env with actual Snowflake credentials"
echo ""
echo "5. 🧪 Test the setup:"
echo "   python main.py"
echo "   python snowflake_example.py"
echo ""
echo "📚 Documentation:"
echo "   - README.md: Complete usage guide"
echo "   - CONTRIBUTING.md: Guide for contributors"
echo "   - examples/: Working code examples"
echo ""
echo "🎉 Repository ready for sharing!"

# Optional: Create example branch structure
read -p "🤔 Create example branch structure for team workflow? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🌿 Creating branch structure..."
    
    # Create development branch
    git checkout -b development
    echo "✅ Created 'development' branch"
    
    # Go back to main
    git checkout main
    echo "✅ Switched back to 'main' branch"
    
    echo ""
    echo "📋 Branch structure created:"
    echo "   - main: Production-ready code"
    echo "   - development: Active development"
    echo ""
    echo "💡 Team workflow suggestion:"
    echo "   1. Create feature branches from 'development'"
    echo "   2. Merge feature branches back to 'development'"
    echo "   3. Merge 'development' to 'main' for releases"
fi

echo ""
echo "✨ Repository initialization complete!"
echo "📖 See README.md for detailed usage instructions"
