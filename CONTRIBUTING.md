# Contributing to PostgreSQL to Snowflake Data Pipeline

Thank you for your interest in contributing to this project! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- Docker and Docker Compose
- Git
- Access to PostgreSQL, Snowflake, and AWS S3 (for testing)

### Development Setup

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/postgres-to-snowflake-data-pipeline.git
   cd postgres-to-snowflake-data-pipeline
   ```

3. Set up the development environment:
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Set up pre-commit hooks (optional)
   pip install pre-commit
   pre-commit install
   ```

## 📝 How to Contribute

### Reporting Issues

- Use the GitHub issue tracker
- Provide detailed information about the problem
- Include steps to reproduce the issue
- Add relevant logs and error messages

### Suggesting Enhancements

- Use the GitHub issue tracker with the "enhancement" label
- Describe the proposed feature clearly
- Explain the use case and benefits
- Consider backward compatibility

### Code Contributions

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Follow the existing code style
   - Add appropriate comments and documentation
   - Include tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   # Run tests
   pytest tests/ -v
   
   # Validate DAGs
   python -c "from airflow.models import DagBag; DagBag()"
   
   # Check code style
   flake8 . --max-line-length=127
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "Add: Brief description of your changes"
   ```

5. **Push and create a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   ```

## 📋 Code Style Guidelines

### Python Code

- Follow PEP 8 style guidelines
- Maximum line length: 127 characters
- Use meaningful variable and function names
- Add docstrings for functions and classes
- Include type hints where appropriate

### Airflow DAGs

- Use descriptive DAG and task names
- Include comprehensive docstrings
- Set appropriate retry and timeout values
- Use Airflow Variables for configuration
- Follow the existing task naming conventions

### Documentation

- Update README.md for significant changes
- Add inline comments for complex logic
- Update docstrings when modifying functions
- Include examples in documentation

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_specific.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

### Test Structure

- Unit tests for individual functions
- Integration tests for DAG workflows
- Mock external services (databases, S3, etc.)
- Test error handling and edge cases

## 📦 Pull Request Process

1. **Ensure your PR is ready**:
   - All tests pass
   - Code follows style guidelines
   - Documentation is updated
   - No merge conflicts

2. **Create a descriptive PR**:
   - Clear title and description
   - Reference related issues
   - Include screenshots for UI changes
   - Add testing instructions

3. **Respond to feedback**:
   - Address review comments promptly
   - Make requested changes
   - Ask questions if something is unclear

## 🏷️ Commit Message Format

Use clear, descriptive commit messages:

```
Add: New feature description
Fix: Bug fix description
Update: Update existing functionality
Remove: Remove deprecated code
Docs: Documentation changes
Test: Add or update tests
```

## 🐛 Bug Reports

When reporting bugs, please include:

- **Environment**: OS, Python version, Airflow version
- **Steps to reproduce**: Detailed steps
- **Expected behavior**: What should happen
- **Actual behavior**: What actually happens
- **Error messages**: Full error logs
- **Screenshots**: If applicable

## 💡 Feature Requests

When suggesting features:

- **Use case**: Why is this feature needed?
- **Proposed solution**: How should it work?
- **Alternatives**: Other approaches considered
- **Additional context**: Any other relevant information

## 📞 Getting Help

- **GitHub Issues**: For bugs and feature requests
- **Email**: amarciales56@gmail.com
- **LinkedIn**: [Andrés Marciales](https://www.linkedin.com/in/andres-marciales-de/)

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to PostgreSQL to Snowflake Data Pipeline! 🚀
