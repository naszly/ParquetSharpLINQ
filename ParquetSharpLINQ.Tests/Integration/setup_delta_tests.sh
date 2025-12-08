#!/bin/bash
# Setup script for Delta Lake integration tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

echo "=================================================="
echo "Delta Lake Integration Tests - Setup"
echo "=================================================="
echo ""

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

echo "Python 3 found: $(python3 --version)"
echo ""

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Virtual environment created"
else
    echo "Virtual environment already exists"
fi

echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip --quiet
pip install pandas pyarrow deltalake --quiet

echo "Dependencies installed"
echo ""

# Generate test data
echo "=================================================="
echo "Generating Delta Lake test data..."
echo "=================================================="
echo ""

python3 "$SCRIPT_DIR/generate_delta_test_data.py"

echo ""
echo "=================================================="
echo "Setup complete!"
echo "=================================================="
echo ""
echo "To run the Delta integration tests:"
echo "  dotnet test --filter \"Category=Integration&FullyQualifiedName~Delta\""
echo ""
echo "To regenerate test data later:"
echo "  source $VENV_DIR/bin/activate"
echo "  python3 $SCRIPT_DIR/generate_delta_test_data.py"
echo ""
