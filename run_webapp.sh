#!/bin/bash
# Run the VA Polling Validator web application

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -e ".[webapp]"
    playwright install chromium
else
    source venv/bin/activate
fi

echo ""
echo "==================================="
echo "VA Polling Place Validator"
echo "==================================="
echo ""
echo "Starting backend server on http://localhost:8000"
echo "Open webapp/frontend/index.html in your browser"
echo ""
echo "Press Ctrl+C to stop"
echo ""

cd webapp/backend
python main.py
