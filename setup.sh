#!/bin/bash

echo "Starting environment setup for E-Commerce Data Pipeline..."

# Stop script if any command fails
set -e

# Create virtual environment if it does not exist
if [ ! -d "venv" ]; then
  echo "Creating Python virtual environment..."
  python3 -m venv venv
fi

# Activate virtual environment (Linux/Mac/Windows Git Bash)
source venv/Scripts/activate 2>/dev/null || source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# Install Python dependencies
echo "Installing required Python packages..."
pip install -r requirements.txt

# Create required directories
echo "Creating required directories..."
mkdir -p logs data/raw data/staging data/processed

# Create .env file if missing
if [ ! -f ".env" ]; then
  echo ".env file not found. Creating from .env.example..."
  cp .env.example .env
  echo "⚠️ Please update the .env file with your database credentials"
fi

echo "Environment setup completed successfully."