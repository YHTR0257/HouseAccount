import json
import os
from pathlib import Path

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/household')

# Directory paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / 'config'
DATA_DIR = BASE_DIR / 'data'
TEMP_UPLOADS_DIR = DATA_DIR / 'uploads'
CONFIRMED_DIR = DATA_DIR / 'confirmed'

# Create directories if they don't exist
TEMP_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
CONFIRMED_DIR.mkdir(parents=True, exist_ok=True)

# Load subject codes from JSON
with open(CONFIG_DIR / 'codes.json', 'r', encoding='utf-8') as f:
    codes_data = json.load(f)

# Convert to the format expected by the application
SUBJECT_CODES = {int(item['id']): name for name, item in codes_data.items()}

# Balance tolerance for validation
BALANCE_TOLERANCE = 0.01