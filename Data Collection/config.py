import os
from datetime import datetime, timezone

# Configuration constants
OUTPUT_DIR = './users'
CSV_DIR = './users/csv'
START_DATE = datetime(2024, 2, 1, tzinfo=timezone.utc)
END_DATE = datetime(2025, 2, 1, tzinfo=timezone.utc)
MAX_USERS = 500

# Bluesky credentials (should use environment variables in production)
BSKY_USERNAME = 'yourname.bsky.social'
BSKY_PASSWORD = 'yourpassword'

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)


