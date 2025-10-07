from atproto import Client
from config import BSKY_USERNAME, BSKY_PASSWORD

def authenticate_client():
    """Authenticate with Bluesky API"""
    client = Client()
    print("Attempting to login...")
    client.login(BSKY_USERNAME, BSKY_PASSWORD)
    print("Login successful!")
    return client