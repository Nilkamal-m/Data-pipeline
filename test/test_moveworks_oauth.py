import json
import urllib.request
import urllib.parse

# ==============================================================================
# MANUALLY PASTE YOUR MOVEWORKS CREDENTIALS & DETAILS HERE
# ==============================================================================
CLIENT_ID = "YOUR_CLIENT_ID_HERE"
CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
TOKEN_URL = "https://api.moveworks.ai/rest/v1/oauth/token"
BASE_URL = "https://api.moveworks.ai"
ENTITY_NAME = "tickets"
# ==============================================================================


def get_oauth_token():
    """Step 1: Get OAuth 2.0 Access Token using Client Credentials Flow"""
    print(f"1. Requesting OAuth token from: {TOKEN_URL}")

    token_data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }).encode("utf-8")

    req = urllib.request.Request(
        TOKEN_URL,
        data=token_data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST"
    )

    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode("utf-8"))
        access_token = res["access_token"]
        print("   SUCCESS! Received Access Token:", access_token[:15] + "...")
        return access_token


def fetch_moveworks_data(access_token):
    """Step 2: Fetch Records from Moveworks API using Bearer Token"""
    url = f"{BASE_URL.rstrip('/')}/export/v1/records/{ENTITY_NAME}?$limit=5"
    print(f"\n2. Fetching records from: {url}")

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        },
        method="GET"
    )

    with urllib.request.urlopen(req) as response:
        res = json.loads(response.read().decode("utf-8"))
        records = res.get("value", [])
        print(f"   SUCCESS! Fetched {len(records)} records.")
        print("\nSample Data:")
        print(json.dumps(records[:2], indent=2))


if __name__ == "__main__":
    try:
        token = get_oauth_token()
        fetch_moveworks_data(token)
    except Exception as e:
        print("\n❌ Error:", e)
