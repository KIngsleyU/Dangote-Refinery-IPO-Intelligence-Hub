import requests
import os
import dotenv
dotenv.load_dotenv()
# 1. Obtain initial tokens
def get_acled_token(email, password):
    url = "https://acleddata.com/oauth/token"
    payload = {"username": email, "password": password, "grant_type": "password", "client_id": "acled"}
    response = requests.post(url, data=payload)
    return response.json() # Contains access_token and refresh_token

# 2. Refresh token before expiration
def refresh_acled_token(refresh_token):
    url = "https://acleddata.com/oauth/token"
    payload = {"refresh_token": refresh_token, "grant_type": "refresh_token", "client_id": "acled"}
    response = requests.post(url, data=payload)
    return response.json()

# Example usage
tokens = get_acled_token(os.getenv("EMAIL"), os.getenv("PASSWORD"))
print("--------------------------------")
print("Tokens:")
print(tokens)
new_tokens = refresh_acled_token(tokens['refresh_token'])
print("--------------------------------")
print("New Tokens:")
print(new_tokens)