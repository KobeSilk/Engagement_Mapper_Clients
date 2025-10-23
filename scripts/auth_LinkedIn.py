try:
    import requests
    from dotenv import load_dotenv, set_key, dotenv_values
    import os
    import re
    import time
except Exception as e:
    print("An error occurred:", e)
    input("Press Enter to exit...")

env_path = '.env'
load_dotenv(env_path,override=True,verbose=True)
env_vars = dotenv_values(".env")

# Replace with your LinkedIn API credentials
CLIENT_ID: str = os.getenv('CLIENT_ID')
CLIENT_SECRET: str = os.getenv('CLIENT_SECRET')
ACCESS_TOKEN: str = os.getenv("ACCESS_TOKEN")
REDIRECT_URI = "https://wearesilk.be/privacy/"  # Ensure this matches your LinkedIn app settings
AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
API_URL = "https://api.linkedin.com/rest/memberSnapshotData?domain=CONNECTIONS&q=criteria"  # Replace with Member Data Portability API endpoint

if "ACCESS_TOKEN" in env_vars:
    print("There is an access token set, you're all good.")
    input("Press enter to exit...")
else:
    print("There is no LinkedIn Access token set.")

    auth_headers = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state":"foobbar",
        "scope": "r_dma_portability_3rd_party",  # Adjust scopes as needed
    }

    r = requests.Request("GET",AUTH_URL,params=auth_headers).prepare().url

    print("Welcome to the LinkedIn API connector, in order to set up please click the following link and provide access (ctrl+click): \n\n", r)
    time.sleep(7)
    print("\nAfter signing in and giving access, the Wearesilk website should open up, please paste the whole link below.")
    auth_token = re.search('code=(.*)&state=',input("Please paste the full link below: ")).group(1)

    access_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    access_data = {
        "grant_type":"authorization_code",
        "code":auth_token,
        "client_id":CLIENT_ID,
        "client_secret":CLIENT_SECRET,
        "redirect_uri":REDIRECT_URI
    }

    r = requests.post(url=TOKEN_URL,data=access_data,headers=access_headers)

    r.json()
    access_token = r.json()['access_token']
    key = 'ACCESS_TOKEN'
    value = access_token
    set_key(env_path,key,value)
    input("Access token has been set, you now have access to sync your LinkedIn contacts. \nPress enter to exit... ")

