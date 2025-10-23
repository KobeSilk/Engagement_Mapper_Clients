import requests
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm

env_path = '.env'
load_dotenv(env_path)

ACCESS_TOKEN: str = os.getenv('ACCESS_TOKEN')   #Load LinkedIn Access token, now for me personally. 
LI_API_URL = "https://api.linkedin.com/rest/memberSnapshotData?q=criteria&domain=INBOX"  # Replace with Member Data Portability API endpoint

def getLinkedInConnections(ACCESS_TOKEN, API_URL):
    data = []
    headers = { #Headers for LinkedIn
        'Authorization':f"Bearer {ACCESS_TOKEN}",
        'Linkedin-Version':'202312',
        'Content-Type':'application/json'
    }
    r = requests.get(API_URL,headers=headers)
    print(r.json())
    #data = r.json()['elements'][0]['snapshotData']
    totalPages = r.json()['paging']['total']
    
    for page in tqdm(range(totalPages),desc="Loading LinkedIn Messages"):
        r = requests.get(f"{API_URL}&start={page}", headers=headers)
        data.extend(r.json()['elements'][0]['snapshotData'])
    return data

def convertMessages():
    LI_data = pd.DataFrame(getLinkedInConnections(ACCESS_TOKEN,LI_API_URL))
    messages_received = LI_data.groupby(['RECIPIENT PROFILE URLS']).agg({
    'TO':'last',
    'CONVERSATION ID': 'count',
    'DATE': 'max'}).sort_values('CONVERSATION ID',ascending = False).reset_index()
    
    messages_received.columns = ["Profile","Name","Messages received","last received"]

    messages_sent = LI_data.groupby(['SENDER PROFILE URL']).agg({
    'FROM':'last',
    'CONVERSATION ID': 'count',
    'DATE': 'max'}).sort_values('CONVERSATION ID',ascending = False).reset_index()

    messages_sent.columns = ["Profile","Name","Messages sent", "last sent"]
    # Merge on 'Profile'
    df = pd.merge(messages_sent, messages_received, on=['Profile','Name'], how='outer')
    df = df[~df["Profile"].str.contains(',')] #Remove groups
    df = df[df["Profile"].notna()]
    return df

if __name__ == "__main__":
    LI_leads = convertMessages()

