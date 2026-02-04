import pandas as pd
#import load_LI_messages as LI #Additional
#import load_pipedrive as PD #Additional
import load_phantombuster as PB 
import requests
import json
import os
from dotenv import load_dotenv

env_path = '.env'
load_dotenv(env_path)

personal_scraping_id: str = os.getenv('SCRAPING_PERSONAL') #Phantom ID
company_scraping_id: str = os.getenv ('SCRAPING_COMPANY')  #Phantom ID

def extract_rightmost_part(s):
    try:
        s = s.rstrip('/')
        s = s.split('/')[-1]
    except:
        s
    return s

#LI_messages = LI.convertMessages() # Load LinkedIn Messages
#PD_leads = PD.convert_leads() #  Load Pipedrive Leads

#Load Phantom Buster Data
PB_scraping = PB.all_posts_list()

# Denote the columns to be used for merging using extract_rightmost_part function
#PD_leads["linkedin_identifier"] = PD_leads["linkedin_url"].apply(extract_rightmost_part)
#LI_messages["linkedin_identifier"] = LI_messages["Profile"].apply(extract_rightmost_part)

#Merge Phantombuster scraping with Pipedrive Leads on Naqme and on LinkedIn identifier
#PB_PD_on_LI = pd.merge(left=PB_scraping,right=PD_leads,how='left',left_on="linkedin_identifier",right_on="linkedin_identifier").dropna(subset="name")
#PB_PD_on_name = pd.merge(left=PB_scraping,right=PD_leads,how='left',left_on="fullName",right_on="name")
#PB_PD_on_name = PB_PD_on_name.rename(columns = {"linkedin_identifier_x":"linkedin_identifier"})
#Merge Phantombuster matching on name and LI 
#concat_PB = pd.concat([ PB_PD_on_name,PB_PD_on_LI], ignore_index=True).sort_values(by="id")
merge_LI_messages = PB_scraping.drop_duplicates(subset=["linkedin_identifier"])

#merge_LI_messages = merge_LI_messages[["linkedin_identifier","profileUrl","firstName","lastName","fullName","degree","occupation","first_engagement","last_engagement","lastCommentedAt","commentUrl_list","comments_list","likescount_silk","likescount_hanne","commentcount_silk","commentcount_hanne","name","add_time","org_name","Messages sent","last sent","Messages received","last received"]]

merge_LI_messages.to_csv("exports/export engagement.csv")

BASEROW_API = "https://api.baserow.io/api"
API_TOKEN: str = os.getenv('BASEROW_TOKEN')
TABLE_ID: str = os.getenv('BASEROW_TABLE_ID')  # <-- your table

HEADERS = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json",
}

# ---------------------------
# Low-level Baserow helpers
# ---------------------------
def list_rows(table_id: int, select=None, page_size=200):
    """Return all rows (paginated). With user_field_names=true so we can use names."""
    url = f"{BASEROW_API}/database/rows/table/{table_id}/?user_field_names=true&size={page_size}"
    if select:
        url += "&select=" + ",".join(select)
    out = []
    while url:
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        url = data.get("next")
    return out

def batch_create(table_id: int, items: list):
    if not items:
        return
    url = f"{BASEROW_API}/database/rows/table/{table_id}/batch/?user_field_names=true"
    r = requests.post(url, headers=HEADERS, data=json.dumps({"items": items}))
    if not r.ok:
        print("Baserow status:", r.status_code)
        print("Baserow error body:", r.text)  # or r.json()

    r.raise_for_status()

def batch_update(table_id: int, items_with_id: list):
    if not items_with_id:
        return
    url = f"{BASEROW_API}/database/rows/table/{table_id}/batch/?user_field_names=true"
    r = requests.patch(url, headers=HEADERS, data=json.dumps({"items": items_with_id}))
    if not r.ok:
        print("Baserow status:", r.status_code)
        print("Baserow error body:", r.text)  # or r.json()

    r.raise_for_status()

def upsert_by_linkedin_identifier(
    df: pd.DataFrame,
    table_id: int,
    key_field: str = "linkedin_identifier",
    # columns you DO NOT want to overwrite (user-managed)
    protect_fields: set[str] = frozenset({"tags"}),
    batch_size: int = 200,
):
    # Ensure we have a DataFrame and the key column present
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if key_field not in df.columns:
        raise ValueError(f"DataFrame is missing required key column '{key_field}'.")

    # Sanity: drop rows without a key
    df = df[df[key_field].notna()].copy()

    # 1) Build index of existing rows: linkedin_identifier -> row_id
    existing = list_rows(table_id, select=["id", key_field])
    index = {row.get(key_field): row["id"] for row in existing if row.get(key_field) is not None}

    # 2) Convert DF to records (dicts)
    records = df.to_dict(orient="records")

    # 3) Split into updates vs creates
    creates, updates = [], []
    for rec in records:
        key = rec.get(key_field)
        if key in index:
            rid = index[key]
            # Only include fields you want to update (skip protected)
            payload = {k: v for k, v in rec.items() if k not in protect_fields}
            payload["id"] = rid                     # required for PATCH
            updates.append(payload)
        else:
            # For creates, ensure the key is included; omit protected fields to start clean
            payload = {k: v for k, v in rec.items() if k not in protect_fields}
            creates.append(payload)

    # 4) Batch write (chunk to be safe)
    for i in range(0, len(updates), batch_size):
        batch_update(table_id, updates[i:i+batch_size])
    for i in range(0, len(creates), batch_size):
        batch_create(table_id, creates[i:i+batch_size])

df = merge_LI_messages
#df = df.drop(df.columns[0],axis =1)
df.fillna("", inplace=True)  # Replace NaN with empty strings for safer baserow upser
df = df.replace("",None)  # Replace NaN with empty strings for safer baserow upser
df = df.where(pd.notna(df), None)
# df should contain at least the 'linkedin_identifier' column plus whatever you want to write.
upsert_by_linkedin_identifier(df, TABLE_ID)



