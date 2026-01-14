import os
import requests
import pandas as pd
import re
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
from urllib.parse import unquote

env_path = '.env'
load_dotenv(env_path)

API_KEY: str = os.getenv('PHANTOM_API_KEY')

HEADERS = {
    'X-Phantombuster-Key': API_KEY,
    'Content-Type': 'application/json',
    "accept": "application/json"
}

personal_scraping_id: str = os.getenv('SCRAPING_PERSONAL')
company_scraping_id: str = os.getenv('SCRAPING_COMPANY')
combined_list_id: str = os.getenv('COMBINED_LIST_ID')
personal_name: str = os.getenv('PERSON_NAME')
company_name: str = os.getenv('COMPANY_NAME')

def load_phantom_results(phantom_id):
    company_resp = requests.get(
        f'https://api.phantombuster.com/api/v2/agents/fetch-output?id={phantom_id}',
        headers=HEADERS
    )

    launches = company_resp.json()
    json_rep = re.search(r'https://phantombuster\.s3\.amazonaws\.com/.+?/result\.json', launches['output']).group()
    results = pd.DataFrame(requests.get(json_rep).json())
    return results

def get_list_results(list_id):
    url = f"https://api.phantombuster.com/api/v2/org-storage/leads/by-list/{list_id}"
    results = pd.DataFrame(requests.get(url,headers=HEADERS).json())
    return results

def extract_rightmost_part(s):
    try:
        s = s.rstrip('/')
        s = s.split('/')[-1]
    except:
        s
    return s

#Get Phantom Leads results
LI_leads =get_list_results(combined_list_id)

LI_leads["first_engagement"] = LI_leads["editionsHistory"].apply(
        lambda editions: datetime.fromtimestamp(editions[0]['timestamp'] / 1000).date()
        if isinstance(editions, list) and editions else None
    )

LI_leads["last_engagement"] = LI_leads["editionsHistory"].apply(
        lambda editions: datetime.fromtimestamp(editions[-1]['timestamp'] / 1000).date()
        if isinstance(editions, list) and editions else None
    )

LI_leads["companyID"] = LI_leads["linkedinCompanyId"]
LI_leads = LI_leads[["linkedinProfileSlug","linkedinJobTitle","companyName","first_engagement","last_engagement","companyID"]]
LI_leads["linkedinProfileSlug"] = LI_leads["linkedinProfileSlug"].apply(unquote) #Decode accents and emoji's

#Get phantom results from comments
company_comments = load_phantom_results(company_scraping_id)
company_comments["Source"] = company_name

personal_comments = load_phantom_results(personal_scraping_id)
personal_comments["Source"] = personal_name

#Merge both comment and likes
comments_concat = pd.concat([company_comments, personal_comments], ignore_index=True)
comments_concat["linkedin_identifier"] = comments_concat["profileUrl"].apply(extract_rightmost_part)
comments_concat["linkedin_identifier"] = comments_concat["linkedin_identifier"].apply(unquote) #Decode accents and emoji's
comments_concat = pd.merge(left=comments_concat,right=LI_leads,left_on="linkedin_identifier",right_on="linkedinProfileSlug",how="left")

URN_RX = re.compile(r"urn:li:(?:ugcPost|activity):\d+")

def _extract_urn(url: str) -> str | None:
    """Return the LinkedIn URN (activity or ugcPost) from a URL."""
    if not isinstance(url, str):
        return None
    m = URN_RX.search(url)
    return m.group(0) if m else None

def _normalize_list(col: pd.Series) -> pd.Series:
    """Split ' | '-separated strings into list, keep NaNs as empty list."""
    return (
        col.fillna("")
           .astype(str)
           .str.split(r"\s*\|\s*")        # split on pipe with optional spaces
           .apply(lambda lst: [x.strip() for x in lst if x.strip()])
    )


def map_comment_urns_to_texts(comment_urls, comment_texts):
    urls = comment_urls or []
    texts = comment_texts or []

    urns = [_extract_urn(u) for u in urls if _extract_urn(u) is not None]

    # Match only if lengths match
    if len(urns) == len(texts):
        return dict(zip(urns, texts))
    else:
        # Attempt to align best effort: truncate longer list
        min_len = min(len(urns), len(texts))
        return dict(zip(urns[:min_len], texts[:min_len]))

def all_posts_list():
# STEP 1: Normalize all relevant list fields

    comments_concat["postsUrl_list"]     = _normalize_list(comments_concat["postsUrl"])
    comments_concat["commentUrl_list"]   = _normalize_list(comments_concat.get("commentUrl"))
    comments_concat["comments_list"]     = _normalize_list(comments_concat.get("comments"))

    comments_concat["comment count"] = comments_concat["comments_list"].apply(len)
    comments_concat["likes count"] = comments_concat["postsUrl"].str.count(r"\|") + 1

    comments_concat[f'likescount_{company_name}'] = 0
    comments_concat.loc[comments_concat['Source'] == company_name, f'likescount_{company_name}'] = comments_concat['likes count']

    comments_concat[f'likescount_{personal_name}'] = 0
    comments_concat.loc[comments_concat['Source'] == personal_name, f'likescount_{personal_name}'] = comments_concat['likes count']

    comments_concat[f'commentcount_{company_name}'] = 0
    comments_concat.loc[comments_concat['Source'] == company_name, f'commentcount_{company_name}'] = comments_concat['comment count']

    comments_concat[f'commentcount_{personal_name}'] = 0
    comments_concat.loc[comments_concat['Source'] == personal_name, f'commentcount_{personal_name}'] = comments_concat['comment count']

    grouped_engagements = comments_concat.groupby(by=["linkedin_identifier"]).agg({"profileUrl":"first","firstName":"first","lastName":"first","fullName":"first","degree":"first","linkedinJobTitle":"first","companyName":"first","companyID":"first","timestamp": "max","lastCommentedAt":"max","commentUrl_list": lambda x: sum([i for i in x if i], []) ,"comments_list": lambda x: sum([i for i in x if i], []),f"likescount_{company_name}":"sum",f"likescount_{personal_name}":"sum",f"commentcount_{company_name}":"sum",f"commentcount_{personal_name}":"sum"}).reset_index()
    grouped_engagements['timestamp'] = grouped_engagements['timestamp'].str.slice(0,10)
    
    return grouped_engagements

if __name__ == "__main__":

    pb_scraping = all_posts_list()
