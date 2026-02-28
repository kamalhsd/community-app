import streamlit as st
import os
import pandas as pd
from google.cloud import bigquery
import requests
from datetime import datetime

# ==========================================
# 1. BigQuery Authentication & Initialization
# ==========================================

CREDENTIALS_PATH = "google_credentials.json"

# Check local credentials map as per strict coding directive #1
if os.path.exists(CREDENTIALS_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
else:
    st.error(f"⚠️ Credentials file '{CREDENTIALS_PATH}' not found in the root directory. Database queries will fail.")

st.set_page_config(page_title="Forum CRM Hub", layout="wide", page_icon="📡")

# IMPORTANT: Admin should update these variables to their GCP project and dataset.
PROJECT_ID = "my-new-project-480609"
DATASET_ID = "forum_crm"

USERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.users_master"
CATEGORIES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.categories_master"
THREADS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.threads_and_posts"

# ==========================================
# Google Apps Script Proxy (Firewall Bypass)
# ==========================================
# If your Nginx/Cloudflare blocks Python requests, deploy the provided Google_Apps_Script_Proxy.js
# and paste its Web App URL here. Leave empty "" to connect directly to XenForo.
GAS_PROXY_URL = "https://script.google.com/macros/s/AKfycbwIetBB304AzQePfZTcjD5QkJNjWHzlAz0-sWoDVIwIyQuqPfUm5uyuMkBy6_RVzpLu/exec"

def get_bq_client():
    """Initialize the BigQuery client. Will use the os.environ credentials automatically."""
    try:
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            return bigquery.Client()
        return None
    except Exception as e:
        st.error(f"Failed to initialize BigQuery Client: {e}")
        return None

bq_client = get_bq_client()

# ==========================================
# 2. State Management (Zone 2 to Zone 3 Link)
# ==========================================

# Using st.session_state to store the active thread so UI is retained upon reruns.
if 'active_thread_id' not in st.session_state:
    st.session_state.active_thread_id = None
if 'active_site_id' not in st.session_state:
    st.session_state.active_site_id = None
if 'dashboard_data' not in st.session_state:
    st.session_state.dashboard_data = None
if 'dashboard_posts' not in st.session_state:
    st.session_state.dashboard_posts = None


# ==========================================
# 3. Modular Functions
# ==========================================

def fetch_metadata():
    """Fetch initial lists (sites, categories, users) to populate the sidebar filters."""
    if not bq_client: return [], pd.DataFrame(), pd.DataFrame()
    try:
        sites_query = f"SELECT DISTINCT site_id FROM `{USERS_TABLE}` WHERE site_id IS NOT NULL"
        sites = bq_client.query(sites_query).to_dataframe()['site_id'].tolist()
        
        cats_query = f"SELECT site_id, category_name, node_id FROM `{CATEGORIES_TABLE}`"
        cats_df = bq_client.query(cats_query).to_dataframe()
        
        users_query = f"SELECT site_id, username, api_user_id FROM `{USERS_TABLE}`"
        users_df = bq_client.query(users_query).to_dataframe()
        
        return sites, cats_df, users_df
    except Exception as e:
        st.error(f"Error fetching metadata from BigQuery: {e}")
        return [], pd.DataFrame(), pd.DataFrame()


def fetch_filtered_threads(site_id, ans_min, ans_max, selected_user, keyword, selected_categories=None):
    """
    BigQuery SELECT query to feed Zone 2 dataframe based on Zone 1 filters.
    Includes explicit schema parsing from threads_and_posts.
    """
    if not bq_client: return pd.DataFrame()
    
    try:
        # Base query to aggregate thread metrics
        where_clauses = ["site_id = @site_id"]
        
        if keyword:
            where_clauses.append("(LOWER(content) LIKE LOWER(@keyword) OR LOWER(target_link) LIKE LOWER(@keyword))")
            
        if selected_categories and len(selected_categories) > 0:
            where_clauses.append("category_name IN UNNEST(@selected_categories)")
            
        where_str = " AND ".join(where_clauses)

        query = f"""
            WITH thread_stats AS (
                SELECT 
                    thread_id,
                    MAX(CASE WHEN post_id = 0 THEN content ELSE NULL END) AS thread_title,
                    MAX(category_name) AS category,
                    COUNT(CASE WHEN post_type = 'Answer' THEN 1 END) AS total_answers,
                    MAX(timestamp) AS last_active_date,
                    LOGICAL_OR(username = @selected_user) AS user_participated
                FROM `{THREADS_TABLE}`
                WHERE {where_str}
                GROUP BY thread_id
            )
            SELECT 
                thread_id, 
                thread_title, 
                IFNULL(category, 'Not Mapped') AS category, 
                total_answers, 
                last_active_date
            FROM thread_stats
            WHERE total_answers BETWEEN @ans_min AND @ans_max
        """
        
        # User filter applied at the aggregated CTE level if selected
        if selected_user and selected_user != "All":
            query += " AND user_participated = TRUE"
            
        query += " ORDER BY last_active_date DESC"

        query_params = [
            bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
            bigquery.ScalarQueryParameter("selected_user", "STRING", selected_user if selected_user != "All" else ""),
            bigquery.ScalarQueryParameter("keyword", "STRING", f"%{keyword}%" if keyword else ""),
            bigquery.ScalarQueryParameter("ans_min", "INT64", ans_min),
            bigquery.ScalarQueryParameter("ans_max", "INT64", ans_max)
        ]
        
        if selected_categories and len(selected_categories) > 0:
            query_params.append(bigquery.ArrayQueryParameter("selected_categories", "STRING", selected_categories))

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        
        return bq_client.query(query, job_config=job_config).to_dataframe()
        
    except Exception as e:
        st.error(f"Error executing fetch_filtered_threads: {e}")
        return pd.DataFrame()


def fetch_bulk_thread_history(thread_ids, site_id):
    """BigQuery SELECT query to pull chronological thread history for ALL threads loaded into Zone 2."""
    if not bq_client or not thread_ids: return pd.DataFrame()
    try:
        query = f"""
            SELECT site_id, thread_id, post_id, username, content, post_type, timestamp, target_link, question_url, answer_url
            FROM `{THREADS_TABLE}`
            WHERE site_id = @site_id AND thread_id IN UNNEST(@thread_ids)
            ORDER BY timestamp ASC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ArrayQueryParameter("thread_ids", "INT64", thread_ids)
            ]
        )
        return bq_client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Error executing fetch_bulk_thread_history: {e}")
        return pd.DataFrame()

def fetch_user_stats(site_id):
    """Aggregate threads and answers per user for a specific site."""
    if not bq_client: return pd.DataFrame()
    query = f"""
        SELECT 
            username as Username,
            COUNTIF(post_type = 'Question') AS Threads_Created,
            COUNTIF(post_type = 'Answer') AS Answers_Posted
        FROM `{THREADS_TABLE}`
        WHERE site_id = @site_id
        GROUP BY username
        ORDER BY Answers_Posted DESC, Threads_Created DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("site_id", "STRING", site_id)
        ]
    )
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching user stats: {e}")
        return pd.DataFrame()

def fetch_daily_questions(site_id, target_date):
    """Fetch all Question threads published on a specific date."""
    if not bq_client: return pd.DataFrame()
    query = f"""
        SELECT 
            FORMAT_DATETIME('%Y-%m-%d', timestamp) AS Date,
            username AS User,
            IFNULL(category_name, 'Not Mapped') AS Forum_Category,
            content AS Question,
            question_url AS Question_URL
        FROM `{THREADS_TABLE}`
        WHERE site_id = @site_id 
          AND DATE(timestamp) = @target_date 
          AND post_type = 'Question'
        ORDER BY timestamp DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching daily questions: {e}")
        return pd.DataFrame()

def fetch_daily_answers(site_id, target_date):
    """Fetch all Answer replies published on a specific date."""
    if not bq_client: return pd.DataFrame()
    query = f"""
        SELECT 
            FORMAT_DATETIME('%Y-%m-%d', timestamp) AS Date,
            username AS Username,
            content AS Answer,
            question_url AS Question_URL,
            answer_url AS Answer_Links
        FROM `{THREADS_TABLE}`
        WHERE site_id = @site_id 
          AND DATE(timestamp) = @target_date 
          AND post_type = 'Answer'
        ORDER BY timestamp DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date)
        ]
    )
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching daily answers: {e}")
        return pd.DataFrame()

def generate_ai_reply(provider, api_key_or_url, model, context_df, username):
    """
    Takes the chronological thread_history_df and formats it into a robust prompt 
    for Gemini, Groq, or Ollama to generate a contextual response.
    """
    if context_df.empty:
        return "Not enough context to generate a reply."
        
    # Build conversation context string
    context_str = "Thread History:\n\n"
    for _, row in context_df.iterrows():
        context_str += f"[{row['timestamp']}] {row['username']} ({row['post_type']}):\n{row['content']}\n\n"
        
    system_prompt = f"""You are a helpful, knowledgeable participant on a specialized internet forum.
Your username is {username}. 
Review the thread history provided, and write a natural, conversational reply that directly addresses the ongoing discussion.
Do not wrap your response in quotes. Do not include signature blocks. Just write the message body."""

    try:
        if provider == "Gemini":
            if not api_key_or_url: return "Error: Gemini API Key required."
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key_or_url}"
            payload = {
                "contents": [{"parts": [{"text": f"{system_prompt}\n\n{context_str}"}]}],
                "generationConfig": {
                    "temperature": 0.7
                }
            }
            res = requests.post(url, json=payload, timeout=30)
            if res.status_code == 200:
                data = res.json()
                return data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', 'Error: No text returned.')
            else:
                return f"Gemini API Error: {res.text}"
                
        elif provider == "Groq":
            if not api_key_or_url: return "Error: Groq API Key required."
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key_or_url}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context_str}
                ],
                "temperature": 0.7
            }
            res = requests.post(url, headers=headers, json=payload, timeout=30)
            if res.status_code == 200:
                data = res.json()
                return data.get('choices', [{}])[0].get('message', {}).get('content', 'Error: No text returned.')
            else:
                return f"Groq API Error: {res.text}"
                
        elif provider == "Ollama":
            if not api_key_or_url: return "Error: Ollama Base URL required (e.g., http://localhost:11434)."
            url = f"{api_key_or_url.rstrip('/')}/api/chat"
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context_str}
                ],
                "stream": False
            }
            res = requests.post(url, json=payload, timeout=60)
            if res.status_code == 200:
                data = res.json()
                return data.get('message', {}).get('content', 'Error: No text returned.')
            else:
                return f"Ollama API Error: {res.text}"
                
    except Exception as e:
        return f"Request Error: {str(e)}"

# ==========================================
# XenForo Sites Configuration
# ==========================================
WEBSITES = {
    "techzeel": { "url": "https://community.techzeel.net", "key": "N1WiyPo_LLSBxwH5DjIFIG_VtLXFJ6WR" },
    "triphippies": { "url": "https://community.triphippies.com", "key": "usw7qzpu-DJSLNJRjZwtFX_UJCfptbbN" },
    "healthgroovy": { "url": "https://community.healthgroovy.com", "key": "sjdhr-_b8KcC74-rARoJ9Vf-uCO8F5Yz" },
    "learningtoday": { "url": "https://community.learningtoday.net", "key": "FSyv1CkfBqDpGd-mkI8wgCIVSYuzsubF" },
    "allinsider": { "url": "https://forum.allinsider.net", "key": "ZJoQ2YJ2HeFD4Nn2tvqJ8lLgndbHkmps" },
    "radarro": { "url": "https://community.radarro.com", "key": "xAAsWa0-Y-BEoNJASELYHYFypKtxKC9m" },
    "getassist": { "url": "https://forum.getassist.net", "key": "JfGAv9RuSVUpO7_LehiAVGRgdg19YqB1" },
    "yourhomify": { "url": "https://community.yourhomify.com", "key": "e9QvVWyGa37-0GGusiiTiuvYH3onHrW-" },
    "accountingbyte": { "url": "https://forum.accountingbyte.com", "key": "oOrx1YU-mEtvw_GEKMBdJowERzaL938H" },
    "sportsbyte": { "url": "https://community.sportsbyte.net", "key": "8OtIhtd-R1BPHi13jyXT2WsHcsayoXGZ" }
}

def post_to_xenforo(site_id, thread_id, api_user_id, message):
    """The REST API call using requests to XenForo."""
    site_config = WEBSITES.get(site_id.lower())
    if not site_config:
        st.error(f"Unknown site ID: {site_id}")
        return None
        
    api_key = site_config["key"]
    site_url = f"{site_config['url']}/api/posts"
    
    headers = {
        "XF-Api-Key": api_key,
        "XF-Api-User": str(api_user_id),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    payload = {
        "thread_id": int(thread_id),
        "message": message
    }
    
    # -----------------------------------
    # Route via Google Apps Script Proxy
    # -----------------------------------
    if GAS_PROXY_URL:
        proxy_payload = {
            "target_url": site_url,
            "headers": headers,
            "payload": payload
        }
        try:
            response = requests.post(GAS_PROXY_URL, json=proxy_payload, timeout=15)
            if response.status_code == 200:
                res_json = response.json()
                if "error" in res_json:
                    st.error(f"GAS Proxy Error: {res_json['error']}")
                    return None
                    
                import json
                raw_xf_data = res_json.get("data", "{}")
                
                try:
                    xf_res = json.loads(raw_xf_data)
                    if res_json.get("status") == 200:
                        post_data = xf_res.get('post', {})
                        if 'post_id' in post_data:
                            return int(post_data['post_id']), post_data.get('view_url')
                        else:
                            st.warning(f"Reply created, but couldn't parse ID. Raw: {xf_res}")
                            return None, None
                    else:
                        st.error(f"XenForo API Error via Proxy (Code {res_json.get('status')}): {xf_res}")
                        return None, None
                except json.JSONDecodeError:
                    # XenForo returned HTML instead of JSON (usually a 404/403 block)
                    st.error(f"XenForo Proxy Error (HTML Returned, Code {res_json.get('status')}): {raw_xf_data[:500]}...")
                    return None, None
            else:
                st.error(f"GAS Proxy Request Failed: {response.text}")
                return None
        except Exception as e:
            st.error(f"GAS Proxy Network Error: {e}")
            return None

    # -----------------------------------
    # Direct Route
    # -----------------------------------
    try:
        response = requests.post(site_url, headers=headers, data=payload, timeout=10)
        
        if response.status_code == 200:
            post_data = response.json().get('post', {})
            return int(post_data.get('post_id')), post_data.get('view_url')
        else:
            st.error(f"XenForo API Error [{response.status_code}]: {response.text}")
            return None, None
    except requests.exceptions.ConnectionError:
        try:
            fallback_url = site_url.replace("https://", "http://")
            response = requests.post(fallback_url, headers=headers, data=payload, timeout=10)
            if response.status_code == 200:
                post_data = response.json().get('post', {})
                return int(post_data.get('post_id')), post_data.get('view_url')
            else:
                st.error(f"XenForo API Error (HTTP Fallback) [{response.status_code}]: {response.text}")
                return None, None
        except Exception as fallback_e:
            st.error(f"XenForo Network Error (HTTPS/HTTP Failed): {fallback_e}")
            return None, None
    except Exception as e:
        st.error(f"XenForo Network/Request Error: {e}")
        return None, None

def log_to_bigquery(site_id, thread_id, post_id, username, content, target_link, answer_url=None):
    """BigQuery INSERT statement logging the successful API post."""
    if not bq_client: return False
    try:
        # We assume the parent thread already has a category name logged, so we just pass NULL here for Answers
        # since the UI groups by thread_id and takes MAX(category_name).
        query = f"""
            INSERT INTO `{THREADS_TABLE}` 
            (site_id, thread_id, post_id, username, content, post_type, timestamp, target_link, category_name, question_url, answer_url)
            VALUES (@site_id, @thread_id, @post_id, @username, @content, 'Answer', CURRENT_DATETIME(), @target_link, NULL, NULL, @answer_url)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter("thread_id", "INT64", thread_id),
                bigquery.ScalarQueryParameter("post_id", "INT64", post_id),
                bigquery.ScalarQueryParameter("username", "STRING", username),
                bigquery.ScalarQueryParameter("content", "STRING", content),
                bigquery.ScalarQueryParameter("target_link", "STRING", target_link if target_link else None),
                bigquery.ScalarQueryParameter("answer_url", "STRING", answer_url if answer_url else None)
            ]
        )
        # Block until the query completes
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"BigQuery Insert Error: {e}")
        return False

def create_xenforo_thread(site_id, node_id, api_user_id, title, message):
    """The REST API call to XenForo to create a new thread."""
    site_config = WEBSITES.get(site_id.lower())
    if not site_config:
        st.error(f"Unknown site ID: {site_id}")
        return None
        
    api_key = site_config["key"]
    site_url = f"{site_config['url']}/api/threads"
        
    headers = {
        "XF-Api-Key": api_key,
        "XF-Api-User": str(api_user_id),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    payload = {
        "node_id": int(node_id),
        "title": title,
        "message": message,
        "discussion_type": "discussion"
    }
    
    # -----------------------------------
    # Route via Google Apps Script Proxy
    # -----------------------------------
    if GAS_PROXY_URL:
        proxy_payload = {
            "target_url": site_url,
            "headers": headers,
            "payload": payload
        }
        try:
            response = requests.post(GAS_PROXY_URL, json=proxy_payload, timeout=15)
            if response.status_code == 200:
                res_json = response.json()
                if "error" in res_json:
                    st.error(f"GAS Proxy Error: {res_json['error']}")
                    return None
                    
                import json
                raw_xf_data = res_json.get("data", "{}")
                
                try:
                    xf_res = json.loads(raw_xf_data)
                    if res_json.get("status") == 200:
                        thread_data = xf_res.get('thread', {})
                        if 'thread_id' in thread_data:
                            return int(thread_data['thread_id']), thread_data.get('view_url')
                        else:
                            st.warning(f"Thread created, but couldn't parse ID. Raw: {xf_res}")
                            return None, None
                    else:
                        st.error(f"XenForo API Error via Proxy (Code {res_json.get('status')}): {xf_res}")
                        return None, None
                except json.JSONDecodeError:
                    # XenForo returned HTML instead of JSON (usually a 404/403 block)
                    st.error(f"XenForo Proxy Error (HTML Returned, Code {res_json.get('status')}): {raw_xf_data[:500]}...")
                    return None, None
            else:
                st.error(f"GAS Proxy Request Failed: {response.text}")
                return None
        except Exception as e:
            st.error(f"GAS Proxy Network Error: {e}")
            return None

    # -----------------------------------
    # Direct Route
    # -----------------------------------
    try:
        response = requests.post(site_url, headers=headers, data=payload, timeout=10)
        
        if response.status_code == 200:
            thread_data = response.json().get('thread', {})
            return int(thread_data.get('thread_id')), thread_data.get('view_url')
        else:
            st.error(f"XenForo API Error [{response.status_code}]: {response.text}")
            return None, None
    except requests.exceptions.ConnectionError:
        try:
            fallback_url = site_url.replace("https://", "http://")
            response = requests.post(fallback_url, headers=headers, data=payload, timeout=10)
            if response.status_code == 200:
                thread_data = response.json().get('thread', {})
                return int(thread_data.get('thread_id')), thread_data.get('view_url')
            else:
                st.error(f"XenForo API Error (HTTP Fallback) [{response.status_code}]: {response.text}")
                return None, None
        except Exception as fallback_e:
            st.error(f"XenForo Network Error (HTTPS/HTTP Failed): {fallback_e}")
            return None, None
    except Exception as e:
        st.error(f"XenForo Network/Request Error: {e}")
        return None, None

def log_new_thread_to_bigquery(site_id, thread_id, username, title, category_name, target_link, question_url):
    """BigQuery INSERT statement logging a NEW thread directly natively."""
    if not bq_client: return False
    try:
        query = f"""
            INSERT INTO `{THREADS_TABLE}` 
            (site_id, thread_id, post_id, username, content, post_type, timestamp, target_link, category_name, question_url, answer_url)
            VALUES (@site_id, @thread_id, 0, @username, @content, 'Question', CURRENT_DATETIME(), @target_link, @category_name, @question_url, NULL)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter("thread_id", "INT64", thread_id),
                bigquery.ScalarQueryParameter("username", "STRING", username),
                bigquery.ScalarQueryParameter("content", "STRING", title),
                bigquery.ScalarQueryParameter("target_link", "STRING", target_link if target_link else None),
                bigquery.ScalarQueryParameter("category_name", "STRING", category_name),
                bigquery.ScalarQueryParameter("question_url", "STRING", question_url if question_url else None)
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"BigQuery Insert Error: {e}")
        return False

def delete_post_from_bigquery(site_id, thread_id, post_id):
    """Deletes an Answer post from BigQuery (useful for scrubbing accidental duplicates)."""
    if not bq_client: return False
    try:
        query = f"""
            DELETE FROM `{THREADS_TABLE}` 
            WHERE site_id = @site_id 
              AND thread_id = @thread_id 
              AND post_id = @post_id 
              AND post_type = 'Answer'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter("thread_id", "INT64", thread_id),
                bigquery.ScalarQueryParameter("post_id", "INT64", post_id)
            ]
        )
        bq_client.query(query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"BigQuery Delete Error: {e}")
        return False


# ==========================================
# UI LAYOUT & ZONES
# ==========================================

st.title("📡 Forum CRM Hub")

# Fetch overall available metadata
sites, cats_df, users_df = fetch_metadata()


# ------------------------------------------
# ZONE 1: Left Sidebar (The Filter Funnel)
# ------------------------------------------
with st.sidebar:
    st.header("The Filter Funnel")
    
    selected_site = st.selectbox("Forum Selector", options=sites) if sites else None
    
    ans_min, ans_max = st.slider("Answer Count Filter", min_value=0, max_value=50, value=(0, 50))
    
    # Render categories based on site dynamically
    available_cats = cats_df[cats_df['site_id'] == selected_site]['category_name'].tolist() if not cats_df.empty and selected_site else []
    selected_categories = st.multiselect("Category Filter", options=available_cats)
    
    # Render users based on site dynamically
    available_users = ["All"] + users_df[users_df['site_id'] == selected_site]['username'].tolist() if not users_df.empty and selected_site else ["All"]
    selected_user_filter = st.selectbox("User History Search", options=available_users)
    
    keyword_filter = st.text_input("Keyword/Link Tracker")
    
    st.markdown("---")
    fetch_data_btn = st.button("🔄 Import Data from BigQuery", type="primary", use_container_width=True)

# State Management Check: Reset target thread if forum switches
if selected_site != st.session_state.active_site_id:
    st.session_state.active_site_id = selected_site
    st.session_state.active_thread_id = None
    st.session_state.dashboard_data = None
    st.session_state.dashboard_posts = None


# ------------------------------------------
# ZONE 2: Top Main Screen (Live Dashboard)
# ------------------------------------------
tab_main, tab_import, tab_new_thread, tab_drip_scheduler, tab_user_stats, tab_daily_reports = st.tabs(["Live Dashboard", "Bulk Data Import", "Create New Thread", "Bulk Drip Scheduler", "User Stats", "Daily Reports"])

with tab_import:
    st.header("📁 Bulk Data Import")
    destination_table = st.selectbox(
        "Select Destination Table",
        options=["users_master", "categories_master", "threads_and_posts"]
    )
    
    # Generate and provide Sample CSV template
    expected_schema = {
        "users_master": ["site_id", "username", "api_user_id"],
        "categories_master": ["site_id", "category_name", "node_id"],
        "threads_and_posts": ["site_id", "thread_id", "post_id", "username", "content", "post_type", "timestamp", "target_link", "category_name", "question_url", "answer_url"]
    }
    
    st.info(f"**Expected Columns:** `{', '.join(expected_schema[destination_table])}`")
    
    sample_df = pd.DataFrame(columns=expected_schema[destination_table])
    sample_csv = sample_df.to_csv(index=False).encode('utf-8')
    
    st.download_button(
        label="📥 Download Sample CSV Template",
        data=sample_csv,
        file_name=f"{destination_table}_template.csv",
        mime="text/csv",
    )
    
    st.markdown("---")
    uploaded_file = st.file_uploader("Upload Populated CSV File", type=["csv"])
    
    if uploaded_file is not None:
        try:
            try:
                df_import = pd.read_csv(uploaded_file)
            except UnicodeDecodeError:
                uploaded_file.seek(0)
                df_import = pd.read_csv(uploaded_file, encoding='latin-1')
                
            st.write("Data Preview (First 5 Rows):")
            st.dataframe(df_import.head())
            
            if st.button("Push to BigQuery", type="primary"):
                if not bq_client:
                    st.error("BigQuery client not initialized.")
                else:
                    with st.spinner("Pushing data to BigQuery..."):
                        table_id = f"{PROJECT_ID}.{DATASET_ID}.{destination_table}"
                        job_config = bigquery.LoadJobConfig(
                            write_disposition="WRITE_APPEND",
                        )
                        
                        try:
                            # 1. Cast Timestamp natively for BigQuery compatibility before pushing
                            if "timestamp" in df_import.columns:
                                df_import["timestamp"] = pd.to_datetime(df_import["timestamp"], errors="coerce", dayfirst=True)
                                
                            job = bq_client.load_table_from_dataframe(
                                df_import, table_id, job_config=job_config
                            )
                            job.result()  # Wait for the job to complete.
                            
                            st.success(f"✅ Successfully appended {len(df_import)} rows to {destination_table}.")
                        except Exception as e:
                            st.error(f"Failed to push to BigQuery: {e}")
                            
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")

with tab_main:
    st.subheader("📊 Live Dashboard")
    
    if selected_site:
        if fetch_data_btn:
            with st.spinner("Importing data from BigQuery..."):
                df_temp = fetch_filtered_threads(
                    site_id=selected_site,
                    ans_min=ans_min,
                    ans_max=ans_max,
                    selected_user=selected_user_filter,
                    keyword=keyword_filter,
                    selected_categories=selected_categories
                )
                
                if not df_temp.empty:
                    thread_ids = df_temp['thread_id'].tolist()
                    st.session_state.dashboard_posts = fetch_bulk_thread_history(thread_ids, selected_site)
                else:
                    st.session_state.dashboard_posts = pd.DataFrame()
                    
                st.session_state.dashboard_data = df_temp
                st.session_state.active_thread_id = None
        
        df_threads = st.session_state.dashboard_data
        
        if df_threads is not None and not df_threads.empty:
            # Utilize new Streamlit dataframe row selection (Supported >= 1.35)
            # Clicking a row writes it into session state variables natively, triggering rerun.
            event = st.dataframe(
                df_threads, 
                use_container_width=True, 
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun"
            )
            
            # Capture selection into session state
            selected_rows = event.selection.rows
            if selected_rows:
                st.session_state.active_thread_id = int(df_threads.iloc[selected_rows[0]]['thread_id'])
            else:
                # If standard selection un-clicks, clear state
                st.session_state.active_thread_id = None
                
        elif df_threads is not None and df_threads.empty:
            st.info("No threads found matching the current filters.")
        else:
            st.info("👈 Click **Import Data from BigQuery** in the sidebar to load threads.")
    else:
        st.info("👈 Please select a forum site to display threads.")


    # ------------------------------------------
    # ZONE 3: Bottom Main Screen (Action Hub)
    # ------------------------------------------
    if st.session_state.active_thread_id:
        st.markdown("---")
        st.header(f"✍️ Action & Publishing Hub (Thread: {st.session_state.active_thread_id})")
        
        # Pull from local session cache instead of querying BigQuery live
        all_posts_df = st.session_state.dashboard_posts
        
        if all_posts_df is not None and not all_posts_df.empty:
            thread_history_df = all_posts_df[all_posts_df['thread_id'] == st.session_state.active_thread_id]
        else:
            thread_history_df = pd.DataFrame()
        
        # Collision detection cache
        participating_users = set()
    
        if not thread_history_df.empty:
            st.subheader("Chronological History Feed")
            
            # Draw Loop natively using st.chat_message
            for _, row in thread_history_df.iterrows():
                is_question = (row['post_type'] == 'Question')
                message_type = "user" if is_question else "assistant"
                
                with st.chat_message(name=message_type):
                    st.markdown(f"**{row['username']}** | *{row['timestamp']}*")
                    st.write(row['content'])
                    
                    # Render metadata conditionally
                    if pd.notnull(row.get('target_link')) and row['target_link'] != "":
                        st.caption(f"🔗 Target Link: {row['target_link']}")
                    
                    if is_question and pd.notnull(row.get('question_url')) and row['question_url'] != "":
                        st.caption(f"🌐 Thread URL: {row['question_url']}")
                    elif not is_question and pd.notnull(row.get('answer_url')) and row['answer_url'] != "":
                        st.caption(f"🌐 Post URL: {row['answer_url']}")
                        
                    # Delete Button for Duplicate Scrubbing
                    if not is_question:
                        if st.button(f"🗑️ Delete from BigQuery", key=f"del_{row['site_id']}_{row['thread_id']}_{row['post_id']}_{_}"):
                            with st.spinner("Deleting record from database..."):
                                success = delete_post_from_bigquery(row['site_id'], row['thread_id'], row['post_id'])
                                if success:
                                    st.success("Deleted from BigQuery!")
                                    st.rerun()
                        
                # Record usernames for collision check
                participating_users.add(row['username'])
                
        # Publishing Controls
        st.subheader("Draft Reply")
        
        site_users = users_df[users_df['site_id'] == selected_site] if not users_df.empty else pd.DataFrame()
        reply_username = st.selectbox(
            "Select User to Reply As", 
            options=site_users['username'].tolist() if not site_users.empty else []
        )
        
        # Collision Detection Warning
        if reply_username in participating_users:
            st.warning("⚠️ This user has already posted in this thread.")
            
        with st.expander("🤖 AI Auto-Draft Settings", expanded=False):
            ai_provider = st.selectbox("AI Provider", ["Gemini", "Groq", "Ollama"])
            
            if ai_provider == "Ollama":
                ai_key_or_url = st.text_input("Ollama Base URL", value="http://localhost:11434")
                ai_model = st.text_input("Model Name", value="llama3.1")
            else:
                ai_key_or_url = st.text_input("API Key", type="password")
                ai_model = st.text_input("Model Name", value="gemini-2.5-flash" if ai_provider == "Gemini" else "llama3-70b-8192")

        # Session State for AI Draft Injection
        if 'draft_reply' not in st.session_state:
            st.session_state.draft_reply = ""

        reply_content = st.text_area("Message Body", value=st.session_state.draft_reply)
        target_link = st.text_input("Target SEO Link (Optional)")
        
        # Phase 2 Action button layout
        col_submit, col_ai = st.columns(2)
        
        with col_submit:
            if st.button("Publish Reply Live", type="primary"):
                if not reply_content.strip():
                    st.error("Please enter a message body before publishing.")
                elif not reply_username:
                    st.error("Please select a user to reply as.")
                else:
                    user_row = site_users[site_users['username'] == reply_username].iloc[0]
                    api_user_id = user_row['api_user_id']
                    
                    with st.spinner("Publishing via XenForo API & Logging to BigQuery..."):
                        # Execute API
                        new_post_id, api_answer_url = post_to_xenforo(selected_site, st.session_state.active_thread_id, api_user_id, reply_content)
                        
                        if new_post_id:
                            # Find parent question URL to construct canonical XenForo answer URL
                            final_answer_url = api_answer_url
                            if not thread_history_df.empty:
                                q_rows = thread_history_df[thread_history_df['post_type'] == 'Question']
                                if not q_rows.empty and pd.notnull(q_rows.iloc[0].get('question_url')):
                                    q_url = str(q_rows.iloc[0]['question_url']).strip()
                                    if q_url:
                                        # Assemble https://domain.com/threads/title.id/#post-id
                                        final_answer_url = f"{q_url.rstrip('/')}/#post-{new_post_id}"

                            # Log to BQ
                            bq_logged = log_to_bigquery(
                                selected_site, 
                                st.session_state.active_thread_id, 
                                new_post_id, 
                                reply_username, 
                                reply_content, 
                                target_link,
                                final_answer_url
                            )
                            if bq_logged:
                                st.success("Successfully published to XenForo and logged state into BigQuery!")
                                st.rerun() # Refresh Feed and Zone 2 count
        
        with col_ai:
            if st.button("Auto-Draft with AI", type="secondary"):
                if not reply_username:
                    st.error("Please select a user to reply as first.")
                else:
                    with st.spinner(f"Drafting contextual reply using {ai_provider}..."):
                        generated_text = generate_ai_reply(
                            ai_provider, 
                            ai_key_or_url, 
                            ai_model, 
                            thread_history_df, 
                            reply_username
                        )
                        # Inject into Streamlit session state and force reload
                        st.session_state.draft_reply = generated_text
                        st.rerun()

with tab_new_thread:
    st.header("📝 Create New Thread")
    st.write("Publish brand new questions directly to any forum category.")
    
    if not sites:
        st.warning("No sites available in metadata.")
    else:
        new_site = st.selectbox("Select Forum Site", options=sites, key="new_thread_site")
        
        # Get users for this site
        site_users_new = users_df[users_df['site_id'] == new_site] if not users_df.empty else pd.DataFrame()
        new_username = st.selectbox(
            "Select User to Post As", 
            options=site_users_new['username'].tolist() if not site_users_new.empty else [],
            key="new_thread_user"
        )
        
        # Get categories for this site
        site_cats_new = cats_df[cats_df['site_id'] == new_site] if not cats_df.empty else pd.DataFrame()
        new_category = st.selectbox(
            "Select Category",
            options=site_cats_new['category_name'].tolist() if not site_cats_new.empty else [],
            key="new_thread_cat"
        )
        
        new_title = st.text_input("Thread Title", key="new_thread_title")
        new_content = st.text_area("Message Body", key="new_thread_content")
        new_target_link = st.text_input("Target SEO Link (Optional)", key="new_thread_target_link")
        new_question_url = st.text_input("Expected Thread URL (Optional)", key="new_thread_question_url", help="If you know what the URL will be, log it here.")
        
        if st.button("Publish New Thread", type="primary"):
            if not new_title.strip() or not new_content.strip():
                st.error("Title and Message Body are required.")
            elif not new_username or not new_category:
                st.error("User and Category must be selected.")
            else:
                user_row = site_users_new[site_users_new['username'] == new_username].iloc[0]
                api_user_id = user_row['api_user_id']
                
                cat_row = site_cats_new[site_cats_new['category_name'] == new_category].iloc[0]
                node_id = cat_row['node_id']
                
                with st.spinner(f"Publishing New Thread to {new_site}..."):
                    new_thread_id, auto_question_url = create_xenforo_thread(new_site, node_id, api_user_id, new_title, new_content)
                    
                    if new_thread_id:
                        # Prioritize user-provided question_url if typed, otherwise fall back to auto-extracted URL
                        final_question_url = new_question_url if new_question_url.strip() != "" else auto_question_url
                        
                        bq_logged = log_new_thread_to_bigquery(
                            new_site, 
                            new_thread_id, 
                            new_username, 
                            new_title, 
                            new_category, 
                            new_target_link, 
                            final_question_url
                        )
                        if bq_logged:
                            st.success(f"Successfully created thread #{new_thread_id} and logged directly to Live Dashboard!")
                            # Reset fields without blowing away entire session
                            st.rerun()

with tab_drip_scheduler:
    st.header("⏳ Bulk Drip Scheduler")
    st.write("Schedule bulk questions or answers to be dripped over time via Google Cloud Functions.")
    
    # 1. Data Entry
    schema = {
        "site_id": [""],
        "thread_id": [0],
        "username": [""],
        "content": [""],
        "post_type": ["Answer"],
        "target_link": [""]
    }
    df_in = pd.DataFrame(schema)
    
    edited_df = st.data_editor(df_in, num_rows="dynamic", use_container_width=True)
    
    st.markdown("---")
    # 2. Scheduling Controls
    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("Start Date")
    with col2:
        start_time = st.time_input("Start Time")
    with col3:
        interval_str = st.selectbox("Interval Between Posts", ["15 Minutes", "30 Minutes", "1 Hour", "2 Hours"])
        
    # 3. Execution Logic
    if st.button("Schedule Bulk Campaign", type="primary", use_container_width=True):
        import datetime
        
        interval_map = {
            "15 Minutes": datetime.timedelta(minutes=15),
            "30 Minutes": datetime.timedelta(minutes=30),
            "1 Hour": datetime.timedelta(hours=1),
            "2 Hours": datetime.timedelta(hours=2)
        }
        interval = interval_map[interval_str]
        
        base_time = datetime.datetime.combine(start_date, start_time)
        
        # Filter out empty rows
        valid_df = edited_df[edited_df["content"].str.strip() != ""].copy()
        
        if valid_df.empty:
            st.error("Please enter at least one valid post.")
        elif not bq_client:
            st.error("BigQuery client not initialized.")
        else:
            with st.spinner("Calculating schedule and pushing to Queue..."):
                try:
                    publish_times = []
                    for idx in range(len(valid_df)):
                        pub_time = base_time + (interval * idx)
                        publish_times.append(pub_time)
                        
                    valid_df['publish_time'] = publish_times
                    valid_df['post_id'] = 0
                    
                    # Ensure correct column order and type conversion for BQ
                    final_df = valid_df[['site_id', 'thread_id', 'post_id', 'username', 'content', 'post_type', 'target_link', 'publish_time']]
                    final_df['publish_time'] = pd.to_datetime(final_df['publish_time'])
                    final_df['thread_id'] = pd.to_numeric(final_df['thread_id'], errors='coerce').fillna(0).astype('Int64')
                    final_df['post_id'] = final_df['post_id'].astype('Int64')
                    
                    # Push to BQ
                    table_id = f"{bq_client.project}.forum_crm.scheduled_queue"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    
                    job = bq_client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
                    job.result() # Wait for job to finish
                    
                    st.success(f"Successfully scheduled {len(final_df)} posts starting at {base_time.strftime('%Y-%m-%d %H:%M:%S')}!")
                except Exception as e:
                    st.error(f"Error scheduling campaign: {e}")

with tab_user_stats:
    st.header("👥 User Activity Stats")
    st.write("View the total number of Threads and Answers authored by each user.")
    
    if not sites:
        st.warning("No sites available in metadata.")
    else:
        stats_site = st.selectbox("Select Forum Site", options=sites, key="stats_site_selector")
        
        if st.button("Fetch User Stats", type="primary"):
            with st.spinner(f"Aggregating activity for {stats_site}..."):
                stats_df = fetch_user_stats(stats_site)
                
                if not stats_df.empty:
                    # Provide a metric summary above the table
                    total_threads = int(stats_df['Threads_Created'].sum())
                    total_answers = int(stats_df['Answers_Posted'].sum())
                    
                    col1, col2 = st.columns(2)
                    col1.metric("Total Threads Authored", total_threads)
                    col2.metric("Total Answers Posted", total_answers)
                    
                    st.dataframe(stats_df, use_container_width=True, hide_index=True)
                else:
                    st.info(f"No activity found for {stats_site}.")

with tab_daily_reports:
    st.header("📅 Daily Operations Report")
    st.write("Fetch all questions and answers published on a specific date.")
    
    if not sites:
        st.warning("No sites available in metadata.")
    else:
        report_site = st.selectbox("Select Forum Site", options=sites, key="report_site_selector")
        report_date = st.date_input("Select Report Date")
        
        if st.button("Generate Reports", type="primary"):
            with st.spinner(f"Fetching records for {report_site} on {report_date}..."):
                q_df = fetch_daily_questions(report_site, report_date)
                a_df = fetch_daily_answers(report_site, report_date)
                
                st.subheader(f"Questions ({len(q_df)})")
                if not q_df.empty:
                    st.dataframe(q_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No questions found for this date.")
                    
                st.subheader(f"Answers ({len(a_df)})")
                if not a_df.empty:
                    st.dataframe(a_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No answers found for this date.")
