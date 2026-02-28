import streamlit as st
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
from datetime import datetime

# ==========================================
# 1. BigQuery Authentication & Initialization
# ==========================================

st.set_page_config(page_title="Forum CRM Hub (Cloud)", layout="wide", page_icon="☁️")

# IMPORTANT: Admin should update these variables to their GCP project and dataset.
PROJECT_ID = "my-new-project-480609"
DATASET_ID = "forum_crm"

USERS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.users_master"
CATEGORIES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.categories_master"
THREADS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.threads_and_posts"

def get_bq_client():
    """Initialize the BigQuery client using Streamlit Secrets or local environment variables."""
    try:
        # 1. Cloud Deployment: Check for Streamlit Secrets
        if "gcp_service_account" in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # 2. Local Deployment: Check for Environment Credentials
        elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            return bigquery.Client()
            
        else:
            st.error("⚠️ BigQuery credentials not found. Please configure Streamlit Secrets or set GOOGLE_APPLICATION_CREDENTIALS.")
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
        
        cats_query = f"SELECT site_id, category_name FROM `{CATEGORIES_TABLE}`"
        cats_df = bq_client.query(cats_query).to_dataframe()
        
        users_query = f"SELECT site_id, username, api_user_id FROM `{USERS_TABLE}`"
        users_df = bq_client.query(users_query).to_dataframe()
        
        return sites, cats_df, users_df
    except Exception as e:
        st.error(f"Error fetching metadata from BigQuery: {e}")
        return [], pd.DataFrame(), pd.DataFrame()


def fetch_filtered_threads(site_id, ans_min, ans_max, selected_user, keyword):
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

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter("selected_user", "STRING", selected_user if selected_user != "All" else ""),
                bigquery.ScalarQueryParameter("keyword", "STRING", f"%{keyword}%" if keyword else ""),
                bigquery.ScalarQueryParameter("ans_min", "INT64", ans_min),
                bigquery.ScalarQueryParameter("ans_max", "INT64", ans_max)
            ]
        )
        
        return bq_client.query(query, job_config=job_config).to_dataframe()
        
    except Exception as e:
        st.error(f"Error executing fetch_filtered_threads: {e}")
        return pd.DataFrame()


def fetch_bulk_thread_history(thread_ids, site_id):
    """BigQuery SELECT query to pull chronological thread history for ALL threads loaded into Zone 2."""
    if not bq_client or not thread_ids: return pd.DataFrame()
    try:
        query = f"""
            SELECT thread_id, post_id, username, content, post_type, timestamp, target_link, question_url, answer_url
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


def post_to_xenforo(site_id, thread_id, api_user_id, message):
    """The REST API call using requests to XenForo."""
    # Hardcoded API key as requested
    api_key = "8OtIhtd-R1BPHi13jyXT2WsHcsayoXGZ"
    
    # Custom site mapping based on site_id
    if site_id == "sportsbyte":
        site_url = f"https://sportsbyte.com/api/threads/{thread_id}/posts"
    else:
        site_url = f"https://{site_id}.com/api/threads/{thread_id}/posts" 
    
    headers = {
        "XF-Api-Key": api_key,
        "XF-Api-User": str(api_user_id),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    payload = {
        "message": message
    }
    
    try:
        response = requests.post(site_url, headers=headers, data=payload, timeout=10)
        
        if response.status_code == 200:
            return response.json().get('post', {}).get('post_id')
        else:
            st.error(f"XenForo API Error [{response.status_code}]: {response.text}")
            return None
    except requests.exceptions.ConnectionError:
        # Fallback to HTTP if HTTPS is actively refused (e.g. local dev servers or expired SSLs)
        try:
            fallback_url = site_url.replace("https://", "http://")
            response = requests.post(fallback_url, headers=headers, data=payload, timeout=10)
            if response.status_code == 200:
                return response.json().get('post', {}).get('post_id')
            else:
                st.error(f"XenForo API Error (HTTP Fallback) [{response.status_code}]: {response.text}")
                return None
        except Exception as fallback_e:
            st.error(f"XenForo Network/Request Error (Both HTTPS & HTTP Failed): {fallback_e}")
            return None
    except Exception as e:
        st.error(f"XenForo Network/Request Error: {e}")
        return None

def log_to_bigquery(site_id, thread_id, post_id, username, content, target_link):
    """BigQuery INSERT statement logging the successful API post."""
    if not bq_client: return False
    try:
        # We assume the parent thread already has a category name logged, so we just pass NULL here for Answers
        # since the UI groups by thread_id and takes MAX(category_name).
        query = f"""
            INSERT INTO `{THREADS_TABLE}` 
            (site_id, thread_id, post_id, username, content, post_type, timestamp, target_link, category_name, question_url, answer_url)
            VALUES (@site_id, @thread_id, @post_id, @username, @content, 'Answer', CURRENT_DATETIME(), @target_link, NULL, NULL, NULL)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter("thread_id", "INT64", thread_id),
                bigquery.ScalarQueryParameter("post_id", "INT64", post_id),
                bigquery.ScalarQueryParameter("username", "STRING", username),
                bigquery.ScalarQueryParameter("content", "STRING", content),
                bigquery.ScalarQueryParameter("target_link", "STRING", target_link if target_link else None)
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
    api_key = "8OtIhtd-R1BPHi13jyXT2WsHcsayoXGZ"
    
    if site_id == "sportsbyte":
        site_url = "https://sportsbyte.com/api/threads"
    else:
        site_url = f"https://{site_id}.com/api/threads"
        
    headers = {
        "XF-Api-Key": api_key,
        "XF-Api-User": str(api_user_id),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    payload = {
        "node_id": node_id,
        "title": title,
        "message": message
    }
    
    try:
        response = requests.post(site_url, headers=headers, data=payload, timeout=10)
        
        if response.status_code == 200:
            return response.json().get('thread', {}).get('thread_id')
        else:
            st.error(f"XenForo API Error [{response.status_code}]: {response.text}")
            return None
    except requests.exceptions.ConnectionError:
        # Fallback to HTTP if HTTPS is actively refused (e.g. local dev servers or expired SSLs)
        try:
            fallback_url = site_url.replace("https://", "http://")
            response = requests.post(fallback_url, headers=headers, data=payload, timeout=10)
            if response.status_code == 200:
                return response.json().get('thread', {}).get('thread_id')
            else:
                st.error(f"XenForo API Error (HTTP Fallback) [{response.status_code}]: {response.text}")
                return None
        except Exception as fallback_e:
            st.error(f"XenForo Network/Request Error (Both HTTPS & HTTP Failed): {fallback_e}")
            return None
    except Exception as e:
        st.error(f"XenForo Network/Request Error: {e}")
        return None

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
tab_main, tab_import, tab_new_thread = st.tabs(["Live Dashboard", "Bulk Data Import", "Create New Thread"])

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
                    keyword=keyword_filter
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
            
        reply_content = st.text_area("Message Body")
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
                        new_post_id = post_to_xenforo(selected_site, st.session_state.active_thread_id, api_user_id, reply_content)
                        
                        if new_post_id:
                            # Log to BQ
                            bq_logged = log_to_bigquery(
                                selected_site, 
                                st.session_state.active_thread_id, 
                                new_post_id, 
                                reply_username, 
                                reply_content, 
                                target_link
                            )
                            if bq_logged:
                                st.success("Successfully published to XenForo and logged state into BigQuery!")
                                st.rerun() # Refresh Feed and Zone 2 count
        
        with col_ai:
            st.button("Auto-Draft with Llama 3.1 (Phase 2)", disabled=True)

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
                    new_thread_id = create_xenforo_thread(new_site, node_id, api_user_id, new_title, new_content)
                    
                    if new_thread_id:
                        bq_logged = log_new_thread_to_bigquery(
                            new_site, 
                            new_thread_id, 
                            new_username, 
                            new_title, 
                            new_category, 
                            new_target_link, 
                            new_question_url
                        )
                        if bq_logged:
                            st.success(f"Successfully created thread #{new_thread_id} and logged directly to Live Dashboard!")
                            # Reset fields without blowing away entire session
                            st.rerun()
