import streamlit as st
import pandas as pd
import os

# --- App Title ---
st.title("📊 Reddit Leads Dashboard")

# --- Sidebar ---
st.sidebar.header("Options")
csv_file = st.sidebar.selectbox(
    "Select CSV file to display:",
    ["reddit_14d_leads.csv"],  # You can add more files later
)

# --- Load and Display CSV ---
if os.path.exists(csv_file):
    df = pd.read_csv(csv_file)
    st.success(f"Loaded data from: {csv_file}")
    st.dataframe(df)
else:
    st.error(f"File '{csv_file}' not found! Please run your scraper first.")

# --- Optional: Run scraper ---
if st.sidebar.button("Run Scraper"):
    try:
        st.info("Running scraper... please wait.")
        os.system("python reddit_live_leads.py")  # executes your scraper
        st.success("✅ Scraper finished running! Refresh to see new data.")
    except Exception as e:
        st.error(f"Error running scraper: {e}")

# --- Footer ---
st.markdown("---")
st.caption("Made by Quratulaaein 🚀 | Reddit Lead Automation")
