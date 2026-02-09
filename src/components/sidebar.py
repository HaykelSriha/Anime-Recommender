"""Sidebar navigation component"""
import streamlit as st


def render_sidebar():
    """Render sidebar with navigation"""
    st.sidebar.title("Anime Recommender")
    st.sidebar.markdown("---")

    pages = ["Home", "Browse", "My Recommendations"]
    selected = st.sidebar.radio("Navigate", pages, label_visibility="collapsed")

    st.sidebar.markdown("---")
    st.sidebar.caption("Powered by TF-IDF + NMF Hybrid ML")
    st.sidebar.caption("844 anime | 95K ratings | 4,554 similarities")

    return selected
