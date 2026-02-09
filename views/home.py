"""
Home page - Featured anime and statistics
"""

import streamlit as st
import pandas as pd
from src.recommender import AnimeRecommender
from components.anime_card import render_anime_card_featured


def render_home(recommender):
    """Render home page with featured content and stats"""
    
    # Hero Banner
    st.markdown("""
    <div class='featured-card'>
        <div class='featured-title'>Welcome to Anime Hub</div>
        <div class='featured-desc'>Discover your next favorite anime with our advanced recommendation engine. Browse thousands of titles, find top-rated gems, and get personalized suggestions.</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Statistics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Anime", len(recommender.df))
    
    with col2:
        st.metric("Genres", len(recommender.get_all_genres()))
    
    with col3:
        avg_score = recommender.df['averageScore'].dropna().mean()
        avg_display = f"{avg_score:.1f}" if pd.notna(avg_score) else "N/A"
        st.metric("Avg Rating", avg_display)
    
    # Featured picks
    st.markdown("### Featured Picks")
    
    featured = recommender.get_top_rated(6)
    cols = st.columns(3)
    
    for idx, (_, anime) in enumerate(featured.iterrows()):
        with cols[idx % 3]:
            render_anime_card_featured(anime)
