"""
Browse page - Filter and explore anime
"""

import streamlit as st
import pandas as pd
from components.anime_card import render_anime_card_simple


def render_browse(recommender):
    """Render browse page with filtering options"""
    
    st.markdown("<h1 style='color: #a78bfa;'>Browse Anime</h1>", unsafe_allow_html=True)
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        selected_genre = st.selectbox(
            "Filter by Genre:",
            ["All Genres"] + recommender.get_all_genres(),
            key="browse_genre"
        )
    
    with col2:
        sort_by = st.selectbox(
            "Sort by:",
            ["Popularity", "Average Score", "Episodes"],
            key="browse_sort"
        )
    
    # Apply filters
    if selected_genre == "All Genres":
        filtered_df = recommender.df.copy()
    else:
        filtered_df = recommender.filter_by_genre(selected_genre)
    
    # Apply sorting
    if sort_by == "Popularity":
        filtered_df = filtered_df.sort_values('popularity', ascending=False)
    elif sort_by == "Average Score":
        filtered_df = filtered_df.sort_values('averageScore', ascending=False)
    else:
        filtered_df = filtered_df.sort_values('episodes', ascending=False, na_position='last')
    
    # Display results
    st.markdown(f"<p style='color: #a78bfa; font-weight: 600;'>Found {len(filtered_df)} anime</p>", unsafe_allow_html=True)
    
    cols = st.columns(3)
    for idx, (_, anime) in enumerate(filtered_df.iterrows()):
        with cols[idx % 3]:
            render_anime_card_simple(anime, show_image=True)
