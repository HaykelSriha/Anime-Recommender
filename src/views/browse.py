"""Browse anime view"""
import streamlit as st
import pandas as pd


def render_browse(recommender):
    """Browse all anime with genre filter"""
    st.title("Browse Anime")

    # Genre filter
    all_genres = recommender.get_all_genres()
    selected_genre = st.selectbox("Filter by Genre", ["All"] + all_genres)

    if selected_genre == "All":
        df = recommender.df
    else:
        df = recommender.filter_by_genre(selected_genre)

    st.caption(f"Showing {len(df)} anime")

    # Display as grid
    for i in range(0, min(len(df), 30), 3):
        cols = st.columns(3)
        for j, col in enumerate(cols):
            idx = i + j
            if idx < len(df):
                anime = df.iloc[idx]
                with col:
                    title = str(anime.get('title', 'Unknown') or 'Unknown')
                    score = anime.get('averageScore', 0)
                    score = 0 if pd.isna(score) else score
                    genres = str(anime.get('genres', '') or '')
                    cover = str(anime.get('coverImage', '') or '')

                    if cover:
                        st.image(cover, width="stretch")
                    st.markdown(f"**{title}**")
                    st.caption(f"Score: {score} | {genres}")
