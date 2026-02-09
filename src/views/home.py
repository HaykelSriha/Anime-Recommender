"""Home page view"""
import streamlit as st
import pandas as pd


def render_home(recommender):
    """Render home page with stats and featured anime"""
    st.title("Anime Recommender")
    st.markdown("Discover your next favorite anime using hybrid ML recommendations.")

    # Stats
    stats = recommender.get_stats()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Anime", f"{stats.get('total_anime', 0):,}")
    avg = stats.get('avg_score')
    col2.metric("Avg Score", f"{avg:.1f}" if avg else "N/A")
    col3.metric("Similarity Pairs", f"{stats.get('similarity_scores', 0):,}")

    st.markdown("---")

    # Featured: top 6 anime
    st.subheader("Featured Anime")
    top = recommender.get_top_rated(6)

    cols = st.columns(3)
    for i, (_, anime) in enumerate(top.iterrows()):
        with cols[i % 3]:
            title = str(anime.get('title', 'Unknown') or 'Unknown')
            score = anime.get('averageScore', 0)
            score = 0 if pd.isna(score) else score
            genres = str(anime.get('genres', '') or '')
            cover = str(anime.get('coverImage', '') or '')

            if cover:
                st.image(cover, width="stretch")
            st.markdown(f"**{title}**")
            st.caption(f"Score: {score} | {genres}")
