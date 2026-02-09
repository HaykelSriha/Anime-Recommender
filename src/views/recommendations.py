"""My Recommendations view - get personalized anime recommendations"""
import streamlit as st
import pandas as pd


def render_recommendations(recommender):
    """Get anime recommendations based on user's favorite anime"""
    st.title("My Recommendations")
    st.markdown("Tell us what anime you love, and we'll find your next favorite.")

    all_titles = sorted(recommender.df['title'].dropna().tolist())

    selected = st.multiselect(
        "What anime do you enjoy?",
        all_titles,
        max_selections=10,
        placeholder="Type to search anime..."
    )

    col1, col2 = st.columns([1, 3])
    with col1:
        n_recs = st.slider("How many?", 5, 30, 10)

    if selected:
        st.caption(f"Selected {len(selected)} anime")

    if st.button("Get Recommendations", type="primary", disabled=len(selected) == 0):
        with st.spinner("Finding anime you'll love..."):
            if len(selected) == 1:
                results = recommender.get_recommendations(selected[0], n_recs)
            else:
                results = recommender.get_multi_anime_recommendations(selected, n_recs)

        if results is None or len(results) == 0:
            st.info("No recommendations found. Try selecting different anime.")
            return

        st.success(f"Found {len(results)} recommendations for you!")
        st.markdown("---")

        for i in range(0, len(results), 3):
            cols = st.columns(3)
            for j, col in enumerate(cols):
                idx = i + j
                if idx < len(results):
                    anime = results.iloc[idx]
                    with col:
                        title = str(anime.get('title', 'Unknown') or 'Unknown')
                        score = anime.get('averageScore', 0)
                        score = 0 if pd.isna(score) else int(score)
                        genres = str(anime.get('genres', '') or '')
                        cover = str(anime.get('coverImage', '') or '')
                        similarity = anime.get('avg_similarity', anime.get('similarity_score', 0))
                        similarity = 0 if pd.isna(similarity) else float(similarity)

                        if cover:
                            st.image(cover, width="stretch")
                        st.markdown(f"**{title}**")

                        info_parts = []
                        if similarity:
                            info_parts.append(f"Match: {similarity:.0%}")
                        if score:
                            info_parts.append(f"Score: {score}")
                        if info_parts:
                            st.caption(" | ".join(info_parts))
                        if genres:
                            st.caption(genres)
