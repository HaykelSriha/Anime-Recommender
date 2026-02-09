"""My Recommendations - Get personalized anime recommendations"""

import json
import streamlit as st
import pandas as pd
from pathlib import Path


@st.cache_data
def load_english_titles():
    """Load romaji -> english title mapping"""
    path = Path(__file__).parent.parent / "data" / "english_titles.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def render_recommendations(recommender):
    """Render My Recommendations page"""

    st.markdown("<h1 style='color: #a78bfa;'>My Recommendations</h1>", unsafe_allow_html=True)
    st.markdown("""
    <div class='featured-card'>
        <div style='color: #a78bfa; font-weight: 600; margin-bottom: 8px;'>How it works:</div>
        <div style='color: #d0d0d0; font-size: 14px;'>
            Pick one or more anime you enjoy, and our ML model will find similar titles.
            You can search by English or Japanese name.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Build searchable title list with English aliases
    english_map = load_english_titles()

    all_romaji = sorted(recommender.df["title"].dropna().tolist())

    # Create display labels: "Romaji  (English)" for titles that have English names
    display_titles = []
    display_to_romaji = {}
    for title in all_romaji:
        eng = english_map.get(title)
        if eng:
            label = f"{title}  ({eng})"
        else:
            label = title
        display_titles.append(label)
        display_to_romaji[label] = title

    selected_labels = st.multiselect(
        "What anime do you enjoy?",
        display_titles,
        max_selections=10,
        placeholder="Type to search (English or Japanese)...",
    )

    # Map back to romaji titles
    selected_titles = [display_to_romaji[lbl] for lbl in selected_labels]

    col1, col2 = st.columns([1, 3])
    with col1:
        n_recs = st.slider("How many?", 5, 30, 10)

    if selected_labels:
        st.caption(f"Selected {len(selected_labels)} anime")

    if st.button("Get Recommendations", type="primary", disabled=len(selected_labels) == 0):
        with st.spinner("Finding anime you'll love..."):
            if len(selected_titles) == 1:
                results = recommender.get_recommendations(selected_titles[0], n_recs)
            else:
                results = recommender.get_multi_anime_recommendations(selected_titles, n_recs)

        if results is None or len(results) == 0:
            st.info("No recommendations found. Try selecting different anime.")
            return

        st.success(f"Found {len(results)} recommendations for you!")
        st.markdown("---")

        for idx, (_, anime) in enumerate(results.iterrows(), 1):
            title = str(anime.get("title", "Unknown") or "Unknown")
            score = anime.get("averageScore", None)
            if pd.isna(score):
                score = None
            else:
                score = int(score)

            similarity = anime.get("avg_similarity", anime.get("similarity_score", None))
            if pd.isna(similarity):
                similarity = None
            else:
                similarity = float(similarity)

            genres = str(anime.get("genres", "") or "")
            cover = str(anime.get("coverImage", "") or "")
            episodes = anime.get("episodes", None)
            fmt = str(anime.get("format", "") or "")

            # English subtitle
            eng_title = english_map.get(title, "")

            col_img, col_info = st.columns([1, 3])

            with col_img:
                if cover:
                    st.image(cover, width=200)

            with col_info:
                st.markdown(f"**#{idx}. {title}**")
                if eng_title:
                    st.caption(eng_title)

                info_parts = []
                if similarity:
                    info_parts.append(f"Match: {similarity:.0%}")
                if score:
                    info_parts.append(f"Score: {score}/100")
                if fmt:
                    info_parts.append(fmt)
                if episodes and not pd.isna(episodes):
                    info_parts.append(f"{int(episodes)} eps")
                if info_parts:
                    st.markdown(" | ".join(info_parts))
                if genres:
                    st.caption(genres.replace("|", " | "))

            st.markdown("---")
