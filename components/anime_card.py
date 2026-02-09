"""
Anime card component for displaying anime information
"""

import streamlit as st
import pandas as pd
from src.utils import load_image, render_genre_badges, get_episode_count, format_number


def _safe_score(val):
    """Safely format averageScore, returning 'N/A' for NaN/None"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    return f"{int(val)}/100"


def _safe_str(val, default=""):
    """Safely convert to string, handling NaN/None"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val)


def render_anime_card_simple(anime, show_image=True):
    """Render a simple anime card (for grid layout)"""
    genres_html = render_genre_badges(_safe_str(anime.get('genres')), max_count=2)

    img_html = ""
    if show_image:
        cover = anime.get('coverImage')
        img = load_image(cover) if pd.notna(cover) else None
        if img:
            import base64
            from io import BytesIO
            buffered = BytesIO()
            img.save(buffered, format="PNG")
            img_str = base64.b64encode(buffered.getvalue()).decode()
            img_html = f'<img src="data:image/png;base64,{img_str}" style="width: 100%; height: 350px; object-fit: cover; border-radius: 8px; margin-bottom: 12px;">'

    st.markdown(f"""
    <div class='anime-card-container'>
        {img_html}
        <div class='anime-card'>
            <div class='anime-title'>{anime['title']}</div>
            <div class='anime-score'>{_safe_score(anime.get('averageScore'))}</div>
            <div class='anime-info'>
                Episodes: {get_episode_count(anime.get('episodes'))}<br>
                {_safe_str(anime.get('format'), 'Unknown')}<br>
                Popularity: {format_number(anime.get('popularity'))}
            </div>
            <div style='margin-top: 8px;'>{genres_html}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_anime_card_featured(anime):
    """Render a featured anime card (for home page)"""
    cover = anime.get('coverImage')
    img = load_image(cover) if pd.notna(cover) else None
    if img:
        st.image(img, width='stretch')

    st.markdown(f"""
    <div class='anime-card'>
        <div class='anime-title'>{anime['title']}</div>
        <div class='anime-score'>{_safe_score(anime.get('averageScore'))}</div>
        <div class='anime-info'>
            {get_episode_count(anime.get('episodes'))} Episodes<br>
            {_safe_str(anime.get('format'), 'Unknown')}<br>
            {format_number(anime.get('popularity'))} popularity
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_anime_card_detailed(anime, index, show_image=True):
    """Render a detailed anime card (for listings with descriptions)"""
    cover = anime.get('coverImage')
    img = load_image(cover) if pd.notna(cover) else None

    if show_image and img:
        st.image(img, width='stretch')

    genres_html = render_genre_badges(_safe_str(anime.get('genres')), max_count=3)
    desc = _safe_str(anime.get('description'))
    desc_preview = f"{desc[:200]}..." if len(desc) > 200 else desc

    st.markdown(f"""
    <div class='anime-card'>
        <div style='display: flex; justify-content: space-between; align-items: start;'>
            <div style='flex: 1;'>
                <div class='anime-title' style='font-size: 20px;'>#{index}. {anime['title']}</div>
                <div class='anime-info'>
                    {_safe_str(anime.get('format'), 'Unknown')} | {get_episode_count(anime.get('episodes'))} Episodes | {format_number(anime.get('popularity'))}
                </div>
                <p style='color: #d0d0d0; font-size: 13px; margin-top: 10px; line-height: 1.6;'>{desc_preview}</p>
                <div style='margin-top: 8px;'>{genres_html}</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
