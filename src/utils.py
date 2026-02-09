"""
Utility functions for Anime Recommender App
"""

import streamlit as st
from PIL import Image
from io import BytesIO
import requests


@st.cache_data(ttl=3600)
def load_image(image_url):
    """Load and cache anime cover image from URL"""
    try:
        response = requests.get(image_url, timeout=5)
        if response.status_code == 200:
            return Image.open(BytesIO(response.content))
    except:
        pass
    return None


def render_genre_badges(genres_str, max_count=2):
    """Render genre badges HTML"""
    if not genres_str:
        return ""
    
    genre_list = genres_str.split('|')[:max_count]
    badges = " ".join([f"<span class='genre-badge'>{g}</span>" for g in genre_list])
    return badges


def format_number(value):
    """Format large numbers with commas"""
    import math
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return "N/A"


def get_episode_count(episodes):
    """Safely get episode count"""
    import math
    if episodes is None or (isinstance(episodes, float) and math.isnan(episodes)):
        return "N/A"
    try:
        return int(episodes)
    except (ValueError, TypeError):
        return "N/A"
