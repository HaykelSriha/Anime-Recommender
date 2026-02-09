"""
Anime Recommender Application
Main entry point for the Streamlit app
"""

import streamlit as st
from src.recommender import AnimeRecommender
from src.styles import DARK_THEME
from components.sidebar import render_sidebar
from views.home import render_home
from views.browse import render_browse
from views.recommendations import render_recommendations


# Page configuration
st.set_page_config(
    page_title="Anime Recommender",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply theme
st.markdown(DARK_THEME, unsafe_allow_html=True)


# Load recommender (cached)
@st.cache_resource
def load_recommender():
    return AnimeRecommender(use_warehouse=True)


# Initialize app
recommender = load_recommender()

# Render sidebar and get selected page
selected_page = render_sidebar()

# Route to selected page
if selected_page == "Home":
    render_home(recommender)
elif selected_page == "Browse":
    render_browse(recommender)
elif selected_page == "My Recommendations":
    render_recommendations(recommender)

# Footer
st.markdown("<hr style='border-color: #6d28d9;'>", unsafe_allow_html=True)
