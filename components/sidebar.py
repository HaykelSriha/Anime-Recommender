"""Sidebar component for navigation"""

import streamlit as st


def render_sidebar():
    """Render and manage sidebar navigation"""
    with st.sidebar:
        st.markdown("<h2 style='text-align: center; color: #a78bfa;'>ANIME HUB</h2>", unsafe_allow_html=True)
        st.markdown("<hr style='border-color: #6d28d9;'>", unsafe_allow_html=True)

        page = st.radio(
            "Navigation",
            ["Home", "Browse", "My Recommendations"],
            label_visibility="collapsed"
        )

        st.markdown("<hr style='border-color: #6d28d9;'>", unsafe_allow_html=True)
        st.markdown("""
        <div style='text-align: center; color: #a78bfa; font-size: 12px; margin-top: 20px;'>
            <p>Powered by TF-IDF + NMF Hybrid ML</p>
            <p style='font-size: 11px; color: #6b7280;'>844 anime | 4,554 similarities</p>
        </div>
        """, unsafe_allow_html=True)

    return page
