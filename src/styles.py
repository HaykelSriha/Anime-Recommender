"""
CSS Styling for Anime Recommender App
"""

DARK_THEME = """
    <style>
    /* Main Background */
    [data-testid="stAppViewContainer"] {
        background-color: #0a0e27;
    }
    
    [data-testid="stSidebar"] {
        background-color: #111633;
    }
    
    /* Text Colors */
    * {
        color: #e0e0e0;
    }
    
    /* Headers and Titles */
    h1, h2, h3 {
        color: #a78bfa !important;
        font-weight: 700;
    }
    
    /* Anime Card Styling */
    .anime-card {
        background: linear-gradient(135deg, #1e1b4b 0%, #2d1b69 100%);
        border: 1px solid #6d28d9;
        border-radius: 12px;
        padding: 16px;
        margin: 0;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(109, 40, 217, 0.2);
        height: 225px;
        display: flex;
        flex-direction: column;
        flex-shrink: 0;
        overflow: hidden;
    }
    
    .anime-card:hover {
        border-color: #a78bfa;
        box-shadow: 0 8px 25px rgba(167, 139, 250, 0.4);
        transform: translateY(-2px);
    }
    
    /* Anime Card Container */
    .anime-card-container {
        display: flex;
        flex-direction: column;
        height: 600px;
    }

    .anime-card-container img {
        width: 100%;
        height: 350px;
        object-fit: cover;
        border-radius: 8px;
        margin-bottom: 12px;
        flex-shrink: 0;
    }

    .anime-title {
        font-size: 18px;
        font-weight: 700;
        margin-bottom: 10px;
        color: #f3e8ff;
        overflow: hidden;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        line-height: 1.4;
        max-height: 2.8em;
    }
    
    .anime-score {
        background: linear-gradient(135deg, #6d28d9 0%, #7c3aed 100%);
        padding: 6px 12px;
        border-radius: 6px;
        display: inline-block;
        margin: 5px 5px 5px 0;
        font-weight: 600;
        color: #f3e8ff;
        border: 1px solid #a78bfa;
    }
    
    .anime-info {
        font-size: 13px;
        color: #d0d0d0;
        margin-top: 10px;
        line-height: 1.6;
    }
    
    .genre-badge {
        display: inline-block;
        background: rgba(167, 139, 250, 0.1);
        border: 1px solid #a78bfa;
        color: #a78bfa;
        padding: 4px 8px;
        border-radius: 4px;
        margin: 2px;
        font-size: 11px;
    }
    
    /* Featured Section */
    .featured-card {
        background: linear-gradient(135deg, #2d1b4d 0%, #3d2563 100%);
        border: 2px solid #a78bfa;
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 8px 32px rgba(167, 139, 250, 0.3);
    }
    
    .featured-title {
        color: #f3e8ff;
        font-size: 22px;
        font-weight: 700;
        margin-bottom: 8px;
    }
    
    .featured-desc {
        color: #d0d0d0;
        font-size: 14px;
        line-height: 1.6;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #7c3aed 0%, #a78bfa 100%);
        color: #ffffff;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        box-shadow: 0 6px 20px rgba(167, 139, 250, 0.6);
        transform: translateY(-2px);
    }
    
    /* Select Boxes & Inputs */
    .stSelectbox > div > div {
        background: #1e1b4b;
        border: 1px solid #6d28d9;
        border-radius: 8px;
    }
    
    .stSlider > div > div {
        color: #a78bfa;
    }
    
    /* Info/Success Boxes */
    .stAlert {
        background-color: rgba(167, 139, 250, 0.1) !important;
        border: 1px solid #a78bfa !important;
        border-radius: 8px;
    }
    
    /* Dividers */
    hr {
        border-color: #6d28d9;
    }
    
    /* Grid Layout */
    .grid-container {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
        gap: 16px;
        margin: 20px 0;
    }
    
    /* Metric styling */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e1b4b 0%, #2d1b69 100%);
        border: 1px solid #6d28d9;
        border-radius: 8px;
        padding: 16px;
    }
    </style>
"""
