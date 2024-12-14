import streamlit as st
import duckdb
import data_export_page
import overview_page
import model_page

st.set_page_config(page_title="Streamlit")

# Apply Open Sans globally, including headers and titles
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;600&display=swap');
        
        html, body, div, h1, h2, h3, h4, h5, h6, [class*="css"] {
            font-family: 'Open Sans', sans-serif !important;
        }
    </style>
""", unsafe_allow_html=True)

# CSS for button styling
st.markdown("""
    <style>
        div.stButton > button, div.stDownloadButton > button {
            background-color: #FFFFFF !important; 
            color: #007CC1 !important; 
            border: 2px solid #007CC1 !important; 
            border-radius: 8px;
            padding: 10px 20px;
            font-size: 16px;
	        width: 150px;
        }

        div.stButton > button:hover, div.stDownloadButton > button:hover {
            background-color: #007CC1 !important; 
            color: #FFFFFF !important; 
        }
    </style>
""", unsafe_allow_html=True)

# CSS for multiselect dropdown

st.markdown(
    """
    <style>
        span[data-baseweb="tag"] {
            background-color: #007CC1 !important;
        }
        
        div[data-baseweb="select"] > div {
            border: none !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# CSS for checkboxes dropdown
st.markdown(
    """
    <style>
        /* Style for checkbox only */
        input[type="checkbox"] {
            accent-color: #007CC1;
            width: 20px;
            height: 20px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

#-----------------------------------------------------

col1, col2, col3 = st.columns(3)

# Navigation
if "page" not in st.session_state:
    st.session_state.page = "Home"

def set_page(page_name):
    st.session_state.page = page_name

if st.session_state.page == "Home":

# Home page with horizontal options
    st.markdown("<h1 style='text-align: center;'>All-in-one</h1>", unsafe_allow_html=True)
    st.write("")
    st.markdown("<p style='text-align: center;'>A landing page of Estonian business entities to serve different kinds of needs - overview of the data, predictions and access to raw data.</p>", unsafe_allow_html=True)  
    st.write("")

    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Overview", on_click=set_page, args=("Overview",)):
            pass
    
    with col2:
        if st.button("Model", on_click=set_page, args=("Model",)):
            pass
    
    with col3:
        if st.button("Data Export", on_click=set_page, args=("Data Export",)):
            pass
    
    # Home page content

elif st.session_state.page == "Overview":
    if st.button("Back", on_click=set_page, args=("Home",)):
        pass
    # Overview page content
    st.markdown("<h1 style='text-align: center;'>Overview</h1>", unsafe_allow_html=True)
    overview_page.app()

elif st.session_state.page == "Model":
    if st.button("Back", on_click=set_page, args=("Home",)):
        pass
    # Machine learning page content
    st.markdown("<h1 style='text-align: center;'>Machine Learning</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>A simple proof-of-concept model for testing if an entity is likely to be audited based on given year data.</p>", unsafe_allow_html=True)
    model_page.app()

elif st.session_state.page == "Data Export":
    if st.button("Back", on_click=set_page, args=("Home",)):
        pass
    # Data export page content
    st.title("Data Export")
    st.write("Easy access to data in different forms.")
    data_export_page.app()
