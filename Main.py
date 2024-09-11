import streamlit as st

from streamlit_option_menu import option_menu
from pages import Palette_kms as plt
from pages import Dashboard as dsd
from st_pages import hide_pages

if "sidebar_state" not in st.session_state:
    st.session_state.sidebar_state = 'auto'

st.set_page_config(
    page_title="AI-KMS Prototyping",
    page_icon="house",
    layout="wide",  # 또는 wide / centered
    initial_sidebar_state = st.session_state.sidebar_state,  # auto / collapsed
    menu_items={
        'Get Help': 'https://www.naver.com',
        'Report a bug': 'https://www.naver.com',
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

def hide_sidebar():

    hide_pages(['Main', 'Palette_kms', 'Dashboard'])  #main과 pages 디렉토리에 있는 메뉴를 숨김

hide_sidebar() 

with st.sidebar:
    selected = option_menu("Main Menu", [ 'Palette', 'Dashboard'], 
            icons=[ 'book','android','list-task','gear'], menu_icon="house", default_index=0,
            styles={
            "container": {"padding": "0!important", "background-color": "#fafafa"},
            "menu-title": {"font-size": "14px","color": "green"},
            "menu-icon": {"font-size": "14px"},
            "icon": {"color": "orange", "font-size": "20px"}, 
            "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "lightblue"},
            "nav-link-selected": {"background-color": "cadetblue"},
    })
 
st.markdown(
    """
    <style>
    [data-testid="baseButton-secondary"] {
        font-size: 10px;
        color: white;
        background-color: lightskyblue;
    },
    [data-testid="stMarkdownContainer"]  {
        font-size: 10px;
        color: red;
    },
   </style>
    """,
    unsafe_allow_html=True
)

if selected == 'Palette':
    plt.app()
if selected == 'Dashboard':
    dsd.app()


