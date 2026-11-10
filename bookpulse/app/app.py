"""Streamlit entry point (streamlit run app/app.py): two pages.
  /         Gợi ý sách       app/home.py     user ID -> top-10 unread books, live streaming statistics
  /setting  Điều khiển demo  app/setting.py  buttons for the demo commands (stream job, reset, scripts)
"""
import streamlit as st

st.set_page_config(page_title="BookPulse", page_icon="📚", layout="wide")
st.navigation([
    st.Page("home.py", title="Gợi ý sách", icon="📚", default=True),
    st.Page("setting.py", title="Điều khiển demo", icon="⚙️", url_path="setting"),
]).run()
