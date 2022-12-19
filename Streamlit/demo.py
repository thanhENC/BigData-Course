import streamlit as st
import pandas as pd

st.title('Chào bạn, tôi biết bạn đấy')
st.write('''
# Bạn có tin không
Không tin thì cho tui 1 chút thông tin đi
''')

name = st.text_input('Tên của bạn là')

if st.button('Xác nhận'):
    st.write(f'Xời, tôi biết bạn tên {name} mà :>>')
