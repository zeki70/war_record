import streamlit as st

# セッションステートの初期化（初回実行時のみ）
if 'selected_option_in_form' not in st.session_state:
    st.session_state.selected_option_in_form = "オプションA" # 初期値

options = ["オプションA", "オプションB", "オプションC"]

st.write(f"（フォームの外）現在のセッションステートの値: {st.session_state.selected_option_in_form}")

with st.form("my_test_form", clear_on_submit=False): # clear_on_submit=False を明示
    st.write("--- フォーム内部 ---")
    
    # フォーム内のセレクトボックス。key を介してセッションステートと連携。
    st.selectbox(
        "オプションを選択してください:",
        options,
        key="selected_option_in_form" # このキーで st.session_state.selected_option_in_form が更新される
    )

    st.write(f"（フォーム内部 selectbox の直後）現在のセッションステートの値: {st.session_state.selected_option_in_form}")

    submitted = st.form_submit_button("送信ボタン（動作確認用）")
    if submitted:
        st.write("フォームが送信されました！")

st.write("--- フォームの後 ---")
st.write(f"（フォームの外）現在のセッションステートの値（再確認）: {st.session_state.selected_option_in_form}")