import streamlit as st
import pandas as pd
from datetime import datetime
import io
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from streamlit.errors import StreamlitAPIException
from streamlit_cookies_manager import EncryptedCookieManager

st.set_page_config(layout="wide")

# --- 定数定義 ---
SPREADSHEET_NAME_DISPLAY = "Waic-戦績"
SPREADSHEET_ID = "1V9guZQbpV8UDU_W2pC1WBsE1hOHqIO4yTsG8oGzaPQU" # ユーザー提供のID。実際のIDに置き換えてください。
WORKSHEET_NAME = "シート1"
COLUMNS = [ 'season', 'date', 'environment', 
'my_deck', 'my_deck_type', 'opponent_deck', 'opponent_deck_type', 'first_second', 'result', 'finish_turn', 'memo' ]
NEW_ENTRY_LABEL = "（新しい値を入力）"
SELECT_PLACEHOLDER = "--- 選択してください ---" # 分析用
ALL_TYPES_PLACEHOLDER = "全タイプ" # 分析用

# --- パスワード認証のための設定 ---
def get_app_password():
    if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "password" in st.secrets["app_credentials"]:
        return st.secrets["app_credentials"]["password"]
    else:
        st.warning("アプリケーションパスワードがSecretsに設定されていません。ローカルテスト用に 'test_password' を使用します。デプロイ時には必ずSecretsを設定してください。")
        return "test_password" 
CORRECT_PASSWORD = get_app_password()

# ★追加：クッキーマネージャを初期化
# 暗号化キーは st.secrets から取得することを強く推奨します。
# キー名は任意ですが、ここでは "cookie_encryption_key" としています。
# Streamlit Cloud の場合、Secretsに COOKIE_ENCRYPTION_KEY = "あなた自身の強力な秘密のキー" のように設定してください。
cookie_encryption_key = st.secrets.get("app_credentials", {}).get("cookie_encryption_key", "YOUR_FALLBACK_DEFAULT_KEY_12345")
if cookie_encryption_key == "YOUR_FALLBACK_DEFAULT_KEY_12345":
    st.warning("クッキー暗号化キーがデフォルトのままです。Secretsに 'cookie_encryption_key' を設定してください。")

cookies = EncryptedCookieManager(
    password=cookie_encryption_key,
    # クッキーのプレフィックスやパスは必要に応じて設定できます
    # prefix="streamlit_auth_",
    # path="/",
)
if not cookies.ready(): # クッキーがロードされるまで待機 (通常は不要ですが、念のため)
    st.stop()

# --- Google Sheets 連携 ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]
def get_gspread_client():
    creds = None
    use_streamlit_secrets = False
    if hasattr(st, 'secrets'):
        try:
            if "gcp_service_account" in st.secrets:
                use_streamlit_secrets = True
        except StreamlitAPIException:
            pass 
    if use_streamlit_secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        try:
            creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
        except Exception as e:
            st.error(f"サービスアカウントの認証情報ファイル (service_account.json) の読み込みに失敗しました: {e}")
            return None
    try:
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google Sheetsへの接続に失敗しました: {e}")
        return None

# --- データ操作関数 ---
def load_data(spreadsheet_id, worksheet_name):
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを読み込めません。認証情報を確認してください。")
        empty_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS: 
            if col == 'date': empty_df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': empty_df[col] = pd.Series(dtype='Int64')
            else: empty_df[col] = pd.Series(dtype='object')
        return empty_df
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        df = get_as_dataframe(worksheet, evaluate_formulas=False, header=0, na_filter=True) 
        if df.empty and worksheet.row_count > 0 and worksheet.row_values(1):
            header_row = worksheet.row_values(1)
            df = pd.DataFrame(columns=header_row)
            expected_header = COLUMNS
            actual_header_subset = list(df.columns)[:len(expected_header)]
            # 列名が完全に一致するか、またはCOLUMNSの列がすべて含まれるかチェック
            if not (actual_header_subset == expected_header or list(df.columns) == expected_header or set(COLUMNS).issubset(set(df.columns))):
                 st.warning(f"スプレッドシートのヘッダーが期待と異なります。\n期待(一部): {expected_header}\n実際(一部): {actual_header_subset}")

        # COLUMNS に基づいて DataFrame を整形し、不足列は適切な型で追加
        temp_df = pd.DataFrame(columns=COLUMNS) # 期待する列構成で初期化
        for col in COLUMNS:
            if col in df.columns:
                temp_df[col] = df[col]
            else: # dfに列が存在しない場合は、空のSeriesを適切な型で作成
                if col == 'date': temp_df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': temp_df[col] = pd.Series(dtype='Int64')
                else: temp_df[col] = pd.Series(dtype='object')
        df = temp_df

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'finish_turn' in df.columns:
            df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')
        
        string_cols = ['my_deck_type', 'opponent_deck_type', 'my_deck', 'opponent_deck', 
                       'season', 'memo', 'first_second', 'result', 'environment'] # formatを削除
        for col in string_cols: # dfに実際に列が存在するか確認してから処理
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
            else: # 通常は上の処理で列が作られているはず
                df[col] = pd.Series(dtype='str').fillna('') #念のため
        
        df = df.reindex(columns=COLUMNS) # 最終的にCOLUMNSの順序と列構成を保証

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"スプレッドシート (ID: {spreadsheet_id}) が見つからないか、アクセス権がありません。共有設定を確認してください。")
        df = pd.DataFrame(columns=COLUMNS)
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"ワークシート '{worksheet_name}' がスプレッドシート (ID: {spreadsheet_id}) 内に見つかりません。")
        df = pd.DataFrame(columns=COLUMNS)
    except Exception as e:
        st.error(f"Google Sheetsからのデータ読み込み中に予期せぬエラーが発生しました: {type(e).__name__}: {e}")
        df = pd.DataFrame(columns=COLUMNS)
    return df

def save_data(df_one_row, spreadsheet_id, worksheet_name):
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを保存できませんでした。")
        return False
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        current_headers = []
        if worksheet.row_count > 0:
            current_headers = worksheet.row_values(1)
        if not current_headers or len(current_headers) < len(COLUMNS) or current_headers[:len(COLUMNS)] != COLUMNS :
            worksheet.update('A1', [COLUMNS], value_input_option='USER_ENTERED')
            if not current_headers: st.info("スプレッドシートにヘッダー行を書き込みました。")
            else: st.warning("スプレッドシートのヘッダーを修正しました。")
        data_to_append = []
        for col in COLUMNS:
            if col in df_one_row.columns:
                value = df_one_row.iloc[0][col]
                if pd.isna(value): data_to_append.append("") 
                elif col == 'date' and isinstance(value, (datetime, pd.Timestamp)):
                     data_to_append.append(value.strftime('%Y-%m-%d'))
                elif col == 'finish_turn' and pd.notna(value): 
                     data_to_append.append(int(value)) 
                else: data_to_append.append(str(value))
            else:
                data_to_append.append("")
        worksheet.append_row(data_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Google Sheetsへのデータ書き込み中にエラーが発生しました: {type(e).__name__}: {e}")
        return False

# --- 入力フォーム用ヘルパー関数 (シーズン絞り込み対応) ---
def get_unique_items_with_new_option(df, column_name, predefined_options=None):
    items = []
    if predefined_options is not None:
        items = list(predefined_options) 
    
    if df is not None and not df.empty and column_name in df.columns and not df[column_name].empty:
        valid_items_series = df[column_name].astype(str).replace('', pd.NA).dropna()
        if not valid_items_series.empty:
            unique_valid_items = sorted(valid_items_series.unique().tolist())
            if predefined_options is not None:
                items = sorted(list(set(items + unique_valid_items)))
            else:
                items = unique_valid_items
    
    final_options = []
    if NEW_ENTRY_LABEL not in items:
        final_options.append(NEW_ENTRY_LABEL)
    final_options.extend([item for item in items if item != NEW_ENTRY_LABEL])
    return final_options

def get_decks_for_season_input(df, selected_season):
    """入力フォーム用: 指定シーズンに登場する全デッキアーキタイプ名を取得"""
    df_to_use = df.copy() # 元のdfを変更しないようにコピー
    if selected_season and selected_season != NEW_ENTRY_LABEL and pd.notna(selected_season):
        df_to_use = df_to_use[df_to_use['season'].astype(str) == str(selected_season)]
    
    if df_to_use.empty:
        return [NEW_ENTRY_LABEL]
        
    deck_names_set = set()
    for col_name in ['my_deck', 'opponent_deck']:
        if col_name in df_to_use.columns and not df_to_use[col_name].empty:
            valid_items = df_to_use[col_name].astype(str).replace('', pd.NA).dropna()
            deck_names_set.update(d for d in valid_items.tolist() if d and d.lower() != 'nan') # nan文字列も除外
            
    if not deck_names_set:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(deck_names_set))

def get_types_for_deck_and_season_input(df, selected_season, selected_deck_name):
    """入力フォーム用: 指定シーズン・指定デッキ名に関連する全ユニークな型を取得
       (my_deck_type と opponent_deck_type の両方を参照)
    """
    if (not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name) or
        not selected_season or selected_season == NEW_ENTRY_LABEL or pd.isna(selected_season)):
        return [NEW_ENTRY_LABEL]

    df_filtered = df[df['season'].astype(str) == str(selected_season)]
    if df_filtered.empty:
        return [NEW_ENTRY_LABEL]

    types = set()
    s_deck_name_str = str(selected_deck_name)

    # 選択されたデッキ名がmy_deckの場合のmy_deck_type
    my_deck_matches = df_filtered[df_filtered['my_deck'].astype(str) == s_deck_name_str]
    if not my_deck_matches.empty and 'my_deck_type' in my_deck_matches.columns:
        valid_types = my_deck_matches['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types.update(t for t in valid_types.tolist() if t and t.lower() != 'nan')

    # 選択されたデッキ名がopponent_deckの場合のopponent_deck_type
    opponent_deck_matches = df_filtered[df_filtered['opponent_deck'].astype(str) == s_deck_name_str]
    if not opponent_deck_matches.empty and 'opponent_deck_type' in opponent_deck_matches.columns:
        valid_types = opponent_deck_matches['opponent_deck_type'].astype(str).replace('', pd.NA).dropna()
        types.update(t for t in valid_types.tolist() if t and t.lower() != 'nan')
        
    if not types:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(types))

# --- 分析用ヘルパー関数 (変更なし) ---
# ... (get_all_analyzable_deck_names, get_all_types_for_archetype は変更なし) ...
def get_all_analyzable_deck_names(df):
    my_decks = df['my_deck'].astype(str).replace('', pd.NA).dropna().unique()
    opponent_decks = df['opponent_deck'].astype(str).replace('', pd.NA).dropna().unique()
    all_decks_set = set(my_decks) | set(opponent_decks)
    return sorted([d for d in all_decks_set if d and d.lower() != 'nan'])

def get_all_types_for_archetype(df, deck_name):
    if not deck_name or deck_name == SELECT_PLACEHOLDER or pd.isna(deck_name):
        return [ALL_TYPES_PLACEHOLDER] 
    types = set()
    my_deck_matches = df[df['my_deck'].astype(str) == str(deck_name)]
    if not my_deck_matches.empty and 'my_deck_type' in my_deck_matches.columns:
        types.update(my_deck_matches['my_deck_type'].astype(str).replace('', pd.NA).dropna().tolist())
    opponent_deck_matches = df[df['opponent_deck'].astype(str) == str(deck_name)]
    if not opponent_deck_matches.empty and 'opponent_deck_type' in opponent_deck_matches.columns:
        types.update(opponent_deck_matches['opponent_deck_type'].astype(str).replace('', pd.NA).dropna().tolist())
    valid_types = sorted([t for t in list(types) if t and t.lower() != 'nan'])
    return [ALL_TYPES_PLACEHOLDER] + valid_types

# --- 分析セクション表示関数 (display_general_deck_performance と show_analysis_section は変更なし) ---
# ... (これらの関数は変更なしなので省略) ...
def display_general_deck_performance(df_to_analyze):
    st.subheader("全デッキアーキタイプ パフォーマンス概要")
    all_deck_archetypes = get_all_analyzable_deck_names(df_to_analyze) 
    if not all_deck_archetypes:
        st.info("分析可能なデッキデータが現在の絞り込み条件ではありません。")
        return
    general_performance_data = []
    for deck_a_name in all_deck_archetypes:
        if not deck_a_name: continue
        games_as_my_deck_df = df_to_analyze[df_to_analyze['my_deck'] == deck_a_name]
        wins_as_my_deck = len(games_as_my_deck_df[games_as_my_deck_df['result'] == '勝ち'])
        count_as_my_deck = len(games_as_my_deck_df)
        games_as_opponent_deck_df = df_to_analyze[df_to_analyze['opponent_deck'] == deck_a_name]
        wins_as_opponent_deck = len(games_as_opponent_deck_df[games_as_opponent_deck_df['result'] == '負け'])
        count_as_opponent_deck = len(games_as_opponent_deck_df)
        total_appearances_deck_a = count_as_my_deck + count_as_opponent_deck
        total_wins_deck_a = wins_as_my_deck + wins_as_opponent_deck
        total_losses_deck_a = total_appearances_deck_a - total_wins_deck_a
        simple_overall_win_rate_deck_a = (total_wins_deck_a / total_appearances_deck_a * 100) if total_appearances_deck_a > 0 else 0.0
        deck_a_first_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '先攻']
        deck_a_first_as_opp = games_as_opponent_deck_df[games_as_opponent_deck_df['first_second'] == '後攻']
        total_games_deck_a_first = len(deck_a_first_as_my) + len(deck_a_first_as_opp)
        wins_deck_a_first = len(deck_a_first_as_my[deck_a_first_as_my['result'] == '勝ち']) + \
                             len(deck_a_first_as_opp[deck_a_first_as_opp['result'] == '負け'])
        win_rate_deck_a_first = (wins_deck_a_first / total_games_deck_a_first * 100) if total_games_deck_a_first > 0 else None
        deck_a_second_as_my = games_as_my_deck_df[games_as_my_deck_df['first_second'] == '後攻']
        deck_a_second_as_opp = games_as_opponent_deck_df[games_as_opponent_deck_df['first_second'] == '先攻']
        total_games_deck_a_second = len(deck_a_second_as_my) + len(deck_a_second_as_opp)
        wins_deck_a_second = len(deck_a_second_as_my[deck_a_second_as_my['result'] == '勝ち']) + \
                             len(deck_a_second_as_opp[deck_a_second_as_opp['result'] == '負け'])
        win_rate_deck_a_second = (wins_deck_a_second / total_games_deck_a_second * 100) if total_games_deck_a_second > 0 else None
        matchup_win_rates_for_deck_a = []
        games_involving_deck_a = df_to_analyze[(df_to_analyze['my_deck'] == deck_a_name) | (df_to_analyze['opponent_deck'] == deck_a_name)]
        unique_opponents_faced_by_deck_a = set()
        for _idx, row in games_involving_deck_a.iterrows():
            opponent_for_this_game = None
            if row['my_deck'] == deck_a_name: opponent_for_this_game = row['opponent_deck']
            elif row['opponent_deck'] == deck_a_name: opponent_for_this_game = row['my_deck']
            if opponent_for_this_game and opponent_for_this_game != deck_a_name and \
               str(opponent_for_this_game).strip() and str(opponent_for_this_game).strip().lower() != 'nan':
                unique_opponents_faced_by_deck_a.add(opponent_for_this_game)
        if unique_opponents_faced_by_deck_a:
            for opponent_archetype_name in unique_opponents_faced_by_deck_a:
                a_vs_opp_my_games = games_involving_deck_a[(games_involving_deck_a['my_deck'] == deck_a_name) & (games_involving_deck_a['opponent_deck'] == opponent_archetype_name)]
                a_vs_opp_my_wins = len(a_vs_opp_my_games[a_vs_opp_my_games['result'] == '勝ち'])
                a_vs_opp_opponent_games = games_involving_deck_a[(games_involving_deck_a['opponent_deck'] == deck_a_name) & (games_involving_deck_a['my_deck'] == opponent_archetype_name)]
                a_vs_opp_opponent_wins = len(a_vs_opp_opponent_games[a_vs_opp_opponent_games['result'] == '負け'])
                total_games_vs_specific_opponent = len(a_vs_opp_my_games) + len(a_vs_opp_opponent_games)
                total_wins_for_a_vs_specific_opponent = a_vs_opp_my_wins + a_vs_opp_opponent_wins
                if total_games_vs_specific_opponent > 0:
                    wr = (total_wins_for_a_vs_specific_opponent / total_games_vs_specific_opponent * 100)
                    matchup_win_rates_for_deck_a.append(wr)
        avg_matchup_wr_deck_a = pd.Series(matchup_win_rates_for_deck_a).mean() if matchup_win_rates_for_deck_a else None
        if total_appearances_deck_a > 0:
            appearance_display = f"{total_appearances_deck_a} (先攻: {total_games_deck_a_first})"
            general_performance_data.append({
                "デッキアーキタイプ": deck_a_name, "総登場回数": appearance_display,
                "総勝利数": total_wins_deck_a, "総敗北数": total_losses_deck_a,
                "勝率 (%) [総合]": simple_overall_win_rate_deck_a,
                "平均マッチアップ勝率 (%)": avg_matchup_wr_deck_a,
                "先攻時勝率 (%)": win_rate_deck_a_first, "後攻時勝率 (%)": win_rate_deck_a_second,
            })
    if general_performance_data:
        gen_perf_df = pd.DataFrame(general_performance_data)
        default_sort_column = "平均マッチアップ勝率 (%)"
        if default_sort_column not in gen_perf_df.columns: default_sort_column = "勝率 (%) [総合]"
        if default_sort_column not in gen_perf_df.columns: default_sort_column = "総登場回数" 
        try:
            gen_perf_df_sorted = gen_perf_df.sort_values(by=default_sort_column, ascending=False, na_position='last').reset_index(drop=True)
        except KeyError: 
            gen_perf_df_sorted = gen_perf_df.reset_index(drop=True)
        display_cols_general = [
            "デッキアーキタイプ", "総登場回数", "総勝利数", "総敗北数", 
            "勝率 (%) [総合]", "平均マッチアップ勝率 (%)", 
            "先攻時勝率 (%)", "後攻時勝率 (%)"
        ]
        actual_display_cols_general = [col for col in display_cols_general if col in gen_perf_df_sorted.columns]
        st.dataframe(gen_perf_df_sorted[actual_display_cols_general].style.format({
            "勝率 (%) [総合]": "{:.1f}%",
            "平均マッチアップ勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
            "先攻時勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
            "後攻時勝率 (%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
        }), use_container_width=True)
    else: st.info("表示するデッキパフォーマンスデータがありません。")

def show_analysis_section(original_df):
    st.header("📊 戦績分析") 
    if original_df.empty:
        st.info("まだ分析できる戦績データがありません。")
        return
    st.subheader("絞り込み条件")
    all_seasons = [SELECT_PLACEHOLDER] + sorted([s for s in original_df['season'].astype(str).replace('', pd.NA).dropna().unique() if s and s.lower() != 'nan'])
    selected_season_for_analysis = st.selectbox("シーズンで絞り込み (任意):", options=all_seasons, key='ana_season_filter')
    all_environments = [SELECT_PLACEHOLDER] + sorted([e for e in original_df['environment'].astype(str).replace('', pd.NA).dropna().unique() if e and e.lower() != 'nan'])
    selected_environments = st.multiselect("対戦環境で絞り込み (任意):", options=all_environments, key='ana_environment_filter')
    df_for_analysis = original_df.copy()
    if selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER:
        df_for_analysis = df_for_analysis[df_for_analysis['season'] == selected_season_for_analysis]
    if selected_environments: 
        df_for_analysis = df_for_analysis[df_for_analysis['environment'].isin(selected_environments)]
    if df_for_analysis.empty:
        if (selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER) or selected_environments:
            st.warning("選択された絞り込み条件に合致するデータがありません。")
        else: st.info("分析対象のデータがありません。")
        return

    st.subheader("注目デッキ分析")
    def reset_focus_type_callback(): 
        # セッション状態の直接操作を避け、削除のみ行う
        keys_to_reset = ['ana_focus_deck_type_selector', 'inp_ana_focus_deck_type_new']
        for key in keys_to_reset:
            if key in st.session_state:
                del st.session_state[key]
    deck_names_for_focus_options = [SELECT_PLACEHOLDER] + get_all_analyzable_deck_names(df_for_analysis)
    st.selectbox("注目するデッキアーキタイプを選択:", options=deck_names_for_focus_options, key='ana_focus_deck_name_selector', on_change=reset_focus_type_callback)
    selected_focus_deck = st.session_state.get('ana_focus_deck_name_selector')
    
    if selected_focus_deck and selected_focus_deck != SELECT_PLACEHOLDER:
        types_for_focus_deck_options = get_all_types_for_archetype(df_for_analysis, selected_focus_deck)
        st.selectbox("注目デッキの型を選択 (「全タイプ」で型を問わず集計):", options=types_for_focus_deck_options, key='ana_focus_deck_type_selector')
        selected_focus_type = st.session_state.get('ana_focus_deck_type_selector')
        st.markdown("---")
        focus_deck_display_name = f"{selected_focus_deck}"
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            focus_deck_display_name += f" ({selected_focus_type})"
        st.subheader(f"「{focus_deck_display_name}」の分析結果")
        cond_my_deck_focus = (df_for_analysis['my_deck'] == selected_focus_deck)
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            cond_my_deck_focus &= (df_for_analysis['my_deck_type'] == selected_focus_type)
        focus_as_my_deck_games = df_for_analysis[cond_my_deck_focus]
        cond_opponent_deck_focus = (df_for_analysis['opponent_deck'] == selected_focus_deck)
        if selected_focus_type and selected_focus_type != ALL_TYPES_PLACEHOLDER:
            cond_opponent_deck_focus &= (df_for_analysis['opponent_deck_type'] == selected_focus_type)
        focus_as_opponent_deck_games = df_for_analysis[cond_opponent_deck_focus]
        total_appearances = len(focus_as_my_deck_games) + len(focus_as_opponent_deck_games)
        if total_appearances == 0:
            st.warning(f"「{focus_deck_display_name}」の対戦記録が現在の絞り込み条件で見つかりません。")
            return 
        wins_when_focus_is_my_deck_df = focus_as_my_deck_games[focus_as_my_deck_games['result'] == '勝ち']
        wins_when_focus_is_opponent_deck_df = focus_as_opponent_deck_games[focus_as_opponent_deck_games['result'] == '負け']
        total_wins_for_focus_deck = len(wins_when_focus_is_my_deck_df) + len(wins_when_focus_is_opponent_deck_df)
        total_losses_for_focus_deck = total_appearances - total_wins_for_focus_deck
        win_rate_for_focus_deck = (total_wins_for_focus_deck / total_appearances * 100) if total_appearances > 0 else 0.0
        win_finish_turns = []
        if not wins_when_focus_is_my_deck_df.empty: win_finish_turns.extend(wins_when_focus_is_my_deck_df['finish_turn'].dropna().tolist())
        if not wins_when_focus_is_opponent_deck_df.empty: win_finish_turns.extend(wins_when_focus_is_opponent_deck_df['finish_turn'].dropna().tolist())
        avg_win_finish_turn_val = pd.Series(win_finish_turns).mean() if win_finish_turns else None
        focus_first_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '先攻']
        focus_first_opp = focus_as_opponent_deck_games[focus_as_opponent_deck_games['first_second'] == '後攻']
        total_games_focus_first = len(focus_first_my) + len(focus_first_opp)
        wins_focus_first = len(focus_first_my[focus_first_my['result'] == '勝ち']) + len(focus_first_opp[focus_first_opp['result'] == '負け'])
        win_rate_focus_first = (wins_focus_first / total_games_focus_first * 100) if total_games_focus_first > 0 else None
        focus_second_my = focus_as_my_deck_games[focus_as_my_deck_games['first_second'] == '後攻']
        focus_second_opp = focus_as_opponent_deck_games[focus_as_opponent_deck_games['first_second'] == '先攻']
        total_games_focus_second = len(focus_second_my) + len(focus_second_opp)
        wins_focus_second = len(focus_second_my[focus_second_my['result'] == '勝ち']) + len(focus_second_opp[focus_second_opp['result'] == '負け'])
        win_rate_focus_second = (wins_focus_second / total_games_focus_second * 100) if total_games_focus_second > 0 else None
        st.markdown("**総合パフォーマンス**")
        perf_col1, perf_col2, perf_col3 = st.columns(3)
        with perf_col1:
            st.metric("総登場回数", total_appearances)
            st.metric("先攻時勝率", f"{win_rate_focus_first:.1f}%" if win_rate_focus_first is not None else "N/A",
                      help=f"先攻時 {wins_focus_first}勝 / {total_games_focus_first}戦" if total_games_focus_first > 0 else "データなし")
        with perf_col2:
            st.metric("総勝利数", total_wins_for_focus_deck)
            st.metric("後攻時勝率", f"{win_rate_focus_second:.1f}%" if win_rate_focus_second is not None else "N/A",
                      help=f"後攻時 {wins_focus_second}勝 / {total_games_focus_second}戦" if total_games_focus_second > 0 else "データなし")
        with perf_col3:
            st.metric("総合勝率", f"{win_rate_for_focus_deck:.1f}%")
            st.metric("勝利時平均ターン", f"{avg_win_finish_turn_val:.1f} T" if avg_win_finish_turn_val is not None else "N/A")
        st.markdown("**対戦相手別パフォーマンス（相性）**")
        matchup_data = []
        opponents_set = set()
        if not focus_as_my_deck_games.empty:
            for _, row in focus_as_my_deck_games[['opponent_deck', 'opponent_deck_type']].drop_duplicates().iterrows():
                opponents_set.add((str(row['opponent_deck']), str(row['opponent_deck_type'])))
        if not focus_as_opponent_deck_games.empty:
            temp_df = focus_as_opponent_deck_games[['my_deck', 'my_deck_type']].rename(columns={'my_deck': 'opponent_deck', 'my_deck_type': 'opponent_deck_type'})
            for _, row in temp_df.drop_duplicates().iterrows():
                opponents_set.add((str(row['opponent_deck']), str(row['opponent_deck_type'])))
        all_faced_opponents_tuples = sorted(list(opp_tuple for opp_tuple in opponents_set if opp_tuple[0] and opp_tuple[0].lower() != 'nan'))
        for opp_deck_name, opp_deck_type in all_faced_opponents_tuples:
            games_played_count = 0; focus_deck_wins_count = 0
            focus_deck_win_turns_vs_opp = []; focus_deck_loss_turns_vs_opp = []
            fd_vs_opp_first_games_count = 0; fd_vs_opp_first_wins_count = 0
            fd_vs_opp_second_games_count = 0; fd_vs_opp_second_wins_count = 0
            case1_games = focus_as_my_deck_games[(focus_as_my_deck_games['opponent_deck'] == opp_deck_name) & (focus_as_my_deck_games['opponent_deck_type'] == opp_deck_type)]
            games_played_count += len(case1_games)
            case1_wins_df = case1_games[case1_games['result'] == '勝ち']
            case1_losses_df = case1_games[case1_games['result'] == '負け']
            focus_deck_wins_count += len(case1_wins_df)
            focus_deck_win_turns_vs_opp.extend(case1_wins_df['finish_turn'].dropna().tolist())
            focus_deck_loss_turns_vs_opp.extend(case1_losses_df['finish_turn'].dropna().tolist())
            c1_fd_first = case1_games[case1_games['first_second'] == '先攻']
            fd_vs_opp_first_games_count += len(c1_fd_first)
            fd_vs_opp_first_wins_count += len(c1_fd_first[c1_fd_first['result'] == '勝ち'])
            c1_fd_second = case1_games[case1_games['first_second'] == '後攻']
            fd_vs_opp_second_games_count += len(c1_fd_second)
            fd_vs_opp_second_wins_count += len(c1_fd_second[c1_fd_second['result'] == '勝ち'])
            case2_games = focus_as_opponent_deck_games[(focus_as_opponent_deck_games['my_deck'] == opp_deck_name) & (focus_as_opponent_deck_games['my_deck_type'] == opp_deck_type)]
            games_played_count += len(case2_games)
            case2_focus_wins_df = case2_games[case2_games['result'] == '負け']
            case2_focus_losses_df = case2_games[case2_games['result'] == '勝ち']
            focus_deck_wins_count += len(case2_focus_wins_df)
            focus_deck_win_turns_vs_opp.extend(case2_focus_wins_df['finish_turn'].dropna().tolist())
            focus_deck_loss_turns_vs_opp.extend(case2_focus_losses_df['finish_turn'].dropna().tolist())
            c2_fd_first = case2_games[case2_games['first_second'] == '後攻']
            fd_vs_opp_first_games_count += len(c2_fd_first)
            fd_vs_opp_first_wins_count += len(c2_fd_first[c2_fd_first['result'] == '負け'])
            c2_fd_second = case2_games[case2_games['first_second'] == '先攻']
            fd_vs_opp_second_games_count += len(c2_fd_second)
            fd_vs_opp_second_wins_count += len(c2_fd_second[c2_fd_second['result'] == '負け'])
            if games_played_count > 0:
                win_rate_vs_opp = (focus_deck_wins_count / games_played_count * 100)
                avg_win_turn = pd.Series(focus_deck_win_turns_vs_opp).mean() if focus_deck_win_turns_vs_opp else None
                avg_loss_turn = pd.Series(focus_deck_loss_turns_vs_opp).mean() if focus_deck_loss_turns_vs_opp else None
                win_rate_fd_first_vs_opp = (fd_vs_opp_first_wins_count / fd_vs_opp_first_games_count * 100) if fd_vs_opp_first_games_count > 0 else None
                win_rate_fd_second_vs_opp = (fd_vs_opp_second_wins_count / fd_vs_opp_second_games_count * 100) if fd_vs_opp_second_games_count > 0 else None
                games_played_display = f"{games_played_count} (先攻: {fd_vs_opp_first_games_count})"
                matchup_data.append({
                    "対戦相手デッキ": opp_deck_name, "対戦相手デッキの型": opp_deck_type,
                    "対戦数": games_played_display, "(注目デッキの)勝利数": focus_deck_wins_count,
                    "(注目デッキの)勝率(%)": win_rate_vs_opp,
                    "勝利時平均ターン": avg_win_turn, "敗北時平均ターン": avg_loss_turn,
                    "(注目デッキの)先攻時勝率(%)": win_rate_fd_first_vs_opp, "(注目デッキの)後攻時勝率(%)": win_rate_fd_second_vs_opp
                })
        if matchup_data:
            matchup_df_specific_types = pd.DataFrame(matchup_data)
            agg_matchup_data = []
            for opp_deck_name_agg in matchup_df_specific_types['対戦相手デッキ'].unique():
                case1_agg_games_total = focus_as_my_deck_games[focus_as_my_deck_games['opponent_deck'] == opp_deck_name_agg]
                case2_agg_games_total = focus_as_opponent_deck_games[focus_as_opponent_deck_games['my_deck'] == opp_deck_name_agg]
                total_games_vs_opp_deck_agg = len(case1_agg_games_total) + len(case2_agg_games_total)
                focus_wins_agg1_df = case1_agg_games_total[case1_agg_games_total['result'] == '勝ち']
                focus_wins_agg2_df = case2_agg_games_total[case2_agg_games_total['result'] == '負け']
                total_focus_wins_vs_opp_deck_agg = len(focus_wins_agg1_df) + len(focus_wins_agg2_df)
                win_rate_vs_opp_deck_agg = (total_focus_wins_vs_opp_deck_agg / total_games_vs_opp_deck_agg * 100) if total_games_vs_opp_deck_agg > 0 else 0.0
                focus_losses_agg1_df = case1_agg_games_total[case1_agg_games_total['result'] == '負け']
                focus_losses_agg2_df = case2_agg_games_total[case2_agg_games_total['result'] == '勝ち']
                all_win_turns_agg = focus_wins_agg1_df['finish_turn'].dropna().tolist() + focus_wins_agg2_df['finish_turn'].dropna().tolist()
                all_loss_turns_agg = focus_losses_agg1_df['finish_turn'].dropna().tolist() + focus_losses_agg2_df['finish_turn'].dropna().tolist()
                avg_win_turn_agg = pd.Series(all_win_turns_agg).mean() if all_win_turns_agg else None
                avg_loss_turn_agg = pd.Series(all_loss_turns_agg).mean() if all_loss_turns_agg else None
                c1_fd_first_agg_total = case1_agg_games_total[case1_agg_games_total['first_second'] == '先攻']
                c2_fd_first_agg_total = case2_agg_games_total[case2_agg_games_total['first_second'] == '後攻']
                fd_first_games_agg_total_count = len(c1_fd_first_agg_total) + len(c2_fd_first_agg_total)
                fd_first_wins_agg_total = len(c1_fd_first_agg_total[c1_fd_first_agg_total['result'] == '勝ち']) + len(c2_fd_first_agg_total[c2_fd_first_agg_total['result'] == '負け'])
                win_rate_fd_first_agg_total = (fd_first_wins_agg_total / fd_first_games_agg_total_count * 100) if fd_first_games_agg_total_count > 0 else None
                c1_fd_second_agg_total = case1_agg_games_total[case1_agg_games_total['first_second'] == '後攻']
                c2_fd_second_agg_total = case2_agg_games_total[case2_agg_games_total['first_second'] == '先攻']
                fd_second_games_agg_total_count = len(c1_fd_second_agg_total) + len(c2_fd_second_agg_total)
                fd_second_wins_agg_total = len(c1_fd_second_agg_total[c1_fd_second_agg_total['result'] == '勝ち']) + len(c2_fd_second_agg_total[c2_fd_second_agg_total['result'] == '負け'])
                win_rate_fd_second_agg_total = (fd_second_wins_agg_total / fd_second_games_agg_total_count * 100) if fd_second_games_agg_total_count > 0 else None
                games_played_display_agg = f"{total_games_vs_opp_deck_agg} (先攻: {fd_first_games_agg_total_count})"
                if total_games_vs_opp_deck_agg > 0:
                    agg_matchup_data.append({
                        "対戦相手デッキ": opp_deck_name_agg, "対戦相手デッキの型": ALL_TYPES_PLACEHOLDER,
                        "対戦数": games_played_display_agg, "(注目デッキの)勝利数": total_focus_wins_vs_opp_deck_agg,
                        "(注目デッキの)勝率(%)": win_rate_vs_opp_deck_agg,
                        "勝利時平均ターン": avg_win_turn_agg, "敗北時平均ターン": avg_loss_turn_agg,
                        "(注目デッキの)先攻時勝率(%)": win_rate_fd_first_agg_total, "(注目デッキの)後攻時勝率(%)": win_rate_fd_second_agg_total
                    })
            matchup_df_all_types = pd.DataFrame(agg_matchup_data)
            matchup_df_combined = pd.concat([matchup_df_specific_types, matchup_df_all_types], ignore_index=True)
            if not matchup_df_combined.empty:
                matchup_df_combined['__sort_type'] = matchup_df_combined['対戦相手デッキの型'].apply(lambda x: ('0_AllTypes' if x == ALL_TYPES_PLACEHOLDER else '1_' + str(x)))
                matchup_df_final = matchup_df_combined.sort_values(by=["対戦相手デッキ", "__sort_type"]).drop(columns=['__sort_type']).reset_index(drop=True)
                st.dataframe(matchup_df_final.style.format({
                    "(注目デッキの)勝率(%)": "{:.1f}%",
                    "勝利時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "敗北時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "(注目デッキの)先攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
                    "(注目デッキの)後攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A"
                }), use_container_width=True)
            else: st.info(f"「{focus_deck_display_name}」の対戦相手別の記録が見つかりません。")
        else: st.info(f"「{focus_deck_display_name}」の対戦相手別の記録が見つかりません。")
        
        st.markdown("---")
        st.subheader(f"📝 「{focus_deck_display_name}」のメモ付き対戦記録")
        memo_filter_my_deck = (focus_as_my_deck_games['memo'].astype(str).str.strip() != '') & (focus_as_my_deck_games['memo'].astype(str).str.lower() != 'nan')
        memos_when_my_deck = focus_as_my_deck_games[memo_filter_my_deck]
        memo_filter_opponent_deck = (focus_as_opponent_deck_games['memo'].astype(str).str.strip() != '') & (focus_as_opponent_deck_games['memo'].astype(str).str.lower() != 'nan')
        memos_when_opponent_deck = focus_as_opponent_deck_games[memo_filter_opponent_deck]
        all_memo_games = pd.concat([memos_when_my_deck, memos_when_opponent_deck]).drop_duplicates().reset_index(drop=True)
        if not all_memo_games.empty:
            memo_display_cols = ['date', 'season', 'environment', 'my_deck', 'my_deck_type', 'opponent_deck', 'opponent_deck_type', 'first_second', 'result', 'finish_turn', 'memo']
            actual_memo_display_cols = [col for col in memo_display_cols if col in all_memo_games.columns]
            df_memo_display = all_memo_games[actual_memo_display_cols].copy()
            if 'date' in df_memo_display.columns:
                df_memo_display['date'] = pd.to_datetime(df_memo_display['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            st.dataframe(df_memo_display.sort_values(by='date', ascending=False), use_container_width=True)
        else: st.info(f"「{focus_deck_display_name}」に関するメモ付きの記録は、現在の絞り込み条件ではありません。")
    else: # 注目デッキが選択されていない場合
        display_general_deck_performance(df_for_analysis) # 新しい関数を呼び出し

# --- Streamlit アプリ本体 (main関数) ---
def main():
    
    st.title("カードゲーム戦績管理アプリ (" + SPREADSHEET_NAME_DISPLAY + ")")

    if SPREADSHEET_ID == "ここに実際の Waic-戦績 のスプレッドシートIDを貼り付け":
        st.error("コード内の SPREADSHEET_ID を、お使いのGoogleスプレッドシートの実際のIDに置き換えてください。")
        st.warning("スプレッドシートIDは、スプレッドシートのURLに含まれる長い英数字の文字列です。")
        st.code("https://docs.google.com/spreadsheets/d/【この部分がIDです】/edit")
        st.stop()
    
    # --- ▼▼▼ 認証処理の変更 ▼▼▼ ---
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    # ★追加：アプリ起動時にクッキーを確認し、自動ログインを試みる
    if not st.session_state.authenticated: # まだst.session_stateで認証されていなければ
        try:
            stored_password_from_cookie = cookies.get('auth_password') # クッキーから保存されたパスワードを取得
            if stored_password_from_cookie and stored_password_from_cookie == CORRECT_PASSWORD:
                st.session_state.authenticated = True
                # 自動ログイン成功時は st.rerun() を呼ばない方がスムーズな場合がある
                # st.rerun() # 必要に応じて呼び出す
        except Exception as e:
            # クッキーのデコードエラーやその他の問題が発生した場合のフォールバック
            st.warning(f"クッキーの読み取り中にエラーが発生しました: {e}")
            pass # ログインフォームに進む

    if not st.session_state.authenticated:
        st.title("アプリへのログイン")
        login_col1, login_col2, login_col3 = st.columns([1,1,1])
        with login_col2:
            with st.form("login_form_main"):
                st.markdown("#### パスワードを入力してください")
                password_input = st.text_input("パスワード", type="password", key="password_input_field_main", label_visibility="collapsed")
                login_button = st.form_submit_button("ログイン")
                if login_button:
                    if password_input == CORRECT_PASSWORD:
                        st.session_state.authenticated = True
                        # ★追加：ログイン成功時にパスワードをクッキーに保存
                        cookies['auth_password'] = CORRECT_PASSWORD
                        # クッキーの有効期限を設定（例: 365日）
                        # cookies.set('auth_password', CORRECT_PASSWORD, expires_at=datetime.now() + timedelta(days=365))
                        # ↑ timedelta を使う場合は from datetime import timedelta が必要
                        # EncryptedCookieManager では set 時に expires_at を直接は指定できないようです。
                        # CookieManager の save メソッドでグローバルな有効期限を設定するか、
                        # ライブラリのドキュメントで詳細な有効期限設定方法を確認する必要があります。
                        # ここでは、ライブラリのデフォルトの有効期限（またはブラウザセッション）に依存します。
                        # より長期間の保持のためには、CookieManager の設定を調べるか、
                        # 単純にキーが存在し、CORRECT_PASSWORDと一致するかどうかで判断します。
                        # (EncryptedCookieManagerのデフォルトでは永続的なクッキーになることが多いです)
                        cookies.save() # 変更をクッキーに保存
                        st.rerun()
                    else:
                        st.error("パスワードが正しくありません。")
        st.stop()
    # --- ▲▲▲ 認証処理の変更ここまで ▲▲▲ ---

    df = load_data(SPREADSHEET_ID, WORKSHEET_NAME)

    # --- on_change コールバック関数の定義 (main関数内に移動) ---
    def on_season_select_change_input_form(): # 名前を変更して分析セクションのコールバックと区別
        keys_to_reset = [
            'inp_my_deck', 'inp_my_deck_new', 
            'inp_my_deck_type', 'inp_my_deck_type_new',
            'inp_opponent_deck', 'inp_opponent_deck_new',
            'inp_opponent_deck_type', 'inp_opponent_deck_type_new'
        ]
        for key in keys_to_reset:
            if key.endswith("_new"):
                if key in st.session_state: st.session_state[key] = ""
            else:
                if key in st.session_state: st.session_state[key] = NEW_ENTRY_LABEL
    
    def on_my_deck_select_change_input_form():
        if 'inp_my_deck_type' in st.session_state: st.session_state.inp_my_deck_type = NEW_ENTRY_LABEL
        if 'inp_my_deck_type_new' in st.session_state: st.session_state.inp_my_deck_type_new = ""

    def on_opponent_deck_select_change_input_form():
        if 'inp_opponent_deck_type' in st.session_state: st.session_state.inp_opponent_deck_type = NEW_ENTRY_LABEL
        if 'inp_opponent_deck_type_new' in st.session_state: st.session_state.inp_opponent_deck_type_new = ""
    # --- コールバック定義ここまで ---

    with st.expander("戦績を入力する", expanded=True):
        st.subheader("対戦情報")
        # シーズン選択 (過去の入力からも選択できるように get_unique_items_with_new_option を使用)
        season_options_input = get_unique_items_with_new_option(df, 'season')
        st.selectbox("シーズン *", season_options_input, key='inp_season_select', 
                     help="例: 2025年前期, 〇〇カップ", on_change=on_season_select_change_input_form) # on_change設定
        if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいシーズン名を入力 *", value=st.session_state.get('inp_season_new', ""), key='inp_season_new')
        
        default_dt_for_input = datetime.today().date()
        inp_date_value = st.session_state.get('inp_date', default_dt_for_input)
        if isinstance(inp_date_value, datetime): inp_date_value = inp_date_value.date()
        elif not isinstance(inp_date_value, type(default_dt_for_input)):
            try: inp_date_value = pd.to_datetime(inp_date_value).date()
            except: inp_date_value = default_dt_for_input
        st.date_input("対戦日", value=inp_date_value, key='inp_date') # 日付は保持
        
        predefined_environments = ["Waic内", "野良", "大会"]
        unique_past_environments = []
        if 'environment' in df.columns and not df.empty and not df['environment'].dropna().empty:
            valid_items = df['environment'].astype(str).replace('', pd.NA).dropna()
            if not valid_items.empty: unique_past_environments = sorted(valid_items.unique().tolist())
        current_environments = list(set(predefined_environments + unique_past_environments))
        environment_options_input = [NEW_ENTRY_LABEL] + sorted([opt for opt in current_environments if opt and opt != NEW_ENTRY_LABEL])
        st.selectbox("対戦環境 *", environment_options_input, key='inp_environment_select')
        if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL:
            st.text_input("新しい対戦環境を入力 *", value=st.session_state.get('inp_environment_new', ""), key='inp_environment_new')

        current_selected_season_input = st.session_state.get('inp_season_select')
        deck_name_options_input = get_decks_for_season_input(df, current_selected_season_input)
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("自分のデッキ")
            st.selectbox("使用デッキ *", deck_name_options_input, key='inp_my_deck', on_change=on_my_deck_select_change_input_form) # on_change設定
            if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL:
                st.text_input("新しい使用デッキ名を入力 *", value=st.session_state.get('inp_my_deck_new', ""), key='inp_my_deck_new')
            
            current_my_deck_name_input = st.session_state.get('inp_my_deck')
            my_deck_type_options_input = get_types_for_deck_and_season_input(df, current_selected_season_input, current_my_deck_name_input)
            st.selectbox("使用デッキの型 *", my_deck_type_options_input, key='inp_my_deck_type')
            if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL:
                st.text_input("新しい使用デッキの型を入力 *", value=st.session_state.get('inp_my_deck_type_new', ""), key='inp_my_deck_type_new')
        
        with col2:
            st.subheader("対戦相手のデッキ")
            st.selectbox("相手デッキ *", deck_name_options_input, key='inp_opponent_deck', on_change=on_opponent_deck_select_change_input_form) # on_change設定
            if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL:
                st.text_input("新しい相手デッキ名を入力 *", value=st.session_state.get('inp_opponent_deck_new', ""), key='inp_opponent_deck_new')

            current_opponent_deck_name_input = st.session_state.get('inp_opponent_deck')
            opponent_deck_type_options_input = get_types_for_deck_and_season_input(df, current_selected_season_input, current_opponent_deck_name_input)
            st.selectbox("相手デッキの型 *", opponent_deck_type_options_input, key='inp_opponent_deck_type')
            if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL:
                st.text_input("新しい相手デッキの型を入力 *", value=st.session_state.get('inp_opponent_deck_type_new', ""), key='inp_opponent_deck_type_new')
        
        st.subheader("対戦結果")
        res_col1, res_col2, res_col3 = st.columns(3)
        with res_col1:
            st.selectbox("自分の先攻/後攻 *", ["先攻", "後攻"], key='inp_first_second', index=0 if 'inp_first_second' not in st.session_state else ["先攻", "後攻"].index(st.session_state.inp_first_second))
        with res_col2:
            st.selectbox("勝敗 *", ["勝ち", "負け"], key='inp_result', index=0 if 'inp_result' not in st.session_state else ["勝ち", "負け"].index(st.session_state.inp_result))
        with res_col3:
            st.number_input("決着ターン *", min_value=1, step=1, value=st.session_state.get('inp_finish_turn', 3), placeholder="ターン数を入力", key='inp_finish_turn')
        st.text_area("対戦メモ (任意)", value=st.session_state.get('inp_memo', ""), key='inp_memo')

        st.markdown("---")
        error_placeholder = st.empty()
        success_placeholder = st.empty()

        if st.button("戦績を記録", key='submit_record_button'):
            final_season = st.session_state.get('inp_season_new', '') if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_season_select')
            final_my_deck = st.session_state.get('inp_my_deck_new', '') if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck')
            final_my_deck_type = st.session_state.get('inp_my_deck_type_new', '') if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck_type')
            final_opponent_deck = st.session_state.get('inp_opponent_deck_new', '') if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck')
            final_opponent_deck_type = st.session_state.get('inp_opponent_deck_type_new', '') if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL else st.session_state.get('inp_opponent_deck_type')
            final_environment = st.session_state.get('inp_environment_new', '') if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_environment_select')
            if final_environment == NEW_ENTRY_LABEL : final_environment = ''
            date_val_from_state = st.session_state.get('inp_date')
            if isinstance(date_val_from_state, datetime): date_val = date_val_from_state.date()
            elif isinstance(date_val_from_state, type(datetime.today().date())): date_val = date_val_from_state
            else: 
                try: date_val = pd.to_datetime(date_val_from_state).date()
                except: date_val = datetime.today().date()
            first_second_val = st.session_state.get('inp_first_second')
            result_val = st.session_state.get('inp_result')
            finish_turn_val = st.session_state.get('inp_finish_turn')
            memo_val = st.session_state.get('inp_memo', '')
            
            error_messages = []
            if not final_season or final_season == NEW_ENTRY_LABEL: error_messages.append("シーズンを入力または選択してください。")
            if not final_my_deck or final_my_deck == NEW_ENTRY_LABEL: error_messages.append("使用デッキ名を入力または選択してください。")
            if not final_my_deck_type or final_my_deck_type == NEW_ENTRY_LABEL: error_messages.append("使用デッキの型を入力または選択してください。")
            if not final_opponent_deck or final_opponent_deck == NEW_ENTRY_LABEL: error_messages.append("相手デッキ名を入力または選択してください。")
            if not final_opponent_deck_type or final_opponent_deck_type == NEW_ENTRY_LABEL: error_messages.append("相手デッキの型を入力または選択してください。")
            if not final_environment or final_environment == NEW_ENTRY_LABEL: error_messages.append("対戦環境を選択または入力してください。")
            if finish_turn_val is None: error_messages.append("決着ターンを入力してください。")


            if error_messages:
                error_placeholder.error("、".join(error_messages))
                success_placeholder.empty()
            else:
                error_placeholder.empty()
                new_record_data = {
                    'season': final_season, 'date': pd.to_datetime(date_val),
                    'environment': final_environment, 
                    'my_deck': final_my_deck, 'my_deck_type': final_my_deck_type,
                    'opponent_deck': final_opponent_deck, 'opponent_deck_type': final_opponent_deck_type,
                    'first_second': first_second_val, 'result': result_val,
                    'finish_turn': int(finish_turn_val) if finish_turn_val is not None else None,
                    'memo': memo_val
                }
                new_df_row = pd.DataFrame([new_record_data], columns=COLUMNS)
                if save_data(new_df_row, SPREADSHEET_ID, WORKSHEET_NAME):
                    success_placeholder.success("戦績を記録しました！")
                    # セッション状態の直接操作を避け、特定のキーのみクリア
                    keys_to_clear = [
                        'inp_memo',  # メモフィールドのみクリア
                        # 他のフィールドは保持してユーザビリティを向上
                    ]
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    st.rerun()
                else:
                    error_placeholder.error("データの保存に失敗しました。Google Sheetsへの接続を確認してください。")

    show_analysis_section(df.copy())
    st.header("戦績一覧")
    if df.empty:
        st.info("まだ戦績データがありません。")
    else:
        display_columns = ['date', 'season', 'environment', 'my_deck', 'my_deck_type', 'opponent_deck', 'opponent_deck_type', 'first_second', 'result', 'finish_turn', 'memo']
        cols_to_display_actual = [col for col in display_columns if col in df.columns]
        df_display = df.copy()
        if 'date' in df_display.columns:
            df_display['date'] = pd.to_datetime(df_display['date'], errors='coerce')
            not_nat_dates = df_display.dropna(subset=['date'])
            nat_dates = df_display[df_display['date'].isna()]
            df_display_sorted = pd.concat([not_nat_dates.sort_values(by='date', ascending=False), nat_dates]).reset_index(drop=True)
            if pd.api.types.is_datetime64_any_dtype(df_display_sorted['date']):
                 df_display_sorted['date'] = df_display_sorted['date'].apply(
                     lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
        else:
            df_display_sorted = df_display.reset_index(drop=True)
        st.dataframe(df_display_sorted[cols_to_display_actual])
        csv_export = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="戦績データをCSVでダウンロード", data=csv_export,
            file_name='game_records_download.csv', mime='text/csv',
        )

if __name__ == '__main__':
    main()