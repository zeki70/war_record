import streamlit as st
import pandas as pd
from datetime import datetime
import io
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from streamlit.errors import StreamlitAPIException # エラー処理用

# --- 定数定義 ---
SPREADSHEET_NAME_DISPLAY = "Waic-戦績" # 表示用のスプレッドシート名
SPREADSHEET_ID = "1V9guZQbpV8UDU_W2pC1WBsE1hOHqIO4yTsG8oGzaPQU" # ★★★ ご自身のスプレッドシートIDに置き換えてください ★★★
WORKSHEET_NAME = "シート1" # 必要に応じてワークシート名を変更

COLUMNS = [
    'season', 'date', 'environment', 'my_deck', 'my_deck_type',
    'opponent_deck', 'opponent_deck_type', 'first_second',
    'result', 'finish_turn', 'memo'
]
NEW_ENTRY_LABEL = "（新しい値を入力）"
SELECT_PLACEHOLDER = "--- 選択してください ---"
ALL_TYPES_PLACEHOLDER = "全タイプ"

# --- パスワード認証のための設定 ---
def get_app_password():
    """Streamlit Secretsからアプリケーションパスワードを取得する"""
    if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "password" in st.secrets["app_credentials"]:
        return st.secrets["app_credentials"]["password"]
    else:
        # ローカル開発用にSecretsがない場合のフォールバック
        st.warning("アプリケーションパスワードがSecretsに設定されていません。ローカルテスト用に 'test_password' を使用します。デプロイ時には必ずSecretsを設定してください。")
        return "test_password" # ローカルテスト用の仮パスワード

CORRECT_PASSWORD = get_app_password()

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
        
        # na_filter=True で空セルはNaNとして読み込まれるようにする
        # keep_default_na=False, na_values=[''] を使うとより明示的に空文字をNaNとして扱える
        df = get_as_dataframe(worksheet, evaluate_formulas=False, header=0, na_filter=True, keep_default_na=False, na_values=[''])
        
        # 読み込んだ列名とCOLUMNSを比較し、不足していれば警告（より堅牢なヘッダーチェック）
        if not df.empty and list(df.columns) != COLUMNS[:len(df.columns)]: # 先頭からCOLUMNSの長さ分比較
            if set(COLUMNS).issubset(set(df.columns)): # COLUMNSがdfの列に含まれていれば並び替えのみ
                 df = df.reindex(columns=COLUMNS) # COLUMNSの順序に合わせる
            else:
                 st.warning(f"スプレッドシートのヘッダー ({list(df.columns)}) が期待される形式 ({COLUMNS}) と異なります。データが正しく読み込めない可能性があります。")
        elif df.empty and worksheet.row_count > 0: # データ行はないがヘッダー行はあるかもしれない
            header_row = worksheet.row_values(1)
            if header_row and header_row[:len(COLUMNS)] == COLUMNS: # ヘッダーが期待通りなら空のDFをCOLUMNSで作成
                df = pd.DataFrame(columns=COLUMNS)
            elif header_row: # ヘッダーが期待と異なる
                st.warning(f"スプレッドシートのヘッダーが期待と異なります。1行目: {header_row}")
                df = pd.DataFrame(columns=COLUMNS) # とりあえず期待する列でDF作成
            else: # ヘッダー行すらない完全に空のシート
                df = pd.DataFrame(columns=COLUMNS)


        # COLUMNS に基づいて DataFrame を整形し、不足列は適切な型で追加
        temp_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col in df.columns:
                temp_df[col] = df[col]
            else: 
                if col == 'date': temp_df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': temp_df[col] = pd.Series(dtype='Int64')
                else: temp_df[col] = pd.Series(dtype='object')
        df = temp_df

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'finish_turn' in df.columns:
            df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')
        
        string_cols = ['my_deck_type', 'opponent_deck_type', 'my_deck', 'opponent_deck', 
                       'season', 'memo', 'first_second', 'result', 'environment']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
            else: # このelseは基本的に上の列追加処理でカバーされるはず
                df[col] = pd.Series(dtype='str').fillna('')
        
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
                if pd.isna(value): 
                    data_to_append.append("") 
                elif col == 'date' and isinstance(value, (datetime, pd.Timestamp)):
                     data_to_append.append(value.strftime('%Y-%m-%d'))
                elif col == 'finish_turn' and pd.notna(value): 
                     data_to_append.append(int(value)) 
                else: 
                    data_to_append.append(str(value))
            else:
                data_to_append.append("")
        
        worksheet.append_row(data_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Google Sheetsへのデータ書き込み中にエラーが発生しました: {type(e).__name__}: {e}")
        return False

# --- 入力フォーム用ヘルパー関数 ---
def get_unique_items_with_new_option(df, column_name, predefined_options=None):
    items = []
    if predefined_options is not None:
        items = list(predefined_options) # 事前定義リストをコピー
    
    # dfがNoneまたは空、あるいはcolumn_nameが存在しない場合のフォールバック
    if df is None or df.empty or column_name not in df.columns or df[column_name].empty:
        pass # items は predefined_options のままか空リスト
    else:
        valid_items_series = df[column_name].astype(str).replace('', pd.NA).dropna()
        if not valid_items_series.empty:
            unique_valid_items = sorted(valid_items_series.unique().tolist())
            if predefined_options is not None: # 事前定義と過去データをマージ
                items = sorted(list(set(items + unique_valid_items)))
            else:
                items = unique_valid_items
    
    final_options = []
    if NEW_ENTRY_LABEL not in items: # NEW_ENTRY_LABELがitemsにないことを確認
        final_options.append(NEW_ENTRY_LABEL)
    final_options.extend([item for item in items if item != NEW_ENTRY_LABEL]) # 重複を避ける
    return final_options


def get_decks_for_season_input(df, selected_season):
    df_to_use = df
    if selected_season and selected_season != NEW_ENTRY_LABEL and pd.notna(selected_season):
        df_to_use = df[df['season'].astype(str) == str(selected_season)]
    
    if df_to_use.empty:
        return [NEW_ENTRY_LABEL]
        
    deck_names_set = set()
    for col_name in ['my_deck', 'opponent_deck']:
        if col_name in df_to_use.columns and not df_to_use[col_name].empty:
            valid_items = df_to_use[col_name].astype(str).replace('', pd.NA).dropna()
            deck_names_set.update(d for d in valid_items.tolist() if d)
            
    if not deck_names_set:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(deck_names_set))

def get_types_for_deck_and_season_input(df, selected_season, selected_deck_name):
    if (not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name) or
        not selected_season or selected_season == NEW_ENTRY_LABEL or pd.isna(selected_season)):
        return [NEW_ENTRY_LABEL]

    df_filtered = df[df['season'].astype(str) == str(selected_season)]
    if df_filtered.empty:
        return [NEW_ENTRY_LABEL]

    types = set()
    s_deck_name_str = str(selected_deck_name)
    
    my_deck_matches = df_filtered[df_filtered['my_deck'].astype(str) == s_deck_name_str]
    if not my_deck_matches.empty and 'my_deck_type' in my_deck_matches.columns:
        valid_types = my_deck_matches['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types.update(t for t in valid_types.tolist() if t)

    opponent_deck_matches = df_filtered[df_filtered['opponent_deck'].astype(str) == s_deck_name_str]
    if not opponent_deck_matches.empty and 'opponent_deck_type' in opponent_deck_matches.columns:
        valid_types = opponent_deck_matches['opponent_deck_type'].astype(str).replace('', pd.NA).dropna()
        types.update(t for t in valid_types.tolist() if t)
        
    if not types:
        return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(types))

# --- 分析用ヘルパー関数 ---
def get_all_analyzable_deck_names(df):
    # ... (変更なし)
    my_decks = df['my_deck'].astype(str).replace('', pd.NA).dropna().unique()
    opponent_decks = df['opponent_deck'].astype(str).replace('', pd.NA).dropna().unique()
    all_decks_set = set(my_decks) | set(opponent_decks)
    return sorted([d for d in all_decks_set if d and d.lower() != 'nan'])

def get_all_types_for_archetype(df, deck_name):
    # ... (変更なし)
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

def display_general_deck_performance(df_to_analyze):
    # ... (変更なし、前回のコード) ...
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
    # ... (この関数も変更なしなので省略) ...
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
        st.session_state.ana_focus_deck_type_selector = ALL_TYPES_PLACEHOLDER
        if 'inp_ana_focus_deck_type_new' in st.session_state:
            st.session_state.inp_ana_focus_deck_type_new = ""
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
        display_general_deck_performance(df_for_analysis)

# (main関数は前回提示したものをそのままお使いください)