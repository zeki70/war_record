import streamlit as st
import pandas as pd
from datetime import datetime
import io
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from streamlit.errors import StreamlitAPIException # エラー処理用

# --- 定数定義 ---
SPREADSHEET_NAME_DISPLAY = "Waic-戦績" # 表示用のスプレッドシート名 (これはそのままでOK)
# ↓↓↓ ★★★ この行を必ず追加・修正してください ★★★ ↓↓↓
SPREADSHEET_ID = "1V9guZQbpV8UDU_W2pC1WBsE1hOHqIO4yTsG8oGzaPQU" 
# ↑↑↑ ★★★ ご自身のスプレッドシートのIDに置き換えてください ★★★ ↑↑↑
WORKSHEET_NAME = "シート1"

COLUMNS = [
    'season', 'date', 'environment', 'my_deck', 'my_deck_type',
    'opponent_deck', 'opponent_deck_type', 'first_second',
    'result', 'finish_turn', 'memo'
]
NEW_ENTRY_LABEL = "（新しい値を入力）"
SELECT_PLACEHOLDER = "--- 選択してください ---"
ALL_TYPES_PLACEHOLDER = "全タイプ"
# --- データ操作関数 (変更なし) ---# --- パスワード認証のための設定 ---
def get_app_password():
    """Streamlit Secretsからアプリケーションパスワードを取得する"""
    if hasattr(st, 'secrets') and "app_credentials" in st.secrets and "password" in st.secrets["app_credentials"]:
        return st.secrets["app_credentials"]["password"]

CORRECT_PASSWORD = get_app_password()
    
# --- Google Sheets 連携 ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

def get_gspread_client(): # ★★★ この関数定義が load_data より前にあることを確認 ★★★
    creds = None
    use_streamlit_secrets = False
    if hasattr(st, 'secrets'):
        try:
            if "gcp_service_account" in st.secrets:
                use_streamlit_secrets = True
        except StreamlitAPIException:
            pass 
    if use_streamlit_secrets:
        # st.info("Streamlit Secrets から認証情報を読み込みます。") 
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        # st.info("ローカルのサービスアカウントファイル (service_account.json) を試みます。")
        try:
            creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
        except Exception as e:
            st.error(f"サービスアカウントの認証情報ファイル (service_account.json) の読み込みに失敗しました: {e}")
            st.warning("ヒント: service_account.json をプロジェクトのルートに配置しているか確認してください。")
            return None
    try:
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google Sheetsへの接続に失敗しました: {e}")
        return None

# --- データ操作関数 (Google Sheets対応版、IDで開くように修正) ---
def load_data(spreadsheet_id, worksheet_name):
    client = get_gspread_client() # ← ここで呼び出されている
    # ... (以降の load_data 関数の内容は変更なし) ...
# --- データ操作関数 (Google Sheets対応版、IDで開くように修正) ---
def load_data(spreadsheet_id, worksheet_name): # ← この行を確認！
    client = get_gspread_client()
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを読み込めません。")
        empty_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS: # チューニング情報を維持
            if col == 'date': empty_df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': empty_df[col] = pd.Series(dtype='Int64')
            else: empty_df[col] = pd.Series(dtype='object')
        return empty_df

    try:
        spreadsheet = client.open_by_key(spreadsheet_id) # IDで開く
        worksheet = spreadsheet.worksheet(worksheet_name)
        # get_as_dataframe はヘッダーを自動認識しようとします
        # na_filter=True で空セルはNaNとして読み込まれる
        df = get_as_dataframe(worksheet, evaluate_formulas=False, header=0, na_filter=True) 

        if df.empty and worksheet.row_count > 0 and worksheet.row_values(1):
            header_row = worksheet.row_values(1)
            df = pd.DataFrame(columns=header_row) # 読み取ったヘッダーでDF作成
            # 期待するヘッダーと一致するか確認
            expected_header = COLUMNS
            actual_header_subset = list(df.columns)[:len(expected_header)]
            if actual_header_subset != expected_header and list(df.columns) != expected_header :
                 st.warning(f"スプレッドシートのヘッダーが期待と異なります。\n期待: {expected_header}\n実際: {header_row}")

        # COLUMNS に基づいて DataFrame を整形し、不足列は適切なチューニングで追加
        temp_df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col in df.columns:
                temp_df[col] = df[col]
            else: 
                if col == 'date': temp_df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': temp_df[col] = pd.Series(dtype='Int64')
                else: temp_df[col] = pd.Series(dtype='object')
        df = temp_df

        # データチューニングの変換とfillna
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if 'finish_turn' in df.columns:
            df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')
        
        string_cols = ['my_deck_type', 'opponent_deck_type', 'my_deck', 'opponent_deck', 
                       'season', 'memo', 'first_second', 'result', 'environment']
        for col in string_cols:
            if col in df.columns: # dfに実際に列が存在するか確認
                df[col] = df[col].astype(str).fillna('')
            else: # DataFrameに列が存在しない場合 (通常は上の処理で追加されるはず)
                df[col] = pd.Series(dtype='str').fillna('') # 空の文字列シリーズで初期化
        
        df = df.reindex(columns=COLUMNS) # 最終的にCOLUMNSの順序と列構成を保証

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"スプレッドシート (ID: {spreadsheet_id}) が見つからないか、アクセス権がありません。共有設定を確認してください。")
        df = pd.DataFrame(columns=COLUMNS) # 空のDFを返す
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"ワークシート '{worksheet_name}' がスプレッドシート (ID: {spreadsheet_id}) 内に見つかりません。")
        df = pd.DataFrame(columns=COLUMNS)
    except Exception as e:
        st.error(f"Google Sheetsからのデータ読み込み中に予期せぬエラーが発生しました: {e}")
        df = pd.DataFrame(columns=COLUMNS)
    
    return df
    # ... (コードは前回と同じ)
    try:
        df = pd.read_csv(file_path, parse_dates=['date'])
        for col in COLUMNS:
            if col not in df.columns:
                if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
                elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
                else: df[col] = pd.Series(dtype='object')
    except FileNotFoundError:
        st.info(f"データファイル {file_path} が見つかりません。初回起動時は新しいファイルが作成されます。")
        df = pd.DataFrame(columns=COLUMNS)
        for col in COLUMNS:
            if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object')
    except Exception as e:
        st.error(f"データの読み込み中にエラーが発生しました: {e}")
        df = pd.DataFrame(columns=COLUMNS)

    if 'finish_turn' in df.columns:
        df['finish_turn'] = pd.to_numeric(df['finish_turn'], errors='coerce').astype('Int64')
    string_cols = ['my_deck_type', 'opponent_deck_type', 'my_deck', 'opponent_deck', 
                   'season', 'memo', 'first_second', 'result', 'environment']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('')
        else:
            df[col] = "" 
            df[col] = df[col].astype(str).fillna('')
    return df

# save_data 関数の修正版
def save_data(df_one_row, spreadsheet_id, worksheet_name):
    client = get_gspread_client() # この関数は変更なし
    if client is None:
        st.error("Google Sheetsに接続できなかったため、データを保存できませんでした。")
        return False

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        current_headers = []
        if worksheet.row_count > 0:
            current_headers = worksheet.row_values(1)

        # ヘッダー行が存在しないか、COLUMNSと一致しない場合にヘッダーを書き込む/修正する
        if not current_headers or len(current_headers) < len(COLUMNS) or current_headers[:len(COLUMNS)] != COLUMNS :
            worksheet.update('A1', [COLUMNS], value_input_option='USER_ENTERED')
            if not current_headers: 
                st.info("スプレッドシートにヘッダー行を書き込みました。")
            else:
                st.warning("スプレッドシートのヘッダーを修正しました。")

        data_to_append = []
        for col in COLUMNS:
            if col in df_one_row.columns:
                value = df_one_row.iloc[0][col]
                # --- ここからが முக்கிய修正箇所 ---
                if pd.isna(value): # NaN, NaT, None を統一的にチェック
                    data_to_append.append("") 
                elif col == 'date' and isinstance(value, (datetime, pd.Timestamp)):
                     # pd.notna(value) は isinstance で保証されているので、直接 strftime
                     data_to_append.append(value.strftime('%Y-%m-%d'))
                elif col == 'finish_turn' and pd.notna(value): # pd.notna で <NA> (Int64の欠損値) も False になる
                     data_to_append.append(int(value)) 
                else: 
                    data_to_append.append(str(value)) # その他の型は文字列に変換
                # --- ここまでが முக்கிய修正箇所 ---
            else:
                data_to_append.append("")
        
        worksheet.append_row(data_to_append, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Google Sheetsへのデータ書き込み中にエラーが発生しました: {type(e).__name__}: {e}")
        return False
    # ... (コードは前回と同じ)
    for col in COLUMNS:
        if col not in df.columns:
            if col == 'date': df[col] = pd.Series(dtype='datetime64[ns]')
            elif col == 'finish_turn': df[col] = pd.Series(dtype='Int64')
            else: df[col] = pd.Series(dtype='object').astype(str).fillna('')
    string_cols = ['my_deck_type', 'opponent_deck_type', 'my_deck', 'opponent_deck', 
                   'season', 'memo', 'first_second', 'result', 'environment']
    for col in string_cols:
         if col in df.columns:
            df[col] = df[col].astype(str).fillna('')
    df.to_csv(file_path, index=False)

def get_unique_items_with_new_option(df, column_name, predefined_options=None): # 変更なし
    # ... (コードは前回と同じ)
    items = []
    if predefined_options is not None:
        items = list(predefined_options) 
    elif column_name in df.columns and not df[column_name].empty:
        valid_items = df[column_name].astype(str).replace('', pd.NA).dropna()
        if not valid_items.empty: items = sorted(valid_items.unique().tolist())
    
    final_options = []
    if NEW_ENTRY_LABEL not in items:
        final_options.append(NEW_ENTRY_LABEL)
    final_options.extend(items)
    return final_options

def get_combined_unique_items_with_new_option(df, column_names_list): # 変更なし
    # ... (コードは前回と同じ)
    combined_items_set = set()
    for col_name in column_names_list:
        if col_name in df.columns and not df[col_name].empty:
            valid_items = df[col_name].astype(str).replace('', pd.NA).dropna()
            if not valid_items.empty: combined_items_set.update(valid_items.tolist())
    if not combined_items_set: return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(combined_items_set))

def get_types_for_deck(df, selected_deck_name): # 入力フォーム用、変更なし
    # ... (コードは前回と同じ)
    if not selected_deck_name or selected_deck_name == NEW_ENTRY_LABEL or pd.isna(selected_deck_name):
        return [NEW_ENTRY_LABEL]
    types = set()
    s_deck_name_str = str(selected_deck_name)
    my_deck_rows = df[df['my_deck'].astype(str) == s_deck_name_str]
    if not my_deck_rows.empty and 'my_deck_type' in my_deck_rows.columns:
        valid_types = my_deck_rows['my_deck_type'].astype(str).replace('', pd.NA).dropna()
        types.update(t for t in valid_types.tolist() if t)
    if not types: return [NEW_ENTRY_LABEL]
    return [NEW_ENTRY_LABEL] + sorted(list(types))

def get_all_analyzable_deck_names(df): # 変更なし
    # ... (コードは前回と同じ)
    my_decks = df['my_deck'].astype(str).replace('', pd.NA).dropna().unique()
    opponent_decks = df['opponent_deck'].astype(str).replace('', pd.NA).dropna().unique()
    all_decks_set = set(my_decks) | set(opponent_decks)
    return sorted([d for d in all_decks_set if d and d.lower() != 'nan'])

def get_all_types_for_archetype(df, deck_name): # 変更なし
    # ... (コードは前回と同じ)
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

# --- 分析セクション表示関数 (メモ付き記録表示機能を追加) ---
# --- 分析セクション表示関数 (機能追加) ---
# --- 分析セクション表示関数 (相性表に先攻/後攻勝率追加) ---
# --- 分析セクション表示関数 (「対戦数」列の表示形式変更) ---
def show_analysis_section(original_df):
    st.header("📊 戦績分析 (注目デッキ分析)")

    if original_df.empty:
        st.info("まだ分析できる戦績データがありません。")
        return

    # (シーズンフィルタ、環境フィルタ、注目デッキ選択などの部分は変更ありません)
    # ... (コード省略) ...
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
    no_data_after_filter = False
    if (selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER and df_for_analysis.empty) or \
       (selected_environments and df_for_analysis.empty and not original_df.empty and \
        not (selected_season_for_analysis and selected_season_for_analysis != SELECT_PLACEHOLDER and len(original_df[original_df['season'] == selected_season_for_analysis]) == 0) ):
        st.warning(f"選択された絞り込み条件に合致するデータがありません。")
        no_data_after_filter = True
    elif df_for_analysis.empty and not original_df.empty :
         st.warning("現在の絞り込み条件に合致する分析対象のデータがありません。")
         no_data_after_filter = True
    if no_data_after_filter: return

    st.subheader("分析対象の選択")
    def reset_focus_type(): st.session_state.ana_focus_deck_type = ALL_TYPES_PLACEHOLDER
    deck_names_for_focus_options = [SELECT_PLACEHOLDER] + get_all_analyzable_deck_names(df_for_analysis)
    st.selectbox("注目するデッキアーキタイプを選択:", options=deck_names_for_focus_options, key='ana_focus_deck_name', on_change=reset_focus_type)
    selected_focus_deck = st.session_state.get('ana_focus_deck_name')
    types_for_focus_deck_options = get_all_types_for_archetype(df_for_analysis, selected_focus_deck)
    st.selectbox("注目デッキの型を選択 (「全タイプ」で型を問わず集計):", options=types_for_focus_deck_options, key='ana_focus_deck_type')
    selected_focus_type = st.session_state.get('ana_focus_deck_type')

    if selected_focus_deck and selected_focus_deck != SELECT_PLACEHOLDER:
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
        # (総合パフォーマンスの計算と表示は変更なし)
        # ... (コード省略) ...
        wins_when_focus_is_my_deck_df = focus_as_my_deck_games[focus_as_my_deck_games['result'] == '勝ち']
        wins_when_focus_is_opponent_deck_df = focus_as_opponent_deck_games[focus_as_opponent_deck_games['result'] == '負け']
        total_wins_for_focus_deck = len(wins_when_focus_is_my_deck_df) + len(wins_when_focus_is_opponent_deck_df)
        total_losses_for_focus_deck = total_appearances - total_wins_for_focus_deck
        win_rate_for_focus_deck = (total_wins_for_focus_deck / total_appearances * 100) if total_appearances > 0 else 0
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
        # (all_faced_opponents_tuples の作成ロジックは変更なし)
        # ... (コード省略) ...
        opponents_set = set()
        if not focus_as_my_deck_games.empty:
            for _, row in focus_as_my_deck_games[['opponent_deck', 'opponent_deck_type']].drop_duplicates().iterrows():
                opponents_set.add((str(row['opponent_deck']), str(row['opponent_deck_type'])))
        if not focus_as_opponent_deck_games.empty:
            temp_df = focus_as_opponent_deck_games[['my_deck', 'my_deck_type']].rename(columns={'my_deck': 'opponent_deck', 'my_deck_type': 'opponent_deck_type'})
            for _, row in temp_df.drop_duplicates().iterrows():
                opponents_set.add((str(row['opponent_deck']), str(row['opponent_deck_type'])))
        all_faced_opponents_tuples = sorted(list(opp_tuple for opp_tuple in opponents_set if opp_tuple[0] and opp_tuple[0].lower() != 'nan'))


        # --- 型別相性データ作成ループ内の変更 ---
        for opp_deck_name, opp_deck_type in all_faced_opponents_tuples:
            games_played_count = 0
            focus_deck_wins_count = 0
            focus_deck_win_turns_vs_opp = []
            focus_deck_loss_turns_vs_opp = []
            fd_vs_opp_first_games_count = 0 
            fd_vs_opp_first_wins_count = 0
            fd_vs_opp_second_games_count = 0
            fd_vs_opp_second_wins_count = 0

            case1_games = focus_as_my_deck_games[
                (focus_as_my_deck_games['opponent_deck'] == opp_deck_name) & 
                (focus_as_my_deck_games['opponent_deck_type'] == opp_deck_type)
            ]
            games_played_count += len(case1_games)
            case1_wins_df = case1_games[case1_games['result'] == '勝ち']
            # ... (focus_deck_wins_count, win_turns, loss_turns の計算は変更なし) ...
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

            case2_games = focus_as_opponent_deck_games[
                (focus_as_opponent_deck_games['my_deck'] == opp_deck_name) &
                (focus_as_opponent_deck_games['my_deck_type'] == opp_deck_type)
            ]
            games_played_count += len(case2_games)
            case2_focus_wins_df = case2_games[case2_games['result'] == '負け']
            # ... (focus_deck_wins_count, win_turns, loss_turns の計算は変更なし) ...
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
                
                games_played_display = f"{games_played_count} (先攻: {fd_vs_opp_first_games_count})" # ★★★ 変更点 ★★★

                matchup_data.append({
                    "対戦相手デッキ": opp_deck_name, "対戦相手デッキの型": opp_deck_type,
                    "対戦数": games_played_display, # ★★★ 変更点 ★★★
                    "(注目デッキの)勝利数": focus_deck_wins_count,
                    "(注目デッキの)勝率(%)": win_rate_vs_opp,
                    "勝利時平均ターン": avg_win_turn, "敗北時平均ターン": avg_loss_turn,
                    "(注目デッキの)先攻時勝率(%)": win_rate_fd_first_vs_opp,
                    "(注目デッキの)後攻時勝率(%)": win_rate_fd_second_vs_opp
                })
        
        if matchup_data:
            matchup_df_specific_types = pd.DataFrame(matchup_data)
            
            # --- 「全タイプ」集計ループ内の変更 ---
            agg_matchup_data = []
            for opp_deck_name_agg in matchup_df_specific_types['対戦相手デッキ'].unique():
                # (全タイプ集計の勝率、平均ターン計算は変更なし)
                # ... (コード省略) ...
                case1_agg_games_total = focus_as_my_deck_games[focus_as_my_deck_games['opponent_deck'] == opp_deck_name_agg]
                case2_agg_games_total = focus_as_opponent_deck_games[focus_as_opponent_deck_games['my_deck'] == opp_deck_name_agg]
                total_games_vs_opp_deck_agg = len(case1_agg_games_total) + len(case2_agg_games_total)
                focus_wins_agg1_df = case1_agg_games_total[case1_agg_games_total['result'] == '勝ち']
                focus_wins_agg2_df = case2_agg_games_total[case2_agg_games_total['result'] == '負け']
                total_focus_wins_vs_opp_deck_agg = len(focus_wins_agg1_df) + len(focus_wins_agg2_df)
                win_rate_vs_opp_deck_agg = (total_focus_wins_vs_opp_deck_agg / total_games_vs_opp_deck_agg * 100) if total_games_vs_opp_deck_agg > 0 else 0
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
                
                games_played_display_agg = f"{total_games_vs_opp_deck_agg} (先攻: {fd_first_games_agg_total_count})" # ★★★ 変更点 ★★★

                if total_games_vs_opp_deck_agg > 0:
                    agg_matchup_data.append({
                        "対戦相手デッキ": opp_deck_name_agg,
                        "対戦相手デッキの型": ALL_TYPES_PLACEHOLDER,
                        "対戦数": games_played_display_agg, # ★★★ 変更点 ★★★
                        "(注目デッキの)勝利数": total_focus_wins_vs_opp_deck_agg,
                        "(注目デッキの)勝率(%)": win_rate_vs_opp_deck_agg,
                        "勝利時平均ターン": avg_win_turn_agg,
                        "敗北時平均ターン": avg_loss_turn_agg,
                        "(注目デッキの)先攻時勝率(%)": win_rate_fd_first_agg_total,
                        "(注目デッキの)後攻時勝率(%)": win_rate_fd_second_agg_total
                    })
            
            matchup_df_all_types = pd.DataFrame(agg_matchup_data)
            matchup_df_combined = pd.concat([matchup_df_specific_types, matchup_df_all_types], ignore_index=True)
            
            if not matchup_df_combined.empty:
                matchup_df_combined['__sort_type'] = matchup_df_combined['対戦相手デッキの型'].apply(
                    lambda x: ('0_AllTypes' if x == ALL_TYPES_PLACEHOLDER else '1_' + str(x)))
                matchup_df_final = matchup_df_combined.sort_values(
                    by=["対戦相手デッキ", "__sort_type"]
                ).drop(columns=['__sort_type']).reset_index(drop=True)

                # 「対戦数」列は文字列になったので、style.formatからは除外
                st.dataframe(matchup_df_final.style.format({
                    "(注目デッキの)勝率(%)": "{:.1f}%",
                    "勝利時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "敗北時平均ターン": lambda x: f"{x:.1f} T" if pd.notnull(x) else "N/A",
                    "(注目デッキの)先攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A",
                    "(注目デッキの)後攻時勝率(%)": lambda x: f"{x:.1f}%" if pd.notnull(x) else "N/A"
                }), use_container_width=True)
            else:
                 st.info(f"「{focus_deck_display_name}」の対戦相手別の記録が見つかりません。")
        else:
            st.info(f"「{focus_deck_display_name}」の対戦相手別の記録が見つかりません。")
        
        # (メモ付き記録表示は変更なし)
        st.markdown("---")
        # ...
    else:
        st.info("分析する注目デッキを選択してください。")

# (main関数や他のヘルパー関数は変更ありませんので、ここでは省略します)

# --- Streamlit アプリ本体 (main関数) ---
def main():
    st.set_page_config(layout="wide")
    # --- パスワード認証 ---
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False # 初期状態は未認証

    if not st.session_state.authenticated:
        st.title("アプリへのログイン")
        password_placeholder = st.empty()
        password_input = password_placeholder.text_input("パスワードを入力してください:", type="password", key="password_input_field")
        
        if st.button("ログイン", key="login_button"):
            if password_input == CORRECT_PASSWORD:
                st.session_state.authenticated = True
                password_placeholder.empty() # 入力フィールドを消す
                st.rerun() # 認証成功後にページを再読み込みしてコンテンツ表示
            else:
                st.error("パスワードが正しくありません。")
        st.stop() # 未認証の場合はここで処理を停止し、以下のメインコンテンツを表示しない

    st.title("Waic戦績管理アプリ")

    # ★★★ SPREADSHEET_ID をご自身のIDに置き換えてください ★★★
    # SPREADSHEET_ID = "ここに実際の Waic-戦績 のスプレッドシートIDを貼り付け" 
    # if SPREADSHEET_ID == "ここに実際の Waic-戦績 のスプレッドシートIDを貼り付け":
    #     st.error("コード内の SPREADSHEET_ID を、お使いのGoogleスプレッドシートの実際のIDに置き換えてください。")
    #     st.stop()

    df = load_data(SPREADSHEET_ID, WORKSHEET_NAME)

    with st.expander("戦績を入力する", expanded=True):
        # ... (入力フォームのコードは変更なし) ...
        st.subheader("対戦情報")
        season_options = get_unique_items_with_new_option(df, 'season')
        st.selectbox("シーズン *", season_options, key='inp_season_select', help="例: 2025年前期, 〇〇カップ")
        if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL:
            st.text_input("新しいシーズン名を入力 *", value=st.session_state.get('inp_season_new', ""), key='inp_season_new')
        
        default_dt_for_input = datetime.today().date()
        if 'inp_date' in st.session_state and st.session_state.inp_date is not None:
            if isinstance(st.session_state.inp_date, datetime):
                default_dt_for_input = st.session_state.inp_date.date()
            elif isinstance(st.session_state.inp_date, type(datetime.today().date())):
                default_dt_for_input = st.session_state.inp_date
            else: 
                try: default_dt_for_input = pd.to_datetime(st.session_state.inp_date).date()
                except: pass
        st.date_input("対戦日", value=default_dt_for_input, key='inp_date')
        
        predefined_environments = ["Waic内", "野良", "大会"]
        unique_past_environments = []
        if 'environment' in df.columns and not df.empty and not df['environment'].dropna().empty:
            valid_items = df['environment'].astype(str).replace('', pd.NA).dropna()
            if not valid_items.empty: unique_past_environments = sorted(valid_items.unique().tolist())
        combined_env_options = sorted(list(set(predefined_environments + unique_past_environments)))
        environment_options = [NEW_ENTRY_LABEL] + [opt for opt in combined_env_options if opt and opt != NEW_ENTRY_LABEL]
        st.selectbox("対戦環境", environment_options, key='inp_environment_select')
        if st.session_state.get('inp_environment_select') == NEW_ENTRY_LABEL:
            st.text_input("新しい対戦環境を入力", value=st.session_state.get('inp_environment_new', ""), key='inp_environment_new')

        deck_name_options = get_combined_unique_items_with_new_option(df, ['my_deck', 'opponent_deck'])
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("自分のデッキ")
            st.selectbox("使用デッキ *", deck_name_options, key='inp_my_deck')
            if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL:
                st.text_input("新しい使用デッキ名を入力 *", value=st.session_state.get('inp_my_deck_new', ""), key='inp_my_deck_new')
            my_deck_name_for_type_options = st.session_state.get('inp_my_deck', NEW_ENTRY_LABEL)
            my_deck_type_options = get_types_for_deck(df, my_deck_name_for_type_options)
            st.selectbox("使用デッキのチューニング(特になかったらテンプレで) *", my_deck_type_options, key='inp_my_deck_type')
            if st.session_state.get('inp_my_deck_type') == NEW_ENTRY_LABEL:
                st.text_input("新しい使用デッキのチューニング(特になかったらテンプレで)を入力 *", value=st.session_state.get('inp_my_deck_type_new', ""), key='inp_my_deck_type_new')
        with col2:
            st.subheader("対戦相手のデッキ")
            st.selectbox("相手デッキ *", deck_name_options, key='inp_opponent_deck')
            if st.session_state.get('inp_opponent_deck') == NEW_ENTRY_LABEL:
                st.text_input("新しい相手デッキ名を入力 *", value=st.session_state.get('inp_opponent_deck_new', ""), key='inp_opponent_deck_new')
            opponent_deck_name_for_type_options = st.session_state.get('inp_opponent_deck', NEW_ENTRY_LABEL)
            opponent_deck_type_options = get_types_for_deck(df, opponent_deck_name_for_type_options)
            st.selectbox("相手デッキのチューニング(特になかったらテンプレで) *", opponent_deck_type_options, key='inp_opponent_deck_type')
            if st.session_state.get('inp_opponent_deck_type') == NEW_ENTRY_LABEL:
                st.text_input("新しい相手デッキのチューニング(特になかったらテンプレで)を入力 *", value=st.session_state.get('inp_opponent_deck_type_new', ""), key='inp_opponent_deck_type_new')
        
        st.subheader("対戦結果")
        res_col1, res_col2, res_col3 = st.columns(3)
        with res_col1:
            st.selectbox("自分の先攻/後攻 *", ["先攻", "後攻"], key='inp_first_second')
        with res_col2:
            st.selectbox("勝敗 *", ["勝ち", "負け"], key='inp_result')
        with res_col3:
            st.number_input("決着ターン *", min_value=1, step=1, value=st.session_state.get('inp_finish_turn', 3), placeholder="ターン数を入力", key='inp_finish_turn')
        st.text_area("対戦メモ (任意)", value=st.session_state.get('inp_memo', ""), key='inp_memo')

        st.markdown("---")
        error_placeholder = st.empty()
        success_placeholder = st.empty()

        if st.button("戦績を記録", key='submit_record_button'):
            # ... (記録処理のコードは変更なし) ...
            final_season = st.session_state.get('inp_season_new', '') if st.session_state.get('inp_season_select') == NEW_ENTRY_LABEL else st.session_state.get('inp_season_select')
            final_my_deck = st.session_state.get('inp_my_deck_new', '') if st.session_state.get('inp_my_deck') == NEW_ENTRY_LABEL else st.session_state.get('inp_my_deck')
            # ... (他の final_... 変数の取得も同様) ...
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
            # ... (他のバリデーションも同様) ...
            if not final_my_deck or final_my_deck == NEW_ENTRY_LABEL: error_messages.append("使用デッキ名を入力または選択してください。")
            if not final_my_deck_type or final_my_deck_type == NEW_ENTRY_LABEL: error_messages.append("使用デッキのチューニング(特になかったらテンプレで)を入力または選択してください。")
            if not final_opponent_deck or final_opponent_deck == NEW_ENTRY_LABEL: error_messages.append("相手デッキ名を入力または選択してください。")
            if not final_opponent_deck_type or final_opponent_deck_type == NEW_ENTRY_LABEL: error_messages.append("相手デッキのチューニング(特になかったらテンプレで)を入力または選択してください。")
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
                    keys_to_delete_after_submit = [
                        'inp_date', 'inp_first_second', 'inp_result', 
                        'inp_finish_turn', 'inp_memo'
                    ]
                    for key in keys_to_delete_after_submit:
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
        # ... (戦績一覧表示のコードは変更なし) ...
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