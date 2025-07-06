import pandas as pd
import shutil
from pathlib import Path
from sqlalchemy import text
from typing import Optional, Tuple, Dict
from .database import db_manager
import os
import datetime
from .config import SUBJECT_CODES, TEMP_UPLOADS_DIR, CONFIRMED_DIR, BALANCE_TOLERANCE
from .bank_predictor import BankPredictor
from random import randint

class CSVProcessor:
    """
    統合CSV処理クラス - ファイル処理とデータベース操作を一元化
    """
    def __init__(self) -> None:
        # データベース処理用
        self.db = db_manager
        # 銀行分類器
        self.bank_predictor = BankPredictor()


    def process_csv_for_database(self, file_path: str, clear_temp: bool = True, check_duplicates: bool = True) -> int:
        """CSV処理（データベース保存用 - 旧JournalProcessor機能）
        
        Args:
            file_path: 処理対象のCSVファイルパス
            clear_temp: 処理前にtemp_journalテーブルをクリアするか
            check_duplicates: 重複ファイルのチェックを行うか
            
        Returns:
            処理した行数
        """
        # ファイル存在確認
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
        
        source_filename = Path(file_path).name
        
        # 重複チェック
        if check_duplicates:
            with self.db.get_connection() as conn:
                existing_count = pd.read_sql(
                    text("SELECT COUNT(*) as count FROM temp_journal WHERE source_file = :filename"),
                    conn,
                    params={'filename': source_filename}
                ).iloc[0]['count']
                
                if existing_count > 0:
                    print(f"警告: ファイル '{source_filename}' は既に処理済みです（{existing_count}行）")
                    if not clear_temp:
                        print("重複処理をスキップします。clear_temp=Trueで強制処理可能です。")
                        return 0
        
        # temp_journalテーブルクリア
        if clear_temp:
            with self.db.get_connection() as conn:
                deleted_count = conn.execute(text("DELETE FROM temp_journal")).rowcount
                if deleted_count > 0:
                    print(f"temp_journalテーブルをクリアしました（{deleted_count}行削除）")
        
        df = pd.read_csv(file_path)

        # データ型変換 - 複数の日付形式に対応
        def parse_date_flexible(date_str: str) -> pd.Timestamp:
            """YYYYMMDD形式とYYYY-MM-DD形式の両方に対応した日付パース"""
            if pd.isna(date_str):
                return pd.NaT
            
            date_str = str(date_str).strip()
            
            # YYYYMMDD形式（8桁）の場合
            if len(date_str) == 8 and date_str.isdigit():
                try:
                    return pd.to_datetime(date_str, format='%Y%m%d')
                except:
                    pass
            
            # YYYY-MM-DD形式やその他の一般的な形式
            try:
                return pd.to_datetime(date_str)
            except:
                # パースできない場合はNaTを返す
                return pd.NaT
        
        df['Date'] = df['Date'].apply(parse_date_flexible)
        
        # 日付パースに失敗した行をチェック
        invalid_dates = df[df['Date'].isna()]
        if len(invalid_dates) > 0:
            print(f"警告: {len(invalid_dates)}行の日付をパースできませんでした")
            print(invalid_dates[['Date']].head())
            # 無効な日付の行を除外
            df = df.dropna(subset=['Date'])
        
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        
        # Amount列のデータクリーニング
        # 無効な文字（'m'など）を削除し、数値に変換
        df['Amount'] = pd.to_numeric(df['Amount'].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
        # NaNの場合は0に置換
        df['Amount'] = df['Amount'].fillna(0)

        # SetIDの処理
        if 'SetID' in df.columns:
            # 既存のSetIDを使用し、Date + SetIDの形式に変換
            df['SetID'] = df['Date'].dt.strftime('%Y%m%d') + '_' + df['SetID'].astype(str).str.zfill(3)
        else:
            # SetIDが存在しない場合は従来通り生成（3桁）
            df['SetID'] = df['Date'].dt.strftime('%Y%m%d') + '_' + df['Remarks'].str.extract(r'(\d+)$')[0].fillna('000').str.zfill(3)
        
        # EntryIDの生成（SetID + 連番）
        df['EntryID'] = df.groupby('SetID').cumcount().astype(str).str.zfill(3)
        df['EntryID'] = df['SetID'].astype(str) + '_' + df['EntryID']

        # 科目名の自動変換
        df['Subject'] = df['SubjectCode'].map(SUBJECT_CODES)
        df['source_file'] = Path(file_path).name
        # remarksを全て小文字に変換し、不要な空白を削除
        df['Remarks'] = df['Remarks'].str.lower()

        # 不要な列を削除
        if 'ID' in df.columns:
            df = df.drop('ID', axis=1)

        # 列名を統一
        df = df.rename(columns={
            'Date': 'date',
            'SetID': 'set_id',
            'EntryID': 'entry_id',
            'SubjectCode': 'subject_code',
            'Amount': 'amount',
            'Remarks': 'remarks',
            'Year': 'year',
            'Month': 'month',
            'Subject': 'subject'
        })

        # 一時テーブルに保存
        with self.db.get_connection() as conn:
            df.to_sql('temp_journal', conn, if_exists='append', index=False)

        return len(df)

    def validate_sets(self) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """セット検証（複式簿記平衡チェック）
        日付とSetIDの組み合わせで平衡をチェック（remarksは関係なし）
        Carry over関連の仕訳は繰越処理のため平衡チェックから除外
        
        Returns:
            検証結果のタプル: (成功/失敗, メッセージ, エラー詳細DataFrame)
        """
        query = """
        SELECT
            set_id,
            date,
            COUNT(*) as entry_count,
            SUM(amount) as balance,
            string_agg(DISTINCT remarks, ', ') as remarks_list,
            string_agg(subject || ':' || amount::text, ', ' ORDER BY amount DESC) as entries
        FROM temp_journal
        WHERE remarks NOT ILIKE '%carry over%'
        GROUP BY set_id, date
        HAVING ABS(SUM(amount)) > :tolerance
        ORDER BY date, set_id
        """

        with self.db.get_connection() as conn:
            unbalanced = pd.read_sql(text(query), conn, params={'tolerance': BALANCE_TOLERANCE})

        if len(unbalanced) > 0:
            return False, f"不平衡なセットが{len(unbalanced)}件あります（Carry over除く）", unbalanced
        else:
            return True, "全セット平衡確認（Carry over除く）", None

    def get_trial_balance(self) -> pd.DataFrame:
        """試算表取得
        
        Returns:
            試算表のDataFrame
        """
        query = "SELECT * FROM trial_balance"
        with self.db.get_connection() as conn:
            return pd.read_sql(text(query), conn)

    def get_transaction_summary(self) -> pd.DataFrame:
        """取引集計（セット単位）
        
        Returns:
            取引集計のDataFrame
        """
        query = """
        SELECT
            date,
            set_id,
            remarks,
            COUNT(*) as entry_count,
            string_agg(subject || ':' || amount::text, ', ' ORDER BY amount DESC) as entries
        FROM temp_journal
        GROUP BY date, set_id, remarks
        ORDER BY date, set_id
        """
        with self.db.get_connection() as conn:
            return pd.read_sql(text(query), conn)

    def confirm_entries(self) -> bool:
        """仕訳確定
        
        Returns:
            確定処理の成功/失敗
        """
        # セット検証
        is_valid, message, errors = self.validate_sets()
        if not is_valid:
            print(f"エラー: {message}")
            print(errors)
            return False

        with self.db.get_connection() as conn:
            # 重複チェック
            conn.execute(text("DELETE FROM journal_entries WHERE entry_id IN (SELECT entry_id FROM temp_journal)"))

            # 移行
            conn.execute(text("""
                INSERT INTO journal_entries
                SELECT date, set_id, entry_id, subject_code, amount, remarks, subject, year, month, CURRENT_TIMESTAMP
                FROM temp_journal
            """))

            # 一時テーブルクリア
            conn.execute(text("DELETE FROM temp_journal"))

        # CSVファイル移動
        for file in TEMP_UPLOADS_DIR.glob('*.csv'):
            shutil.move(str(file), str(CONFIRMED_DIR / file.name))

        return True

    def get_cashflow_analysis(self) -> pd.DataFrame:
        """キャッシュフロー分析
        
        Returns:
            キャッシュフロー分析のDataFrame
        """
        query = """
        SELECT
            date,
            set_id,
            remarks,
            SUM(CASE WHEN subject_code IN (100, 101, 102) THEN amount ELSE 0 END) as cash_change
        FROM temp_journal
        GROUP BY date, set_id, remarks
        HAVING ABS(SUM(CASE WHEN subject_code IN (100, 101, 102) THEN amount ELSE 0 END)) > 0
        ORDER BY date
        """
        with self.db.get_connection() as conn:
            return pd.read_sql(text(query), conn)

    def get_monthly_balance_summary(self) -> pd.DataFrame:
        """月次残高集計の取得（financial_balance_sheet.csv形式）
        
        Returns:
            月次残高集計のDataFrame
        """
        query = """
        SELECT 
            CONCAT(year, '-', LPAD(month::text, 2, '0')) as YearMonth,
            SUM(CASE WHEN subject_code = 100 THEN amount ELSE 0 END) as "100",
            SUM(CASE WHEN subject_code = 101 THEN amount ELSE 0 END) as "101",
            SUM(CASE WHEN subject_code = 102 THEN amount ELSE 0 END) as "102",
            SUM(CASE WHEN subject_code = 109 THEN amount ELSE 0 END) as "109",
            SUM(CASE WHEN subject_code = 111 THEN amount ELSE 0 END) as "111",
            SUM(CASE WHEN subject_code = 130 THEN amount ELSE 0 END) as "130",
            SUM(CASE WHEN subject_code = 200 THEN amount ELSE 0 END) as "200",
            SUM(CASE WHEN subject_code = 220 THEN amount ELSE 0 END) as "220",
            SUM(CASE WHEN subject_code = 280 THEN amount ELSE 0 END) as "280",
            SUM(CASE WHEN subject_code = 290 THEN amount ELSE 0 END) as "290",
            SUM(CASE WHEN subject_code = 300 THEN amount ELSE 0 END) as "300",
            SUM(CASE WHEN subject_code = 400 THEN amount ELSE 0 END) as "400",
            SUM(CASE WHEN subject_code = 490 THEN amount ELSE 0 END) as "490",
            SUM(CASE WHEN subject_code = 500 THEN amount ELSE 0 END) as "500",
            SUM(CASE WHEN subject_code = 501 THEN amount ELSE 0 END) as "501",
            SUM(CASE WHEN subject_code = 511 THEN amount ELSE 0 END) as "511",
            SUM(CASE WHEN subject_code = 513 THEN amount ELSE 0 END) as "513",
            SUM(CASE WHEN subject_code = 521 THEN amount ELSE 0 END) as "521",
            SUM(CASE WHEN subject_code = 530 THEN amount ELSE 0 END) as "530",
            SUM(CASE WHEN subject_code = 531 THEN amount ELSE 0 END) as "531",
            SUM(CASE WHEN subject_code = 532 THEN amount ELSE 0 END) as "532",
            SUM(CASE WHEN subject_code = 541 THEN amount ELSE 0 END) as "541",
            SUM(CASE WHEN subject_code = 542 THEN amount ELSE 0 END) as "542",
            SUM(CASE WHEN subject_code = 550 THEN amount ELSE 0 END) as "550",
            SUM(CASE WHEN subject_code = 552 THEN amount ELSE 0 END) as "552",
            SUM(CASE WHEN subject_code = 561 THEN amount ELSE 0 END) as "561",
            SUM(CASE WHEN subject_code = 572 THEN amount ELSE 0 END) as "572",
            SUM(CASE WHEN subject_code = 580 THEN amount ELSE 0 END) as "580",
            SUM(CASE WHEN subject_code = 581 THEN amount ELSE 0 END) as "581",
            SUM(CASE WHEN subject_code = 590 THEN amount ELSE 0 END) as "590",
            SUM(CASE WHEN subject_code = 598 THEN amount ELSE 0 END) as "598",
            SUM(CASE WHEN subject_code = 599 THEN amount ELSE 0 END) as "599",
            SUM(CASE WHEN subject_code = 600 THEN amount ELSE 0 END) as "600",
            SUM(CASE WHEN subject_code BETWEEN 100 AND 199 THEN amount ELSE 0 END) as TotalAssets,
            SUM(CASE WHEN subject_code BETWEEN 200 AND 399 THEN amount ELSE 0 END) as TotalLiabilities,
            SUM(CASE WHEN subject_code BETWEEN 400 AND 499 THEN amount ELSE 0 END) as TotalIncome,
            SUM(CASE WHEN subject_code BETWEEN 500 AND 699 THEN amount ELSE 0 END) as TotalExpenses
        FROM journal_entries 
        GROUP BY year, month
        ORDER BY year, month
        """
        with self.db.get_connection() as conn:
            return pd.read_sql(text(query), conn)

    def generate_balance_sheet_format(self) -> pd.DataFrame:
        """financial_balance_sheet.csv形式の出力生成
        
        Returns:
            financial_balance_sheet.csv形式のDataFrame
        """
        monthly_data = self.get_monthly_balance_summary()
        
        # NetIncomeとTotalEquityの計算
        monthly_data['NetIncome'] = monthly_data['TotalIncome'] + monthly_data['TotalExpenses']
        monthly_data['TotalEquity'] = monthly_data['TotalAssets'] - monthly_data['TotalLiabilities']
        
        return monthly_data

    def process_bank_csv(self, file_path: str, bank: str, clear_temp: bool = True, check_duplicates: bool = True) -> int:
        """銀行CSV処理（機械学習分類付き）
        
        Args:
            file_path: 処理対象のCSVファイルパス
            bank: 銀行名 ('ufj')
            clear_temp: 処理前にtemp_journalテーブルをクリアするか
            check_duplicates: 重複ファイルのチェックを行うか
            
        Returns:
            処理した行数
        """
        # ファイル存在確認
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
        
        source_filename = Path(file_path).name
        
        # 重複チェック
        if check_duplicates:
            with self.db.get_connection() as conn:
                existing_count = pd.read_sql(
                    text("SELECT COUNT(*) as count FROM temp_journal WHERE source_file = :filename"),
                    conn,
                    params={'filename': source_filename}
                ).iloc[0]['count']
                
                if existing_count > 0:
                    print(f"警告: ファイル '{source_filename}' は既に処理済みです（{existing_count}行）")
                    if not clear_temp:
                        print("重複処理をスキップします。clear_temp=Trueで強制処理可能です。")
                        return 0
        
        # temp_journalクリア
        if clear_temp:
            with self.db.get_connection() as conn:
                result = conn.execute(text("DELETE FROM temp_journal"))
                print(f"temp_journalクリア: {result.rowcount}行削除")
        
        # CSV処理とDB保存
        if bank == 'ufj':
            processed_count = self._process_ufj_csv_to_db(file_path, source_filename)
        elif bank == 'jcb':
            processed_count = self._process_jcb_csv_to_db(file_path, source_filename)
        else:
            raise ValueError(f"サポートされていない銀行種別: {bank}")
        
        print(f"処理完了: {processed_count}行をデータベースに保存")
        return processed_count

    
    def _parse_date(self, date_str):
        """日付パース"""
        if pd.isna(date_str):
            return pd.NaT
        
        date_str = str(date_str).strip()
        
        # YYYYMMDD形式
        if len(date_str) == 8 and date_str.isdigit():
            return pd.to_datetime(date_str, format='%Y%m%d', errors='coerce')
        
        # YYYY-MM-DD形式
        try:
            return pd.to_datetime(date_str, errors='coerce')
        except:
            return pd.NaT
    
    def _process_ufj_csv_to_db(self, file_path: str, source_filename: str) -> int:
        """UFJ CSVを処理してデータベースに保存"""
        
        # CSV読み込み
        try:
            df = pd.read_csv(file_path, encoding='shift_jis')
            print(f"CSV読み込み完了（shift_jis）: {len(df)}行")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print(f"CSV読み込み完了（utf-8）: {len(df)}行")
            except Exception as e:
                print(f"CSV読み込みエラー: {e}")
                return 0
        
        if df.empty:
            print("CSVファイルが空です")
            return 0
        
        # UFJ CSV処理（機械学習分類）
        df_processed = self._process_ufj_with_ml(df)
        
        # 学習データ保存（学習用カラム + 予測結果のみ）
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        train_filename = f"ufj_processed_{timestamp}.csv"
        self.bank_predictor.save_training_data(df_processed, train_filename)
        
        # 複式簿記形式に変換してDB保存
        entries = self._convert_to_double_entry(df_processed)
        saved_count = self._save_entries_to_db(entries, source_filename)
        
        return saved_count
    
    def _process_ufj_with_ml(self, df: pd.DataFrame) -> pd.DataFrame:
        """UFJ CSVを機械学習で分類処理"""
        
        # 必要なカラムが存在するかチェック（日付カラム名の対応）
        date_cols = ['取引日', '日付']
        abstruct_cols = ['摘要']  
        memo_cols = ['摘要内容']
        out_cols = ['支払い金額']
        in_cols = ['預かり金額']
        
        # データ整理
        df_clean = df.copy()
        
        # 日付正規化（複数の日付カラム名に対応）
        date_col = None
        for col in date_cols:
            if col in df_clean.columns:
                date_col = col
                break
        
        if date_col:
            df_clean['date'] = pd.to_datetime(df_clean[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
        else:
            print("警告: 日付カラムが見つかりません")
            df_clean['date'] = '2024-01-01'  # デフォルト日付
        
        # テキスト結合（abstruct, memo）
        df_clean['abstruct'] = df_clean.get('摘要', '').fillna('').astype(str)
        df_clean['memo'] = df_clean.get('摘要内容', '').fillna('').astype(str)
        
        # テキスト正規化と結合（directionも特徴量として追加）
        text_columns = ['abstruct', 'memo']
        df_clean['combined_text'] = ''
        for col in text_columns:
            if col in df_clean.columns:
                normalized_text = df_clean[col].fillna('').apply(self.bank_predictor.normalize_text)
                df_clean['combined_text'] += normalized_text + ' '
        
        # 取引方向判定
        df_clean['direction'] = self._determine_direction(df_clean)
        
        # directionを特徴量として追加
        df_clean['combined_text'] += df_clean['direction'].fillna('') + ' '
        
        # 金額計算
        df_clean['amount'] = self._calculate_amount(df_clean)
        
        # 機械学習予測
        predictions = []
        for idx, row in df_clean.iterrows():
            text = row['combined_text'].strip()
            
            # subject_code予測
            subject_result = self.bank_predictor.predict_subject_code_ml(text)
            if subject_result and subject_result[2] > 0.5:
                debit, credit = subject_result[0], subject_result[1]
            else:
                # デフォルト分類
                if row['direction'] == 'out':
                    debit, credit = '598', '101'  # 雑費 → UFJ銀行
                elif row['direction'] == 'in':
                    debit, credit = '101', '490'  # UFJ銀行 → その他収入
                else:
                    debit, credit = '598', '101'
            
            # subject_codeを3桁に統一（floatの場合は整数変換）
            debit = str(int(float(debit))).zfill(3)
            credit = str(int(float(credit))).zfill(3)
            
            # remarks予測
            remarks_result = self.bank_predictor.predict_remarks_ml(text)
            if remarks_result and remarks_result[1] > 0.5:
                remarks = remarks_result[0]
            else:
                remarks = 'Auto classified'
            
            predictions.append({
                'suggested_debit': debit,
                'suggested_credit': credit,
                'remarks_classified': remarks
            })
        
        # 予測結果を追加
        for i, pred in enumerate(predictions):
            for key, value in pred.items():
                df_clean.loc[i, key] = value
        
        return df_clean
    
    def _determine_direction(self, df: pd.DataFrame) -> pd.Series:
        """取引方向判定"""
        directions = []
        for _, row in df.iterrows():
            out_amount = row.get('支払い金額', 0)
            in_amount = row.get('預かり金額', 0)
            
            # 金額から判定
            if pd.notna(out_amount) and str(out_amount).replace(',', '').strip():
                try:
                    if float(str(out_amount).replace(',', '')) > 0:
                        directions.append('out')
                        continue
                except:
                    pass
            
            if pd.notna(in_amount) and str(in_amount).replace(',', '').strip():
                try:
                    if float(str(in_amount).replace(',', '')) > 0:
                        directions.append('in')
                        continue
                except:
                    pass
            
            directions.append('unknown')
        
        return pd.Series(directions)
    
    def _calculate_amount(self, df: pd.DataFrame) -> pd.Series:
        """金額計算"""
        amounts = []
        for _, row in df.iterrows():
            # 入金チェック
            if '預かり金額' in row and pd.notna(row['預かり金額']) and str(row['預かり金額']).replace(',', '').strip():
                try:
                    amounts.append(float(str(row['預かり金額']).replace(',', '')))
                    continue
                except ValueError:
                    pass
            
            # 出金チェック
            if '支払い金額' in row and pd.notna(row['支払い金額']) and str(row['支払い金額']).replace(',', '').strip():
                try:
                    amounts.append(float(str(row['支払い金額']).replace(',', '')))
                    continue
                except ValueError:
                    pass
            
            amounts.append(0.0)
        
        return pd.Series(amounts)
    
    def _convert_to_double_entry(self, df: pd.DataFrame) -> pd.DataFrame:
        """複式簿記形式に変換"""
        entries = []
        
        for idx, row in df.iterrows():
            amount = row.get('amount', 0)
            if amount == 0:
                continue
                
            date = row.get('date', '')
            remarks = row.get('remarks_classified', '')
            debit_code = row.get('suggested_debit', '')
            credit_code = row.get('suggested_credit', '')
            
            import random
            set_id = random.randint(1000, 9999)  # 整数のSetID
            amount_int = int(round(float(amount)))  # 整数のAmount
            
            # 借方エントリ
            entries.append({
                'Date': date,
                'SubjectCode': debit_code,
                'Amount': amount_int,
                'Remarks': remarks,
                'SetID': set_id
            })
            
            # 貸方エントリ
            entries.append({
                'Date': date,
                'SubjectCode': credit_code,
                'Amount': -amount_int,
                'Remarks': remarks,
                'SetID': set_id
            })
        
        return pd.DataFrame(entries)
    
    def _save_entries_to_db(self, df: pd.DataFrame, source_filename: str) -> int:
        """エントリをデータベースに保存"""
        if df.empty:
            return 0
        
        # データベース形式に変換
        df_db = df.copy()
        df_db['date'] = pd.to_datetime(df_db['Date']).dt.date
        df_db['set_id'] = df_db['SetID'].astype(int)  # 整数のSetID
        # subject_codeを整数に変換（文字列の場合も対応）
        df_db['subject_code'] = df_db['SubjectCode'].apply(lambda x: int(float(str(x))))
        df_db['amount'] = df_db['Amount'].astype(int)  # 整数のAmount
        df_db['remarks'] = df_db['Remarks']
        df_db['source_file'] = source_filename
        
        # 科目名マッピング
        df_db['subject'] = df_db['subject_code'].map(SUBJECT_CODES).fillna('Unknown')
        
        # 年月追加
        df_db['year'] = pd.to_datetime(df_db['Date']).dt.year
        df_db['month'] = pd.to_datetime(df_db['Date']).dt.month
        
        # エントリID生成
        df_db['entry_id'] = range(1, len(df_db) + 1)
        
        # データベース保存
        with self.db.get_connection() as conn:
            df_db[['date', 'set_id', 'entry_id', 'subject_code', 'amount', 'remarks', 'subject', 'year', 'month', 'source_file']].to_sql(
                'temp_journal', 
                conn, 
                if_exists='append', 
                index=False
            )
        
        return len(df_db)
    
    def _process_jcb_csv_to_db(self, file_path: str, source_filename: str) -> int:
        """JCB CSVを処理してデータベースに保存"""
        
        # CSV読み込み
        try:
            df = pd.read_csv(file_path, encoding='shift_jis')
            print(f"CSV読み込み完了（shift_jis）: {len(df)}行")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print(f"CSV読み込み完了（utf-8）: {len(df)}行")
            except Exception as e:
                print(f"CSV読み込みエラー: {e}")
                return 0
        
        if df.empty:
            print("CSVファイルが空です")
            return 0
        
        # JCB CSV処理（機械学習分類）
        df_processed = self._process_jcb_with_ml(df)
        
        # 学習データ保存（学習用カラム + 予測結果のみ）
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        train_filename = f"jcb_processed_{timestamp}.csv"
        self.bank_predictor.save_training_data(df_processed, train_filename, bank='jcb')
        
        # 複式簿記形式に変換してDB保存
        entries = self._convert_to_double_entry_jcb(df_processed)
        saved_count = self._save_entries_to_db(entries, source_filename)
        
        return saved_count
    
    def _process_jcb_with_ml(self, df: pd.DataFrame) -> pd.DataFrame:
        """JCB CSVを機械学習で分類処理"""
        
        # 必要なカラムが存在するかチェック（JCBカラム名の対応）
        date_cols = ['ご利用日', '日付']
        abstruct_cols = ['ご利用先など', '利用先']  
        memo_cols = ['備考']
        out_cols = ['ご利用金額（円）', '利用金額']
        
        # データ整理
        df_clean = df.copy()
        
        # 日付正規化（複数の日付カラム名に対応）
        date_col = None
        for col in date_cols:
            if col in df_clean.columns:
                date_col = col
                break
        
        if date_col:
            df_clean['date'] = pd.to_datetime(df_clean[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
        else:
            print("警告: 日付カラムが見つかりません")
            df_clean['date'] = '2024-01-01'  # デフォルト日付
        
        # テキスト結合（abstruct, memo）
        df_clean['abstruct'] = df_clean.get('ご利用先など', df_clean.get('利用先', '')).fillna('').astype(str)
        df_clean['memo'] = df_clean.get('備考', '').fillna('').astype(str)
        
        # テキスト正規化と結合（directionも特徴量として追加）
        text_columns = ['abstruct', 'memo']
        df_clean['combined_text'] = ''
        for col in text_columns:
            if col in df_clean.columns:
                normalized_text = df_clean[col].fillna('').apply(self.bank_predictor.normalize_text)
                df_clean['combined_text'] += normalized_text + ' '
        
        # 取引方向判定（JCBは出金のみ）
        df_clean['direction'] = 'out'
        
        # directionを特徴量として追加
        df_clean['combined_text'] += df_clean['direction'].fillna('') + ' '
        
        # 金額計算
        df_clean['amount'] = self._calculate_jcb_amount(df_clean)
        
        # 機械学習予測
        predictions = []
        for idx, row in df_clean.iterrows():
            text = row['combined_text'].strip()
            
            # subject_code予測（JCB専用モデル使用）
            subject_result = self.bank_predictor.predict_subject_code_ml(text, bank='jcb')
            if subject_result and subject_result[2] > 0.5:
                debit, credit = subject_result[0], subject_result[1]
            else:
                # デフォルト分類（JCBは貸方固定）
                debit, credit = '598', '201'  # 雑費 → JCBカード
            
            # subject_codeを3桁に統一
            debit = str(int(float(debit))).zfill(3)
            credit = str(int(float(credit))).zfill(3)
            
            # remarks予測（JCB専用モデル使用）
            remarks_result = self.bank_predictor.predict_remarks_ml(text, bank='jcb')
            if remarks_result and remarks_result[1] > 0.5:
                remarks = remarks_result[0]
            else:
                remarks = 'JCB Auto classified'
            
            predictions.append({
                'suggested_debit': debit,
                'suggested_credit': credit,
                'remarks_classified': remarks
            })
        
        # 予測結果を追加
        for i, pred in enumerate(predictions):
            for key, value in pred.items():
                df_clean.loc[i, key] = value
        
        return df_clean
    
    def _calculate_jcb_amount(self, df: pd.DataFrame) -> pd.Series:
        """JCB金額計算"""
        amounts = []
        for _, row in df.iterrows():
            # JCBの利用金額チェック
            amount_cols = ['ご利用金額（円）', '利用金額']
            amount = 0
            
            for col in amount_cols:
                if col in row and pd.notna(row[col]) and str(row[col]).replace(',', '').strip():
                    try:
                        amount = float(str(row[col]).replace(',', ''))
                        break
                    except ValueError:
                        pass
            
            amounts.append(amount)
        
        return pd.Series(amounts)
    
    def _convert_to_double_entry_jcb(self, df: pd.DataFrame) -> pd.DataFrame:
        """JCB複式簿記形式に変換"""
        entries = []
        
        for idx, row in df.iterrows():
            amount = row.get('amount', 0)
            if amount == 0:
                continue
                
            date = row.get('date', '')
            remarks = row.get('remarks_classified', '')
            debit_code = row.get('suggested_debit', '')
            credit_code = row.get('suggested_credit', '201')  # JCBカードに固定
            
            import random
            set_id = random.randint(1000, 9999)  # 整数のSetID
            amount_int = int(round(float(amount)))  # 整数のAmount
            
            # 借方エントリ（費用）
            entries.append({
                'Date': date,
                'SubjectCode': debit_code,
                'Amount': amount_int,
                'Remarks': remarks,
                'SetID': set_id
            })
            
            # 貸方エントリ（JCBカード）
            entries.append({
                'Date': date,
                'SubjectCode': credit_code,
                'Amount': -amount_int,
                'Remarks': remarks,
                'SetID': set_id
            })
        
        return pd.DataFrame(entries)

    def close_monthly_balance(self, year_month: str, reclose: bool = False):
        """
        月次締切処理を実行し、損益科目を純資産に振り替える。
        Args:
            year_month: 'YYYY-MM' 形式の年月
            reclose: 既に締切済みの月も再度締切するか
        """
        RETAINED_EARNINGS_CODE = '300'  # 繰越利益
        
        # 年月をパース
        try:
            year, month = map(int, year_month.split('-'))
        except ValueError:
            print(f"無効な年月形式です: {year_month}。YYYY-MM形式で入力してください。")
            return
        
        # 月末日を計算
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime.date(year, month, last_day)
        
        with self.db.get_connection() as conn:
            # 0. 締切処理の重複チェック（recloseフラグがFalseの場合のみ）
            if not reclose:
                check_query = text("""
                    SELECT COUNT(*) as count
                    FROM journal_entries
                    WHERE remarks = 'close' AND year = :year AND month = :month
                """)
                existing_close = conn.execute(check_query, {'year': year, 'month': month}).fetchone()
                
                if existing_close.count > 0:
                    print(f"{year}年{month}月は既に締切処理済みです。再締切する場合は--recloseフラグを使用してください。")
                    return
            else:
                # 再締切の場合、既存の締切仕訳を削除
                print(f"{year}年{month}月の既存の締切仕訳を削除します...")
                delete_query = text("""
                    DELETE FROM journal_entries
                    WHERE remarks = 'close' AND year = :year AND month = :month
                """)
                deleted_count = conn.execute(delete_query, {'year': year, 'month': month}).rowcount
                if deleted_count > 0:
                    print(f"既存の締切仕訳 {deleted_count}件を削除しました。")
            
            # 1. 指定月の損益科目の残高を集計
            query = text("""
                SELECT subject_code, SUM(amount) as balance
                FROM journal_entries
                WHERE (subject_code >= 400 AND subject_code < 600)
                  AND year = :year AND month = :month
                  AND remarks != 'close'
                GROUP BY subject_code
                HAVING SUM(amount) != 0
            """)
            pl_balances = conn.execute(query, {'year': year, 'month': month}).fetchall()

            if not pl_balances:
                print(f"{year}年{month}月の締切対象の損益取引がありません。")
                return

            net_income = sum(row.balance for row in pl_balances)
            
            # 2. 締切仕訳の作成
            set_id = f'{randint(901, 999)}'

            trans = conn.begin()
            try:
                # 2a. 各損益科目の残高をゼロにする仕訳
                for row in pl_balances:
                    conn.execute(text("""
                        INSERT INTO journal_entries (date, set_id, entry_id, subject_code, amount, remarks, subject, year, month)
                        VALUES (:date, :set_id, :entry_id, :code, :amount, :remarks, :subject, :year, :month)
                    """), {
                        'date': end_date, 'set_id': set_id, 'entry_id': f'{set_id}_{row.subject_code}',
                        'code': row.subject_code, 'amount': -row.balance, 'remarks': 'close', 
                        'subject': SUBJECT_CODES.get(int(row.subject_code), 'Unknown'),
                        'year': year, 'month': month
                    })

                # 2b. 純損益を繰越利益に振り替える仕訳
                if net_income != 0:
                    conn.execute(text("""
                        INSERT INTO journal_entries (date, set_id, entry_id, subject_code, amount, remarks, subject, year, month)
                        VALUES (:date, :set_id, :entry_id, :code, :amount, :remarks, :subject, :year, :month)
                    """), {
                        'date': end_date, 'set_id': set_id, 'entry_id': f'{set_id}_{RETAINED_EARNINGS_CODE}',
                        'code': RETAINED_EARNINGS_CODE, 'amount': net_income, 'remarks': 'loss and benefit',
                        'subject': SUBJECT_CODES.get(int(RETAINED_EARNINGS_CODE), 'Unknown'),
                        'year': year, 'month': month
                    })
                
                trans.commit()
                if reclose:
                    print(f"{year}年{month}月の再締切処理が完了しました。純損益: {net_income:,.0f}円")
                else:
                    print(f"{year}年{month}月の締切処理が完了しました。純損益: {net_income:,.0f}円")

            except Exception as e:
                trans.rollback()
                print(f"エラーが発生したため、締切処理をロールバックしました: {e}")
