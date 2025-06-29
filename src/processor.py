import pandas as pd
import shutil
from pathlib import Path
from sqlalchemy import text
from typing import Optional, Tuple
from .models import DatabaseManager
from .config import SUBJECT_CODES, TEMP_UPLOADS_DIR, CONFIRMED_DIR, BALANCE_TOLERANCE

class CSVProcessor:
    """
    統合CSV処理クラス - ファイル処理とデータベース操作を一元化
    """
    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        # データベース処理用
        self.db: DatabaseManager = db_manager or DatabaseManager()


    def process_csv_for_database(self, file_path: str) -> int:
        """CSV処理（データベース保存用 - 旧JournalProcessor機能）
        
        Args:
            file_path: 処理対象のCSVファイルパス
            
        Returns:
            処理した行数
        """
        df = pd.read_csv(file_path)

        # データ型変換
        df['Date'] = pd.to_datetime(df['Date'])
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
            df['SetID'] = df['Date'].dt.strftime('%Y%m%d') + '_' + df['SetID'].astype(str).str.zfill(2)
        else:
            # SetIDが存在しない場合は従来通り生成
            df['SetID'] = df['Date'].dt.strftime('%Y%m%d') + '_' + df['Remarks'].str.extract(r'(\d+)$')[0].fillna('00')
        
        # EntryIDの生成（SetID + 連番）
        df['EntryID'] = df.groupby('SetID').cumcount().astype(str).str.zfill(2)
        df['EntryID'] = df['SetID'].astype(str) + '_' + df['EntryID']

        # 科目名の自動変換
        df['Subject'] = df['SubjectCode'].map(SUBJECT_CODES)
        df['source_file'] = Path(file_path).name

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

    def process_month_end_complete_workflow(self, file_path: str) -> pd.DataFrame:
        """月末処理の完全ワークフロー実行
        
        Args:
            file_path: 処理対象のCSVファイルパス
            
        Returns:
            最終的なbalance sheet形式のDataFrame
        """
        # 1. CSV処理
        processed_count = self.process_csv_for_database(file_path)
        print(f"処理行数: {processed_count}")
        
        # 2. セット検証
        is_valid, message, errors = self.validate_sets()
        if not is_valid:
            raise ValueError(f"セット検証エラー: {message}")
        print(f"検証結果: {message}")
        
        # 3. 仕訳確定
        if not self.confirm_entries():
            raise ValueError("仕訳確定に失敗しました")
        print("仕訳確定完了")
        
        # 4. 月次バランスシート生成
        balance_sheet = self.generate_balance_sheet_format()
        print(f"バランスシート生成完了: {len(balance_sheet)}ヶ月分")
        
        return balance_sheet
