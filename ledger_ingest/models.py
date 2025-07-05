from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.mock import MockConnection
from .config import DATABASE_URL
from typing import Union
from sqlalchemy.engine import Engine

class DatabaseManager:
    def __init__(self):
        self.engine: Engine = create_engine(DATABASE_URL)

    def init_tables(self):
        """テーブル初期化"""
        with self.engine.connect() as conn:
            # 一時仕訳テーブル
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS temp_journal (
                    date DATE,
                    set_id VARCHAR(20),
                    entry_id VARCHAR(20),
                    subject_code INTEGER,
                    amount DECIMAL(12,2),
                    remarks TEXT,
                    subject VARCHAR(50),
                    year INTEGER,
                    month INTEGER,
                    source_file VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # 確定仕訳テーブル
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS journal_entries (
                    date DATE,
                    set_id VARCHAR(20),
                    entry_id VARCHAR(20) PRIMARY KEY,
                    subject_code INTEGER,
                    amount DECIMAL(12,2),
                    remarks TEXT,
                    subject VARCHAR(50),
                    year INTEGER,
                    month INTEGER,
                    confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # 残高集計ビュー
            conn.execute(text("""
                CREATE OR REPLACE VIEW account_balances AS
                SELECT
                    subject_code,
                    subject,
                    SUM(amount) as balance,
                    year,
                    month
                FROM journal_entries
                GROUP BY subject_code, subject, year, month
                ORDER BY year, month, subject_code
            """))

            # 取引集計ビュー（セット単位）
            conn.execute(text("""
                CREATE OR REPLACE VIEW transaction_sets AS
                SELECT
                    date,
                    set_id,
                    remarks,
                    COUNT(*) as entry_count,
                    SUM(amount) as balance_check,
                    string_agg(subject || ':' || amount::text, ', ' ORDER BY amount DESC) as entries
                FROM journal_entries
                GROUP BY date, set_id, remarks
                ORDER BY date, set_id
            """))

            # 試算表ビュー
            conn.execute(text("""
                CREATE OR REPLACE VIEW trial_balance AS
                SELECT
                    subject_code,
                    subject,
                    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as debit_total,
                    SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) as credit_total,
                    SUM(amount) as balance
                FROM journal_entries
                GROUP BY subject_code, subject
                ORDER BY subject_code
            """))

            print("データベーステーブルとビューを作成しました")

    def get_connection(self) -> Connection:
        """データベース接続を取得"""
        return self.engine.connect()
