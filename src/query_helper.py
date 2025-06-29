#!/usr/bin/env python3
"""
HouseAccount - データベース確認用ヘルパースクリプト
confirmコマンド実行前後のデータ状況確認用
"""

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from .models import DatabaseManager
from .config import BALANCE_TOLERANCE

class QueryHelper:
    """データベースクエリヘルパー"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def show_table_summary(self):
        """テーブル概要表示"""
        print("=== テーブル概要 ===")
        
        query = """
        SELECT 
            'temp_journal' as table_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT set_id) as unique_sets,
            COUNT(DISTINCT source_file) as source_files,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM temp_journal
        WHERE EXISTS (SELECT 1 FROM temp_journal)

        UNION ALL

        SELECT 
            'journal_entries' as table_name,
            COUNT(*) as record_count,
            COUNT(DISTINCT set_id) as unique_sets,
            COUNT(DISTINCT 'confirmed') as source_files,
            MIN(date) as earliest_date,
            MAX(date) as latest_date  
        FROM journal_entries
        WHERE EXISTS (SELECT 1 FROM journal_entries)
        """
        
        with self.db.get_connection() as conn:
            result = pd.read_sql(text(query), conn)
            print(result.to_string(index=False))
    
    def check_duplicates(self):
        """重複チェック"""
        print("\n=== 重複チェック ===")
        
        query = """
        SELECT 
            t.entry_id,
            t.set_id,
            t.date,
            t.remarks,
            t.amount,
            CASE WHEN j.entry_id IS NOT NULL THEN '重複' ELSE '新規' END as status
        FROM temp_journal t
        LEFT JOIN journal_entries j ON t.entry_id = j.entry_id
        ORDER BY t.date, t.set_id, t.entry_id
        """
        
        with self.db.get_connection() as conn:
            result = pd.read_sql(text(query), conn)
            
            if len(result) == 0:
                print("temp_journalにデータがありません")
                return
            
            duplicates = result[result['status'] == '重複']
            new_entries = result[result['status'] == '新規']
            
            print(f"新規エントリ: {len(new_entries)}件")
            print(f"重複エントリ: {len(duplicates)}件")
            
            if len(duplicates) > 0:
                print("\n重複エントリ一覧:")
                print(duplicates[['entry_id', 'set_id', 'date', 'remarks', 'amount']].to_string(index=False))
    
    def check_balance_temp(self):
        """temp_journalの平衡チェック"""
        print("\n=== temp_journal 平衡チェック ===")
        
        query = """
        SELECT 
            set_id,
            date,
            COUNT(*) as entry_count,
            SUM(amount) as balance,
            string_agg(DISTINCT remarks, ', ') as remarks_list,
            CASE 
                WHEN ABS(SUM(amount)) <= :tolerance THEN '平衡'
                ELSE '不平衡'
            END as status
        FROM temp_journal
        WHERE remarks NOT ILIKE '%carry over%'
        GROUP BY set_id, date
        ORDER BY date, set_id
        """
        
        with self.db.get_connection() as conn:
            result = pd.read_sql(text(query), conn, params={'tolerance': BALANCE_TOLERANCE})
            
            if len(result) == 0:
                print("temp_journalにデータがありません（またはすべてCarry over）")
                return
            
            balanced = result[result['status'] == '平衡']
            unbalanced = result[result['status'] == '不平衡']
            
            print(f"平衡セット: {len(balanced)}件")
            print(f"不平衡セット: {len(unbalanced)}件")
            
            if len(unbalanced) > 0:
                print("\n不平衡セット一覧:")
                print(unbalanced[['set_id', 'date', 'balance', 'remarks_list']].to_string(index=False))
    
    def check_balance_confirmed(self):
        """journal_entriesの平衡チェック"""
        print("\n=== journal_entries 平衡チェック ===")
        
        query = """
        SELECT 
            set_id,
            date,
            COUNT(*) as entry_count,
            SUM(amount) as balance,
            string_agg(DISTINCT remarks, ', ') as remarks_list,
            CASE 
                WHEN ABS(SUM(amount)) <= :tolerance THEN '平衡'
                ELSE '不平衡'
            END as status
        FROM journal_entries
        WHERE remarks NOT ILIKE '%carry over%'
        GROUP BY set_id, date
        HAVING ABS(SUM(amount)) > :tolerance
        ORDER BY date, set_id
        """
        
        with self.db.get_connection() as conn:
            result = pd.read_sql(text(query), conn, params={'tolerance': BALANCE_TOLERANCE})
            
            if len(result) == 0:
                print("不平衡な確定済みセットはありません")
            else:
                print(f"不平衡な確定済みセット: {len(result)}件")
                print(result[['set_id', 'date', 'balance', 'remarks_list']].to_string(index=False))
    
    def show_recent_confirmations(self, days=7):
        """最近の確定処理結果"""
        print(f"\n=== 最近{days}日間の確定処理 ===")
        
        query = """
        SELECT 
            date,
            set_id,
            COUNT(*) as entry_count,
            SUM(amount) as balance,
            string_agg(subject_code::text || ':' || amount::text, ', ') as entries,
            created_at::date as confirmed_date
        FROM journal_entries 
        WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY date, set_id, created_at::date
        ORDER BY created_at DESC, date, set_id
        """ % days
        
        with self.db.get_connection() as conn:
            result = pd.read_sql(text(query), conn)
            
            if len(result) == 0:
                print(f"過去{days}日間に確定された仕訳はありません")
            else:
                print(f"確定済み仕訳: {len(result)}セット")
                print(result.to_string(index=False))
    
    def show_source_files(self):
        """処理済みファイル一覧"""
        print("\n=== 処理済みファイル ===")
        
        # temp_journal
        query_temp = """
        SELECT 
            source_file,
            COUNT(*) as entry_count,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM temp_journal
        GROUP BY source_file
        ORDER BY source_file
        """
        
        with self.db.get_connection() as conn:
            temp_files = pd.read_sql(text(query_temp), conn)
            
            if len(temp_files) > 0:
                print("temp_journal内のファイル:")
                print(temp_files.to_string(index=False))
            else:
                print("temp_journalにファイルがありません")
    
    def show_sql_preview(self):
        """confirmで実行されるSQLのプレビュー"""
        print("\n=== confirm実行時のSQL（プレビュー） ===")
        
        print("1. 重複削除SQL:")
        print("DELETE FROM journal_entries WHERE entry_id IN (SELECT entry_id FROM temp_journal);")
        
        print("\n2. データ移行SQL:")
        print("""INSERT INTO journal_entries
SELECT date, set_id, entry_id, subject_code, amount, remarks, subject, year, month, CURRENT_TIMESTAMP
FROM temp_journal;""")
        
        print("\n3. 一時テーブルクリアSQL:")
        print("DELETE FROM temp_journal;")
        
        # 影響行数の予測
        with self.db.get_connection() as conn:
            # 重複削除対象
            duplicate_query = """
            SELECT COUNT(*) as count 
            FROM journal_entries 
            WHERE entry_id IN (SELECT entry_id FROM temp_journal)
            """
            duplicates = pd.read_sql(text(duplicate_query), conn).iloc[0]['count']
            
            # 移行対象
            temp_count_query = "SELECT COUNT(*) as count FROM temp_journal"
            temp_count = pd.read_sql(text(temp_count_query), conn).iloc[0]['count']
            
            print(f"\n予想される影響:")
            print(f"  削除される既存レコード数: {duplicates}")
            print(f"  追加される新規レコード数: {temp_count}")


def main():
    """メイン実行関数"""
    if len(sys.argv) < 2:
        print("使用方法: python -m src.query_helper [command]")
        print("\nコマンド:")
        print("  summary     - テーブル概要表示")
        print("  duplicates  - 重複チェック")
        print("  balance     - 平衡チェック（temp_journal + journal_entries）")
        print("  recent      - 最近の確定処理結果")
        print("  files       - 処理済みファイル一覧")
        print("  preview     - confirm実行SQLのプレビュー")
        print("  all         - すべての情報を表示")
        return
    
    command = sys.argv[1]
    helper = QueryHelper()
    
    if command == 'summary':
        helper.show_table_summary()
    elif command == 'duplicates':
        helper.check_duplicates()
    elif command == 'balance':
        helper.check_balance_temp()
        helper.check_balance_confirmed()
    elif command == 'recent':
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        helper.show_recent_confirmations(days)
    elif command == 'files':
        helper.show_source_files()
    elif command == 'preview':
        helper.show_sql_preview()
    elif command == 'all':
        helper.show_table_summary()
        helper.check_duplicates()
        helper.check_balance_temp()
        helper.check_balance_confirmed()
        helper.show_recent_confirmations()
        helper.show_source_files()
        helper.show_sql_preview()
    else:
        print(f"無効なコマンド: {command}")
        print("使用可能なコマンド: summary, duplicates, balance, recent, files, preview, all")


if __name__ == '__main__':
    main()