#!/usr/bin/env python3
"""
HouseAccount - データベース確認用ヘルパースクリプト
confirmコマンド実行前後のデータ状況確認用
"""

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from .database import db_manager
from .config import BALANCE_TOLERANCE

class QueryHelper:
    """データベースクエリヘルパー"""
    
    def __init__(self):
        self.db = db_manager
    
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
            confirmed_at::date as confirmed_date
        FROM journal_entries 
        WHERE confirmed_at >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY date, set_id, confirmed_at::date
        ORDER BY confirmed_at DESC, date, set_id
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
    
    def show_financial_status(self):
        """家計状況の確認"""
        print("\n=== 家計状況 ===")
        
        # 現在の残高状況
        balance_query = """
        SELECT 
            CASE 
                WHEN subject_code BETWEEN 100 AND 199 THEN '資産'
                WHEN subject_code BETWEEN 200 AND 299 THEN '負債'
                WHEN subject_code BETWEEN 300 AND 399 THEN '純資産'
                WHEN subject_code BETWEEN 400 AND 499 THEN '収益'
                WHEN subject_code BETWEEN 500 AND 599 THEN '費用'
                ELSE 'その他'
            END as category,
            subject_code,
            subject,
            SUM(amount) as balance
        FROM journal_entries
        GROUP BY subject_code, subject
        HAVING SUM(amount) != 0
        ORDER BY subject_code
        """
        
        with self.db.get_connection() as conn:
            balances = pd.read_sql(text(balance_query), conn)
            
            if len(balances) == 0:
                print("仕訳データがありません")
                return
            
            # カテゴリ別集計
            category_summary = balances.groupby('category')['balance'].sum()
            
            print("【カテゴリ別残高】")
            for category, total in category_summary.items():
                print(f"  {category}: {total:,.0f}円")
            
            # 純資産計算
            assets = category_summary.get('資産', 0)
            liabilities = category_summary.get('負債', 0)
            equity_calculated = assets - liabilities
            equity_recorded = category_summary.get('純資産', 0)
            
            print(f"\n【純資産】")
            print(f"  計算値（資産-負債）: {equity_calculated:,.0f}円")
            print(f"  記録値（純資産科目）: {equity_recorded:,.0f}円")
            
            # 損益計算
            income = category_summary.get('収益', 0)
            expenses = category_summary.get('費用', 0)
            net_income = income + expenses  # 費用は負の値なので
            
            print(f"\n【損益】")
            print(f"  収益: {income:,.0f}円")
            print(f"  費用: {expenses:,.0f}円")
            print(f"  純損益: {net_income:,.0f}円")
            
            # 詳細表示
            print(f"\n【科目別残高詳細】")
            for category in ['資産', '負債', '純資産', '収益', '費用']:
                category_data = balances[balances['category'] == category]
                if len(category_data) > 0:
                    print(f"\n■ {category}")
                    for _, row in category_data.iterrows():
                        print(f"  {row['subject_code']:03d} {row['subject']}: {row['balance']:,.0f}円")
    
    def show_monthly_trend(self, months=6):
        """月次推移表示"""
        print(f"\n=== 月次推移（過去{months}ヶ月） ===")
        
        trend_query = """
        SELECT 
            year,
            month,
            SUM(CASE WHEN subject_code BETWEEN 100 AND 199 THEN amount ELSE 0 END) as assets,
            SUM(CASE WHEN subject_code BETWEEN 200 AND 299 THEN amount ELSE 0 END) as liabilities,
            SUM(CASE WHEN subject_code BETWEEN 300 AND 399 THEN amount ELSE 0 END) as equity,
            SUM(CASE WHEN subject_code BETWEEN 400 AND 499 THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN subject_code BETWEEN 500 AND 599 THEN amount ELSE 0 END) as expenses
        FROM journal_entries
        WHERE (year * 100 + month) >= (EXTRACT(YEAR FROM CURRENT_DATE) * 100 + EXTRACT(MONTH FROM CURRENT_DATE) - %s)
        GROUP BY year, month
        ORDER BY year, month
        """ % months
        
        with self.db.get_connection() as conn:
            trend = pd.read_sql(text(trend_query), conn)
            
            if len(trend) == 0:
                print("月次データがありません")
                return
            
            print("年月    |   資産   |   負債   |  純資産  |   収益   |   費用   |  純損益")
            print("--------|----------|----------|----------|----------|----------|----------")
            
            for _, row in trend.iterrows():
                net_income = row['income'] + row['expenses']
                print(f"{row['year']}-{row['month']:02d} | {row['assets']:8,.0f} | {row['liabilities']:8,.0f} | {row['equity']:8,.0f} | {row['income']:8,.0f} | {row['expenses']:8,.0f} | {net_income:8,.0f}")
    
    def show_closing_status(self):
        """締切状況確認"""
        print("\n=== 締切状況 ===")
        
        closing_query = """
        SELECT 
            year,
            month,
            COUNT(*) as close_entries,
            SUM(amount) as total_amount
        FROM journal_entries
        WHERE remarks = 'close'
        GROUP BY year, month
        ORDER BY year, month
        """
        
        with self.db.get_connection() as conn:
            closings = pd.read_sql(text(closing_query), conn)
            
            if len(closings) == 0:
                print("締切処理の履歴がありません")
                return
            
            print("締切済み月:")
            for _, row in closings.iterrows():
                print(f"  {row['year']}年{row['month']}月: {row['close_entries']}件の締切仕訳")
            
            # 未締切月の確認
            recent_query = """
            SELECT DISTINCT year, month
            FROM journal_entries
            WHERE remarks != 'close'
            AND (subject_code BETWEEN 400 AND 599)
            AND (year * 100 + month) NOT IN (
                SELECT DISTINCT year * 100 + month
                FROM journal_entries
                WHERE remarks = 'close'
            )
            ORDER BY year, month
            """
            
            unclosed = pd.read_sql(text(recent_query), conn)
            
            if len(unclosed) > 0:
                print("\n未締切月（損益取引あり）:")
                for _, row in unclosed.iterrows():
                    print(f"  {row['year']}年{row['month']}月")
            else:
                print("\n未締切月はありません")


def main():
    """メイン実行関数"""
    if len(sys.argv) < 2:
        print("使用方法: python -m ledger_ingest.query_helper [command]")
        print("\nコマンド:")
        print("  summary     - テーブル概要表示")
        print("  duplicates  - 重複チェック")
        print("  balance     - 平衡チェック（temp_journal + journal_entries）")
        print("  recent      - 最近の確定処理結果")
        print("  files       - 処理済みファイル一覧")
        print("  preview     - confirm実行SQLのプレビュー")
        print("  status      - 家計状況確認")
        print("  trend       - 月次推移表示")
        print("  closing     - 締切状況確認")
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
    elif command == 'status':
        helper.show_financial_status()
    elif command == 'trend':
        months = int(sys.argv[2]) if len(sys.argv) > 2 else 6
        helper.show_monthly_trend(months)
    elif command == 'closing':
        helper.show_closing_status()
    elif command == 'all':
        helper.show_table_summary()
        helper.check_duplicates()
        helper.check_balance_temp()
        helper.check_balance_confirmed()
        helper.show_recent_confirmations()
        helper.show_source_files()
        helper.show_sql_preview()
        helper.show_financial_status()
        helper.show_monthly_trend()
        helper.show_closing_status()
    else:
        print(f"無効なコマンド: {command}")
        print("使用可能なコマンド: summary, duplicates, balance, recent, files, preview, status, trend, closing, all")


if __name__ == '__main__':
    main()