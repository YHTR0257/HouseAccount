#!/usr/bin/env python3
"""
confirmコマンド実行前後の確認用スクリプト
簡単に実行できるように独立したスクリプトとして作成
"""

import sys
import os
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.query_helper import QueryHelper

def main():
    """メイン関数"""
    print("HouseAccount - confirm確認スクリプト")
    print("=" * 50)
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        helper = QueryHelper()
        
        if command == 'before':
            print("🔍 confirm実行前の確認")
            helper.show_table_summary()
            helper.check_duplicates()
            helper.check_balance_temp()
            helper.show_sql_preview()
        elif command == 'after':
            print("✅ confirm実行後の確認")
            helper.show_table_summary()
            helper.check_balance_confirmed()
            helper.show_recent_confirmations(1)
        elif command == 'status':
            print("📊 現在の状況")
            helper.show_table_summary()
            helper.show_source_files()
        else:
            # そのまま query_helper に渡す
            os.system(f"python -m src.query_helper {' '.join(sys.argv[1:])}")
    else:
        print("使用方法:")
        print("  python scripts/check_confirm.py before   # confirm前の確認")
        print("  python scripts/check_confirm.py after    # confirm後の確認") 
        print("  python scripts/check_confirm.py status   # 現在の状況")
        print("  python scripts/check_confirm.py all      # 全情報表示")
        print("\nまたは src.query_helper の任意のコマンドを使用可能:")
        print("  summary, duplicates, balance, recent, files, preview")

if __name__ == '__main__':
    main()