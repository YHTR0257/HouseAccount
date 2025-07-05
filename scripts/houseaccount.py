#!/usr/bin/env python3
"""
HouseAccount - 統合コマンドラインツール
makeコマンドが使えない環境用のPython版ショートカット
"""

import sys
import os
import subprocess
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_command(cmd: str) -> None:
    """コマンドを実行"""
    print(f"実行中: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=project_root)
    if result.returncode != 0:
        print(f"エラー: コマンドが失敗しました (終了コード: {result.returncode})")
        sys.exit(result.returncode)

def show_help() -> None:
    """ヘルプ表示"""
    print("HouseAccount - 利用可能なコマンド:")
    print("")
    print("基本操作:")
    print("  python scripts/houseaccount.py init                    - データベース初期化")
    print("  python scripts/houseaccount.py process path.csv        - CSVファイル処理")
    print("  python scripts/houseaccount.py confirm                 - 仕訳確定")
    print("  python scripts/houseaccount.py trial                   - 試算表表示")
    print("  python scripts/houseaccount.py cashflow                - キャッシュフロー分析")
    print("  python scripts/houseaccount.py summary                 - 取引集計")
    print("")
    print("confirm確認用:")
    print("  python scripts/houseaccount.py check-before            - confirm実行前の確認")
    print("  python scripts/houseaccount.py check-after             - confirm実行後の確認")
    print("  python scripts/houseaccount.py check-status            - 現在の状況確認")
    print("  python scripts/houseaccount.py check-all               - 全情報表示")
    print("")
    print("UFJ銀行CSV処理:")
    print("  python scripts/houseaccount.py process-ufj path.csv    - UFJ銀行CSV処理")
    print("  python scripts/houseaccount.py process-train path.csv  - CSV処理してtrainに保存")
    print("  python scripts/houseaccount.py train-ufj               - UFJ分類モデル学習")
    print("")
    print("その他:")
    print("  python scripts/houseaccount.py test                    - テスト実行")
    print("  python scripts/houseaccount.py clean                   - temp_journal クリア")
    print("")
    print("エイリアス（短縮形）:")
    print("  python scripts/houseaccount.py p path.csv              - process")
    print("  python scripts/houseaccount.py pu path.csv             - process-ufj")
    print("  python scripts/houseaccount.py c                       - confirm")
    print("  python scripts/houseaccount.py t                       - trial")
    print("  python scripts/houseaccount.py cb                      - check-before")
    print("  python scripts/houseaccount.py ca                      - check-after")

def main():
    """メイン関数"""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1]
    
    # 基本操作
    if command == 'init':
        run_command("python -m ledger_ingest.main init")
    
    elif command in ['process', 'p']:
        if len(sys.argv) < 3:
            print("エラー: CSVファイルのパスが必要です")
            print("使用例: python scripts/houseaccount.py process data/sample.csv")
            sys.exit(1)
        file_path = sys.argv[2]
        
        # オプション引数をそのまま渡す
        options = ' '.join(sys.argv[3:]) if len(sys.argv) > 3 else ''
        run_command(f"python -m ledger_ingest.main process {file_path} {options}")
    
    elif command in ['confirm', 'c']:
        run_command("python -m ledger_ingest.main confirm")
    
    elif command in ['trial', 't']:
        run_command("python -m ledger_ingest.main trial")
    
    elif command == 'cashflow':
        run_command("python -m ledger_ingest.main cashflow")
    
    elif command == 'summary':
        run_command("python -m ledger_ingest.main summary")
    
    # confirm確認用
    elif command in ['check-before', 'cb']:
        run_command("python scripts/check_confirm.py before")
    
    elif command in ['check-after', 'ca']:
        run_command("python scripts/check_confirm.py after")
    
    elif command == 'check-status':
        run_command("python scripts/check_confirm.py status")
    
    elif command == 'check-all':
        run_command("python -m ledger_ingest.query_helper all")
    
    # クエリヘルパー系
    elif command in ['summary-db', 'duplicates', 'balance', 'recent', 'files', 'preview']:
        run_command(f"python -m ledger_ingest.query_helper {command}")
    
    # UFJ銀行CSV処理
    elif command in ['process-ufj', 'pu']:
        if len(sys.argv) < 3:
            print("エラー: ファイルパスが必要です")
            print("使用例: python scripts/houseaccount.py process-ufj data/sample.csv")
            sys.exit(1)
        file_path = sys.argv[2]
        
        # オプション引数をそのまま渡す
        options = ' '.join(sys.argv[3:]) if len(sys.argv) > 3 else ''
        run_command(f"python -m ledger_ingest.main process-ufj {file_path} {options}")
    
    elif command == 'train-ufj':
        run_command("python -m ledger_ingest.main train-ufj")
    
    elif command == 'process-train':
        if len(sys.argv) < 3:
            print("エラー: ファイルパスが必要です")
            print("使用例: python scripts/houseaccount.py process-train data/sample.csv")
            sys.exit(1)
        file_path = sys.argv[2]
        
        run_command(f"python -m ledger_ingest.main process-train {file_path}")
    
    # その他
    elif command == 'test':
        run_command("python -m pytest tests/ -v")
    
    elif command == 'clean':
        from ledger_ingest.models import DatabaseManager
        from sqlalchemy import text
        
        db = DatabaseManager()
        with db.get_connection() as conn:
            result = conn.execute(text("DELETE FROM temp_journal"))
            count = result.rowcount
            print(f"temp_journalをクリアしました（{count}行削除）")
    
    elif command == 'help':
        show_help()
    
    else:
        print(f"不明なコマンド: {command}")
        print("使用可能なコマンドを確認するには: python scripts/houseaccount.py help")
        sys.exit(1)

if __name__ == '__main__':
    main()