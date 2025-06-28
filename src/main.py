import sys
from pathlib import Path
from .models import DatabaseManager
from .processor import CSVProcessor


def main():
    if len(sys.argv) < 2:
        print("使用方法: python main.py [init|process|confirm|trial|cashflow|summary] [args...]")
        return

    command = sys.argv[1]

    if command == 'init':
        db = DatabaseManager()
        db.init_tables()
        print("データベース初期化完了")

    elif command == 'process' and len(sys.argv) > 2:
        processor = CSVProcessor()
        file_path = sys.argv[2]

        count = processor.process_csv_for_database(file_path)
        print(f"処理完了: {count}件の仕訳を読み込み")

        # セット検証
        is_valid, message, errors = processor.validate_sets()
        print(f"検証結果: {message}")
        if errors is not None:
            print("不平衡セット:")
            print(errors)

        if is_valid:
            print("\n取引集計:")
            print(processor.get_transaction_summary())

    elif command == 'confirm':
        processor = CSVProcessor()
        if processor.confirm_entries():
            print("仕訳確定完了")
        else:
            print("確定処理中止（検証エラー）")

    elif command == 'trial':
        processor = CSVProcessor()
        print("試算表:")
        print(processor.get_trial_balance())

    elif command == 'cashflow':
        processor = CSVProcessor()
        print("キャッシュフロー:")
        print(processor.get_cashflow_analysis())

    elif command == 'summary':
        processor = CSVProcessor()
        print("取引集計:")
        print(processor.get_transaction_summary())

    else:
        print("無効なコマンドです")

if __name__ == '__main__':
    main()
