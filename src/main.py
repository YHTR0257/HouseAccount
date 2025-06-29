import sys
from pathlib import Path
from .models import DatabaseManager
from .processor import CSVProcessor


def main():
    if len(sys.argv) < 2:
        print("使用方法: python main.py [init|process|process-ufj|confirm|trial|cashflow|summary|train] [args...]")
        print("  process オプション:")
        print("    --no-clear    : temp_journalテーブルをクリアしない")
        print("    --no-duplicates : 重複チェックを行わない")
        print("  process-ufj オプション:")
        print("    python main.py process-ufj <ファイルパス> [--no-clear] [--no-duplicates]")
        print("  train:")
        print("    python main.py train  : 機械学習モデルの学習（subject_code + remarks）")
        return

    command = sys.argv[1]

    if command == 'init':
        db = DatabaseManager()
        db.init_tables()
        print("データベース初期化完了")

    elif command == 'process' and len(sys.argv) > 2:
        processor = CSVProcessor()
        file_path = sys.argv[2]
        
        # オプション解析
        clear_temp = '--no-clear' not in sys.argv
        check_duplicates = '--no-duplicates' not in sys.argv

        count = processor.process_csv_for_database(file_path, clear_temp=clear_temp, check_duplicates=check_duplicates)
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

    elif command == 'process-ufj' and len(sys.argv) > 2:
        processor = CSVProcessor()
        file_path = sys.argv[2]
        
        # オプション解析
        clear_temp = '--no-clear' not in sys.argv
        check_duplicates = '--no-duplicates' not in sys.argv

        count = processor.process_bank_csv(file_path, 'ufj', clear_temp=clear_temp, check_duplicates=check_duplicates)
        print(f"UFJ CSV処理完了: {count}件の仕訳を読み込み")

        # セット検証
        is_valid, message, errors = processor.validate_sets()
        print(f"検証結果: {message}")
        if errors is not None:
            print("不平衡セット:")
            print(errors)

        if is_valid:
            print("\n取引集計:")
            print(processor.get_transaction_summary())

    elif command == 'train':
        from .bank_predictor import BankPredictor
        predictor = BankPredictor()
        success = predictor.train_model()
        
        if success:
            print("機械学習モデル学習完了")
            print("  - subject_code予測モデル: debit/credit科目コード予測")
            print("  - remarks予測モデル: 備考テキスト予測")
        else:
            print("機械学習モデル学習失敗: 学習データが不足している可能性があります")

    else:
        print("無効なコマンドです")

if __name__ == '__main__':
    main()
