import sys
import datetime
from pathlib import Path
from .database import db_manager
from .processor import CSVProcessor
from .config import setup_logging, get_logger

# ログ設定を初期化
setup_logging()
logger = get_logger(__name__)

def main():
    if len(sys.argv) < 2:
        print("使用方法: python main.py [init|process|process-ufj|process-jcb|confirm|trial|cashflow|summary|train|close|status] [args...]")
        print("  process オプション:")
        print("    --no-clear    : temp_journalテーブルをクリアしない")
        print("    --no-duplicates : 重複チェックを行わない")
        print("  process-ufj オプション:")
        print("    python main.py process-ufj <ファイルパス> [--no-clear] [--no-duplicates]")
        print("  process-jcb オプション:")
        print("    python main.py process-jcb <ファイルパス> [--no-clear] [--no-duplicates]")
        print("  train:")
        print("    python main.py train [ufj|jcb] : 機械学習モデルの学習（subject_code + remarks）")
        print("  close:")
        print("    python main.py close [年月] [--reclose] : 月次締切処理（YYYY-MM形式、省略時は前月）")
        print("    --reclose : 既に締切済みの月も再度締切する")
        print("  status:")
        print("    python main.py status : 家計状況確認（query_helperの簡易版）")
        return

    command = sys.argv[1]

    if command == 'init':
        logger.info("データベース初期化を開始します")
        db_manager.init_tables()
        logger.info("データベース初期化が完了しました")
        print("データベース初期化完了")

    elif command == 'process' and len(sys.argv) > 2:
        processor = CSVProcessor()
        file_path = sys.argv[2]
        
        # オプション解析
        clear_temp = '--no-clear' not in sys.argv
        check_duplicates = '--no-duplicates' not in sys.argv

        logger.info(f"CSV処理を開始します: {file_path}")
        count = processor.process_csv_for_database(file_path, clear_temp=clear_temp, check_duplicates=check_duplicates)
        logger.info(f"CSV処理が完了しました: {count}件の仕訳を読み込み")
        print(f"処理完了: {count}件の仕訳を読み込み")

        # セット検証
        logger.debug("セット検証を開始します")
        is_valid, message, errors = processor.validate_sets()
        if is_valid:
            logger.info(f"検証成功: {message}")
        else:
            logger.warning(f"検証で問題が発見されました: {message}")
            if errors is not None:
                logger.error(f"不平衡セット詳細: {errors}")
        print(f"検証結果: {message}")
        if errors is not None:
            print("不平衡セット:")
            print(errors)

        if is_valid:
            print("\n取引集計:")
            print(processor.get_transaction_summary())

    elif command == 'confirm':
        processor = CSVProcessor()
        logger.info("仕訳確定処理を開始します")
        if processor.confirm_entries():
            logger.info("仕訳確定処理が正常に完了しました")
            print("仕訳確定完了")
        else:
            logger.error("仕訳確定処理が検証エラーで中止されました")
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

        logger.info(f"UFJ CSV処理を開始します: {file_path}")
        count = processor.process_bank_csv(file_path, 'ufj', clear_temp=clear_temp, check_duplicates=check_duplicates)
        logger.info(f"UFJ CSV処理が完了しました: {count}件の仕訳を読み込み")
        print(f"UFJ CSV処理完了: {count}件の仕訳を読み込み")

        # セット検証
        logger.debug("セット検証を開始します (UFJ)")
        is_valid, message, errors = processor.validate_sets()
        if is_valid:
            logger.info(f"検証成功 (UFJ): {message}")
        else:
            logger.warning(f"検証で問題が発見されました (UFJ): {message}")
            if errors is not None:
                logger.error(f"不平衡セット詳細 (UFJ): {errors}")
        print(f"検証結果: {message}")
        if errors is not None:
            print("不平衡セット:")
            print(errors)

        if is_valid:
            print("\n取引集計:")
            print(processor.get_transaction_summary())

    elif command == 'process-jcb' and len(sys.argv) > 2:
        processor = CSVProcessor()
        file_path = sys.argv[2]
        
        # オプション解析
        clear_temp = '--no-clear' not in sys.argv
        check_duplicates = '--no-duplicates' not in sys.argv

        logger.info(f"JCB CSV処理を開始します: {file_path}")
        count = processor.process_bank_csv(file_path, 'jcb', clear_temp=clear_temp, check_duplicates=check_duplicates)
        logger.info(f"JCB CSV処理が完了しました: {count}件の仕訳を読み込み")
        print(f"JCB CSV処理完了: {count}件の仕訳を読み込み")

        # セット検証
        logger.debug("セット検証を開始します (JCB)")
        is_valid, message, errors = processor.validate_sets()
        if is_valid:
            logger.info(f"検証成功 (JCB): {message}")
        else:
            logger.warning(f"検証で問題が発見されました (JCB): {message}")
            if errors is not None:
                logger.error(f"不平衡セット詳細 (JCB): {errors}")
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
        
        # 銀行種別の指定（train ufj または train jcb）
        bank = 'ufj'  # デフォルト
        if len(sys.argv) > 2:
            if sys.argv[2] in ['ufj', 'jcb']:
                bank = sys.argv[2]
        
        logger.info(f"機械学習モデルの学習を開始します: {bank.upper()}")
        success = predictor.train_model(bank)
        
        if success:
            logger.info(f"機械学習モデル学習が正常に完了しました: {bank.upper()}")
            print(f"機械学習モデル学習完了: {bank.upper()}")
            print("  - subject_code予測モデル: debit/credit科目コード予測")
            print("  - remarks予測モデル: 備考テキスト予測")
        else:
            logger.error(f"機械学習モデル学習に失敗しました: {bank.upper()} - 学習データが不足している可能性があります")
            print(f"機械学習モデル学習失敗: {bank.upper()} 学習データが不足している可能性があります")

    elif command == 'close':
        # 年月が指定されていない場合は、先月を自動で設定
        if len(sys.argv) > 2 and not sys.argv[2].startswith('--'):
            target_year_month = sys.argv[2]
        else:
            # 先月を計算
            today = datetime.date.today()
            if today.month == 1:
                target_year_month = f"{today.year - 1}-12"
            else:
                target_year_month = f"{today.year}-{today.month - 1:02d}"
        
        # recloseフラグの確認
        reclose = '--reclose' in sys.argv
        
        logger.info(f"月次締切処理を開始します: {target_year_month} (reclose={reclose})")
        processor = CSVProcessor()
        processor.close_monthly_balance(target_year_month, reclose=reclose)
        logger.info(f"月次締切処理が完了しました: {target_year_month}")

    elif command == 'status':
        from .query_helper import QueryHelper
        helper = QueryHelper()
        helper.show_financial_status()
        helper.show_closing_status()

    else:
        logger.warning(f"無効なコマンドが指定されました: {command}")
        print("無効なコマンドです")

if __name__ == '__main__':
    main()
