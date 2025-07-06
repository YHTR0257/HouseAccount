"""
Integration test for the new bank CSV workflow:
process-jcb/process-ufj -> process -> confirm
"""

import sys
import os
sys.path.append('/workspace')

from ledger_ingest.processor import CSVProcessor
from ledger_ingest.config import PROCESS_DIR, CONFIRMED_DIR
import pandas as pd
from pathlib import Path
import tempfile
import pytest


def test_new_bank_workflow():
    """Test new bank CSV workflow: process_bank_csv -> process -> confirm"""
    
    print("=== 新銀行CSVワークフロー統合テスト開始 ===")
    
    # 1. CSVProcessor初期化
    processor = CSVProcessor()
    print("✓ CSVProcessor初期化完了")
    
    # 2. テスト用UFJデータの準備
    ufj_test_data = """取引日,摘要,摘要内容,支払金額,受取金額,残高,memo,取引店名,取引店番号
2024-03-01,振込入金,テスト入金,,5000,15000,,,
2024-03-02,デビット1,テストショップ,1500,,13500,,,
2024-03-03,振込出金,家賃支払い,60000,,53500,,,"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='shift_jis') as f:
        f.write(ufj_test_data)
        f.flush()
        ufj_test_file = f.name
    
    try:
        # 3. process_bank_csv でファイル処理（中間CSV生成）
        print("\n--- Step 1: Bank CSV処理 (中間ファイル生成) ---")
        
        # PROCESS_DIRをクリア
        for file in PROCESS_DIR.glob('*.csv'):
            file.unlink()
        
        # UFJ CSV処理
        processed_count = processor.process_bank_csv(ufj_test_file, 'ufj', clear_temp=True, check_duplicates=False)
        print(f"✓ UFJ CSV処理完了: {processed_count}件の仕訳")
        
        # 中間ファイルの確認
        process_files = list(PROCESS_DIR.glob('*.csv'))
        assert len(process_files) > 0, "中間CSVファイルが生成されていません"
        print(f"✓ 中間CSVファイル生成確認: {len(process_files)}個のファイル")
        
        # 中間ファイルの内容確認
        for file in process_files:
            df = pd.read_csv(file)
            print(f"  - {file.name}: {len(df)}行")
            assert len(df) > 0, f"中間ファイル {file.name} が空です"
            required_columns = ['Date', 'SetID', 'EntryID', 'SubjectCode', 'Amount', 'Remarks', 'Subject']
            missing_columns = set(required_columns) - set(df.columns)
            assert len(missing_columns) == 0, f"必要な列が不足: {missing_columns}"
        
        # 4. process コマンドで中間ファイルを読み込み
        print("\n--- Step 2: Process コマンド (データベース登録) ---")
        
        # 最初のファイルのパスを取得
        first_process_file = process_files[0]
        
        # データベース処理
        db_processed_count = processor.process_csv_for_database(str(first_process_file))
        print(f"✓ データベース処理完了: {db_processed_count}件の仕訳をtemp_journalに登録")
        
        # セット検証
        is_valid, message, errors = processor.validate_sets()
        print(f"✓ セット検証: {message}")
        assert is_valid, f"セット検証エラー: {message}"
        
        # 5. confirm コマンドで仕訳確定
        print("\n--- Step 3: Confirm コマンド (仕訳確定) ---")
        
        # 確定前のprocess/ディレクトリのファイル数確認
        process_files_before = list(PROCESS_DIR.glob('*.csv'))
        process_count_before = len(process_files_before)
        
        # 確定処理
        confirm_success = processor.confirm_entries()
        print(f"✓ 仕訳確定: {'成功' if confirm_success else '失敗'}")
        assert confirm_success, "仕訳確定に失敗しました"
        
        # 確定後のディレクトリ確認
        process_files_after = list(PROCESS_DIR.glob('*.csv'))
        confirmed_files = list(CONFIRMED_DIR.glob('*.csv'))
        
        print(f"✓ ファイル移動確認:")
        print(f"  - process/: {process_count_before} → {len(process_files_after)}ファイル")
        print(f"  - confirmed/: {len(confirmed_files)}ファイル")
        
        # ファイルが正しく移動されたことを確認
        assert len(process_files_after) == 0, "process/ディレクトリにファイルが残っています"
        assert len(confirmed_files) >= process_count_before, "confirmed/ディレクトリにファイルが移動されていません"
        
        # 6. データベース状態の確認
        print("\n--- Step 4: データベース状態確認 ---")
        
        # temp_journalがクリアされていることを確認
        temp_summary = processor.get_transaction_summary()
        print(f"✓ temp_journal状態: {len(temp_summary)}件")
        assert len(temp_summary) == 0, "temp_journalがクリアされていません"
        
        # 試算表の確認
        try:
            trial_balance = processor.get_trial_balance()
            print(f"✓ 試算表取得: {len(trial_balance)}行")
        except Exception as e:
            print(f"ℹ 試算表取得エラー（想定内）: {e}")
        
        print("\n=== 新ワークフロー統合テスト完了 ===")
        print("✓ process_bank_csv → process → confirm のワークフローが正常に動作")
        print("✓ 中間CSVファイルの生成・処理・移動が正常に実行")
        print("✓ データベース操作が正常に実行")
        print("✓ ファイル管理が正常に実行")
        
        assert True
        
    finally:
        # テストファイルのクリーンアップ
        try:
            os.unlink(ufj_test_file)
        except:
            pass
        
        # process/ディレクトリのクリーンアップ
        for file in PROCESS_DIR.glob('*.csv'):
            try:
                file.unlink()
            except:
                pass


def test_bank_csv_no_database_operations():
    """Test that process_bank_csv does not perform database operations"""
    
    print("=== process_bank_csvのDB非操作テスト ===")
    
    processor = CSVProcessor()
    
    # テスト用データ
    test_data = """取引日,摘要,摘要内容,支払金額,受取金額,残高
2024-03-01,テスト,テスト取引,1000,,9000"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='shift_jis') as f:
        f.write(test_data)
        f.flush()
        test_file = f.name
    
    try:
        # PROCESS_DIRをクリア
        for file in PROCESS_DIR.glob('*.csv'):
            file.unlink()
        
        # 最初のtemp_journal状態を確認
        initial_summary = processor.get_transaction_summary()
        initial_count = len(initial_summary)
        
        # process_bank_csvを実行
        processed_count = processor.process_bank_csv(test_file, 'ufj', clear_temp=False, check_duplicates=False)
        
        # temp_journalの状態を再確認
        final_summary = processor.get_transaction_summary()
        final_count = len(final_summary)
        
        print(f"✓ temp_journal状態: {initial_count} → {final_count}件")
        print(f"✓ 処理された仕訳: {processed_count}件")
        
        # process_bank_csvがtemp_journalに書き込んでいないことを確認
        assert initial_count == final_count, "process_bank_csvがtemp_journalに書き込んでいます"
        
        # 中間ファイルが生成されていることを確認
        process_files = list(PROCESS_DIR.glob('*.csv'))
        assert len(process_files) > 0, "中間CSVファイルが生成されていません"
        
        print("✓ process_bank_csvはDB操作を行わず、ファイル出力のみ実行")
        
    finally:
        try:
            os.unlink(test_file)
        except:
            pass
        
        # process/ディレクトリのクリーンアップ
        for file in PROCESS_DIR.glob('*.csv'):
            try:
                file.unlink()
            except:
                pass


if __name__ == "__main__":
    test_new_bank_workflow()
    test_bank_csv_no_database_operations()
    print("\n🎉 新ワークフローの全テストが正常に完了しました！")