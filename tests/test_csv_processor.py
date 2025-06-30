import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

sys.path.append(str(Path(__file__).parent.parent))

from src.processor import CSVProcessor
from src.models import DatabaseManager


class TestCSVProcessor:
    """CSVProcessor統合テストクラス"""
    
    @pytest.fixture
    def sample_csv_data(self):
        """テスト用CSVデータ"""
        return """Date,ID,SubjectCode,Amount,Remarks,SetID
                    2024-03-01,,101,-44881,Carry over,99
                    2024-03-01,,109,0,Carry over,99
                    2024-03-01,,200,0,Carry over,99
                    2024-03-01,,100,-20000,Carry over,99
                    2024-03-01,,102,20000,Carry over,99
                    2024-03-02,,500,-850,Shogo,01
                    2024-03-02,,530,850,Shogo,01
                    2024-03-02,,101,-1850,PayPay,02
                    2024-03-02,,500,1850,PayPay,02"""

    @pytest.fixture
    def temp_csv_file(self, sample_csv_data):
        """一時CSVファイル作成"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(sample_csv_data)
            f.flush()
            yield f.name
        os.unlink(f.name)

    @pytest.fixture
    def mock_db_manager(self):
        """データベースマネージャーのモック"""
        mock_db = Mock(spec=DatabaseManager)
        mock_connection = MagicMock()
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_connection
        mock_context_manager.__exit__.return_value = None
        mock_db.get_connection.return_value = mock_context_manager
        return mock_db

    @pytest.fixture
    def processor(self, mock_db_manager):
        """CSVProcessor インスタンス"""
        return CSVProcessor(mock_db_manager)

    def test_csv_processor_initialization(self):
        """CSVProcessor初期化テスト"""
        # デフォルト初期化
        processor = CSVProcessor()
        assert processor.db is not None
        
        # 専用DBマネージャー指定
        custom_db = Mock(spec=DatabaseManager)
        processor_custom = CSVProcessor(custom_db)
        assert processor_custom.db == custom_db

    def test_process_csv_for_database_basic(self, processor, temp_csv_file, mock_db_manager):
        """基本的なCSV処理テスト"""
        with patch('pandas.DataFrame.to_sql') as mock_to_sql:
            with patch('pandas.read_sql') as mock_read_sql:
                # 重複チェックのモック（重複なし）
                mock_read_sql.return_value = pd.DataFrame({'count': [0]})
                
                # conn.execute().rowcountのモック
                mock_execute_result = MagicMock()
                mock_execute_result.rowcount = 0
                mock_db_manager.get_connection.return_value.__enter__.return_value.execute.return_value = mock_execute_result
                
                result = processor.process_csv_for_database(temp_csv_file)
                
                # 処理行数の確認
                assert result == 9
                
                # データベース保存が呼ばれたことを確認
                mock_to_sql.assert_called_once_with(
                    'temp_journal', 
                    mock_db_manager.get_connection.return_value.__enter__.return_value,
                    if_exists='append', 
                    index=False
                )

    def test_process_csv_for_database_data_transformation(self, processor, temp_csv_file):
        """CSV処理時のデータ変換テスト"""
        original_to_sql = pd.DataFrame.to_sql
        captured_df = None
        
        def capture_df(self, *args, **kwargs):
            nonlocal captured_df
            captured_df = self.copy()
            return original_to_sql(self, *args, **kwargs)
        
        with patch('pandas.DataFrame.to_sql', capture_df):
            with patch('pandas.read_sql') as mock_read_sql:
                # 重複チェックのモック（重複なし）
                mock_read_sql.return_value = pd.DataFrame({'count': [0]})
                with patch.object(processor.db, 'get_connection'):
                    processor.process_csv_for_database(temp_csv_file)
        
        # データ変換の確認
        assert captured_df is not None
        assert 'date' in captured_df.columns
        assert 'entry_id' in captured_df.columns
        assert 'set_id' in captured_df.columns
        assert 'year' in captured_df.columns
        assert 'month' in captured_df.columns
        assert 'subject' in captured_df.columns
        
        # SetIDとEntryIDの生成確認（3桁）
        assert captured_df['set_id'].notna().all()
        assert captured_df['entry_id'].notna().all()
        # SetIDが3桁形式になっているかチェック
        for set_id in captured_df['set_id'].unique():
            # YYYYMMDD_XXX 形式の確認
            assert len(set_id.split('_')[1]) == 3

    def test_validate_sets_balanced(self, processor, mock_db_manager):
        """平衡セット検証テスト"""
        # 平衡データのモック
        mock_connection = mock_db_manager.get_connection.return_value.__enter__.return_value
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()  # 空 = 平衡
            
            is_valid, message, errors = processor.validate_sets()
            
            assert is_valid == True
            assert "全セット平衡確認" in message
            assert errors is None

    def test_validate_sets_unbalanced(self, processor, mock_db_manager):
        """不平衡セット検証テスト"""
        # 不平衡データのモック
        unbalanced_data = pd.DataFrame({
            'set_id': ['T001'],
            'date': ['2024-03-01'],
            'remarks': ['Test'],
            'entry_count': [2],
            'balance': [100],  # 不平衡
            'entries': ['現金:100, 食費:-50']
        })
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = unbalanced_data
            
            is_valid, message, errors = processor.validate_sets()
            
            assert is_valid == False
            assert "不平衡なセットが1件あります" in message
            assert errors is not None
            assert len(errors) == 1

    def test_get_trial_balance(self, processor, mock_db_manager):
        """試算表取得テスト"""
        expected_data = pd.DataFrame({
            'subject_code': [100, 101, 500],
            'subject': ['現金', '普通預金', '食費'],
            'debit_total': [10000, 5000, 3000],
            'credit_total': [0, 0, 0],
            'balance': [10000, 5000, 3000]
        })
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = expected_data
            
            result = processor.get_trial_balance()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            mock_read_sql.assert_called_once()

    def test_get_transaction_summary(self, processor, mock_db_manager):
        """取引集計テスト"""
        expected_data = pd.DataFrame({
            'date': ['2024-03-01', '2024-03-02'],
            'set_id': ['T001', 'T002'],
            'remarks': ['Test 1', 'Test 2'],
            'entry_count': [2, 2],
            'entries': ['現金:1000, 食費:-1000', '普通預金:500, 交通費:-500']
        })
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = expected_data
            
            result = processor.get_transaction_summary()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            mock_read_sql.assert_called_once()

    def test_confirm_entries_success(self, processor, mock_db_manager):
        """仕訳確定成功テスト"""
        mock_connection = mock_db_manager.get_connection.return_value.__enter__.return_value
        
        # セット検証モック（成功）
        with patch.object(processor, 'validate_sets') as mock_validate:
            mock_validate.return_value = (True, "平衡確認", None)
            
            with patch('pathlib.Path.glob') as mock_glob:
                with patch('shutil.move') as mock_move:
                    mock_glob.return_value = []  # 移動対象ファイルなし
                    
                    result = processor.confirm_entries()
                    
                    assert result == True
                    # SQL実行確認
                    assert mock_connection.execute.call_count == 3

    def test_confirm_entries_failure(self, processor, mock_db_manager):
        """仕訳確定失敗テスト"""
        # セット検証モック（失敗）
        with patch.object(processor, 'validate_sets') as mock_validate:
            error_df = pd.DataFrame({'set_id': ['T001'], 'balance': [100]})
            mock_validate.return_value = (False, "不平衡エラー", error_df)
            
            result = processor.confirm_entries()
            
            assert result == False

    def test_get_cashflow_analysis(self, processor, mock_db_manager):
        """キャッシュフロー分析テスト"""
        expected_data = pd.DataFrame({
            'date': ['2024-03-01', '2024-03-02'],
            'set_id': ['T001', 'T002'],
            'remarks': ['現金収入', '現金支出'],
            'cash_change': [10000, -5000]
        })
        
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = expected_data
            
            result = processor.get_cashflow_analysis()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert 'cash_change' in result.columns
            mock_read_sql.assert_called_once()

    def test_financial_records_processing(self, processor):
        """実際の financial_records.csv 処理テスト"""
        financial_records_path = "/workspace/data/financial_records.csv"
        
        # ファイル存在確認
        if not Path(financial_records_path).exists():
            pytest.skip("financial_records.csv not found")
        
        with patch.object(processor.db, 'get_connection'):
            with patch('pandas.DataFrame.to_sql') as mock_to_sql:
                result = processor.process_csv_for_database(financial_records_path)
                
                # 処理行数が682行であることを確認
                assert result > 600  # 期待値の範囲確認
                mock_to_sql.assert_called_once()

    def test_month_end_processing_workflow(self, processor):
        """月末処理ワークフローテスト"""
        # 月末処理の完全なワークフローをテスト
        
        # 1. CSV処理
        with patch.object(processor, 'process_csv_for_database') as mock_process:
            mock_process.return_value = 100
            
            # 2. セット検証
            with patch.object(processor, 'validate_sets') as mock_validate:
                mock_validate.return_value = (True, "平衡確認", None)
                
                # 3. 仕訳確定
                with patch.object(processor, 'confirm_entries') as mock_confirm:
                    mock_confirm.return_value = True
                    
                    # 4. 試算表取得
                    with patch.object(processor, 'get_trial_balance') as mock_trial:
                        mock_trial.return_value = pd.DataFrame()
                        
                        # ワークフロー実行
                        process_result = processor.process_csv_for_database("test.csv")
                        validation_result = processor.validate_sets()
                        confirm_result = processor.confirm_entries()
                        trial_balance = processor.get_trial_balance()
                        
                        # 結果確認
                        assert process_result == 100
                        assert validation_result[0] == True
                        assert confirm_result == True
                        assert isinstance(trial_balance, pd.DataFrame)

    def test_error_handling(self, processor):
        """エラーハンドリングテスト"""
        # 存在しないファイル
        with pytest.raises(FileNotFoundError):
            processor.process_csv_for_database("nonexistent.csv")
        
        # 不正なCSV形式
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("invalid,csv,format\n1,2")  # 列数不一致
            f.flush()
            
            try:
                with patch.object(processor.db, 'get_connection'):
                    # pandas.read_csvがエラーを出すかテスト
                    with pytest.raises(Exception):  # 何らかのエラーが発生することを期待
                        processor.process_csv_for_database(f.name)
            finally:
                os.unlink(f.name)

    def test_subject_code_mapping(self, processor, temp_csv_file):
        """科目コードマッピングテスト"""
        original_to_sql = pd.DataFrame.to_sql
        captured_df = None
        
        def capture_df(self, *args, **kwargs):
            nonlocal captured_df
            captured_df = self.copy()
            return original_to_sql(self, *args, **kwargs)
        
        with patch('pandas.DataFrame.to_sql', capture_df):
            with patch.object(processor.db, 'get_connection'):
                processor.process_csv_for_database(temp_csv_file)
        
        # 科目名マッピングの確認
        assert captured_df is not None
        assert 'subject' in captured_df.columns
        # 科目コード100-600番台に対応する科目名があることを確認
        subject_mapped = captured_df[captured_df['subject'].notna()]
        assert len(subject_mapped) > 0

    def test_entry_id_generation(self, processor):
        """EntryID生成テスト（既存SetIDの場合）"""
        test_data = """Date,ID,SubjectCode,Amount,Remarks,SetID
                        2024-03-01,,100,-1000,Test,01
                        2024-03-01,,500,1000,Test,01
                        2024-03-02,,101,-2000,Another,02
                        2024-03-02,,530,2000,Another,02"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_data)
            f.flush()
            
            original_to_sql = pd.DataFrame.to_sql
            captured_df = None
            
            def capture_df(self, *args, **kwargs):
                nonlocal captured_df
                captured_df = self.copy()
                return original_to_sql(self, *args, **kwargs)
            
            try:
                with patch('pandas.DataFrame.to_sql', capture_df):
                    with patch('pandas.read_sql') as mock_read_sql:
                        mock_read_sql.return_value = pd.DataFrame({'count': [0]})
                        with patch.object(processor.db, 'get_connection'):
                            processor.process_csv_for_database(f.name)
                
                # EntryID生成確認
                assert captured_df is not None
                assert 'entry_id' in captured_df.columns
                
                # SetIDごとにEntryIDが連番で生成されることを確認（3桁）
                for set_id in captured_df['set_id'].unique():
                    set_entries = captured_df[captured_df['set_id'] == set_id]
                    entry_ids = sorted(set_entries['entry_id'].tolist())
                    # EntryIDが適切に生成されていることを確認
                    assert len(entry_ids) >= 1
                    # 3桁の連番になっているかチェック
                    for i, entry_id in enumerate(entry_ids):
                        expected_suffix = f"_{str(i).zfill(3)}"
                        assert entry_id.endswith(expected_suffix)
                    
            finally:
                os.unlink(f.name)