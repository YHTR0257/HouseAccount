import pytest
import os
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import OperationalError
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ledger_ingest.database import DatabaseConfig, DatabaseManager, db_manager


class TestDatabaseConfig:
    """DatabaseConfig クラスのテスト"""
    
    @patch.dict(os.environ, {}, clear=True)
    def test_default_configuration(self):
        """デフォルト設定のテスト"""
        config = DatabaseConfig()
        
        assert config.database_url == 'postgresql://postgres:dev_password@postgres:5432/household_dev'
        assert config.debug is False
        assert config.log_level == 'INFO'
        assert 'poolclass' in config.engine_options
        assert config.engine_options['pool_size'] == 5
        assert config.engine_options['max_overflow'] == 10
        assert config.engine_options['pool_pre_ping'] is True
        assert config.engine_options['pool_recycle'] == 3600
        assert config.engine_options['echo'] is False
    
    @patch.dict(os.environ, {
        'DATABASE_URL': 'postgresql://test:test@localhost:5432/test',
        'DEBUG': 'true',
        'LOG_LEVEL': 'DEBUG'
    })
    def test_environment_override(self):
        """環境変数での設定上書きテスト"""
        config = DatabaseConfig()
        
        assert config.database_url == 'postgresql://test:test@localhost:5432/test'
        assert config.debug is True
        assert config.log_level == 'DEBUG'
        assert config.engine_options['echo'] is True
    
    @patch.dict(os.environ, {'DEBUG': 'false'})
    def test_debug_false_setting(self):
        """DEBUG=falseの設定テスト"""
        config = DatabaseConfig()
        assert config.debug is False
        assert config.engine_options['echo'] is False
    
    @patch.dict(os.environ, {'DEBUG': 'True'})
    def test_debug_case_insensitive(self):
        """DEBUGの大文字小文字を区別しないテスト"""
        config = DatabaseConfig()
        assert config.debug is True


class TestDatabaseManager:
    """DatabaseManager クラスのテスト"""
    
    def test_singleton_pattern(self):
        """シングルトンパターンのテスト"""
        manager1 = DatabaseManager()
        manager2 = DatabaseManager()
        
        assert manager1 is manager2
        assert manager1 is db_manager
    
    @patch('ledger_ingest.database.create_engine')
    @patch('ledger_ingest.database.sessionmaker')
    def test_initialization(self, mock_sessionmaker, mock_create_engine):
        """初期化処理のテスト"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_session_factory = MagicMock()
        mock_sessionmaker.return_value = mock_session_factory
        
        # 新しいインスタンスを作成（シングルトンをリセット）
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        
        mock_create_engine.assert_called_once()
        mock_sessionmaker.assert_called_once_with(
            bind=mock_engine,
            autocommit=False,
            autoflush=False
        )
        
        assert manager._engine is mock_engine
        assert manager._session_factory is mock_session_factory
    
    @patch('ledger_ingest.database.create_engine')
    def test_engine_property(self, mock_create_engine):
        """engine プロパティのテスト"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        engine = manager.engine
        
        assert engine is mock_engine
    
    @patch('ledger_ingest.database.create_engine')
    @patch('ledger_ingest.database.sessionmaker')
    def test_get_session(self, mock_sessionmaker, mock_create_engine):
        """get_session メソッドのテスト"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_session_factory = MagicMock()
        mock_session = MagicMock()
        mock_session_factory.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_factory
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        session = manager.get_session()
        
        mock_session_factory.assert_called_once()
        assert session is mock_session
    
    @patch('ledger_ingest.database.create_engine')
    def test_get_connection(self, mock_create_engine):
        """get_connection メソッドのテスト"""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        connection = manager.get_connection()
        
        mock_engine.connect.assert_called_once()
        assert connection is mock_connection
    
    @patch('ledger_ingest.database.create_engine')
    def test_init_tables(self, mock_create_engine):
        """init_tables メソッドのテスト"""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        manager.init_tables()
        
        # SQLが実行されたことを確認
        assert mock_connection.execute.call_count >= 5  # テーブル2つ + ビュー3つ
        mock_connection.commit.assert_called_once()
    
    @patch('ledger_ingest.database.create_engine')
    def test_test_connection_success(self, mock_create_engine):
        """test_connection メソッド（成功）のテスト"""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 1
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        result = manager.test_connection()
        
        assert result is True
        mock_connection.execute.assert_called_once()
    
    @patch('ledger_ingest.database.create_engine')
    def test_test_connection_failure(self, mock_create_engine):
        """test_connection メソッド（失敗）のテスト"""
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError("Connection failed", None, None)
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        result = manager.test_connection()
        
        assert result is False
    
    @patch('ledger_ingest.database.create_engine')
    def test_close(self, mock_create_engine):
        """close メソッドのテスト"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        manager.close()
        
        mock_engine.dispose.assert_called_once()
        assert manager._engine is None
        assert manager._session_factory is None


class TestIntegration:
    """統合テスト"""
    
    def test_db_manager_singleton(self):
        """グローバルインスタンスのテスト"""
        # シングルトンをリセット
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        # 新しいインスタンスとグローバルインスタンスが同じになることを確認
        manager1 = DatabaseManager()
        manager2 = DatabaseManager()
        
        assert manager1 is manager2
        assert isinstance(manager1, DatabaseManager)
    
    @patch.dict(os.environ, {
        'DATABASE_URL': 'postgresql://test_user:test_pass@test_host:5432/test_db',
        'DEBUG': 'true',
        'LOG_LEVEL': 'DEBUG'
    })
    @patch('ledger_ingest.database.create_engine')
    def test_full_configuration_flow(self, mock_create_engine):
        """完全な設定フローのテスト"""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # インスタンスをリセット
        DatabaseManager._instance = None
        DatabaseManager._engine = None
        DatabaseManager._session_factory = None
        
        manager = DatabaseManager()
        
        # create_engineが正しい設定で呼ばれたかチェック
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == 'postgresql://test_user:test_pass@test_host:5432/test_db'
        
        engine_options = call_args[1]
        assert engine_options['echo'] is True
        assert engine_options['pool_size'] == 5
        assert engine_options['max_overflow'] == 10
        assert engine_options['pool_pre_ping'] is True
        assert engine_options['pool_recycle'] == 3600