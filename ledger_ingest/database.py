import os
import logging
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Connection
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseConfig:
    def __init__(self) -> None:
        self.database_url: str = os.getenv('DATABASE_URL', 'postgresql://postgres:dev_password@postgres:5432/household_dev')
        self.debug: bool = os.getenv('DEBUG', 'false').lower() == 'true'
        self.log_level: str = os.getenv('LOG_LEVEL', 'INFO')
        
        self.engine_options: Dict[str, Any] = {
            'poolclass': QueuePool,
            'pool_size': 5,
            'max_overflow': 10,
            'pool_pre_ping': True,
            'pool_recycle': 3600,
            'echo': self.debug,
        }

class DatabaseManager:
    _instance: Optional['DatabaseManager'] = None
    _engine: Optional[Engine] = None
    _session_factory: Optional[sessionmaker[Session]] = None
    
    def __new__(cls) -> 'DatabaseManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        if self._engine is None:
            self._initialize()
    
    def _initialize(self) -> None:
        config = DatabaseConfig()

        self._engine: Engine = create_engine(
            config.database_url,
            **config.engine_options
        )

        self._session_factory: sessionmaker[Session] = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False
        )
        
        # ロガー設定は config.setup_logging() で一元管理されるため、ここでは設定しない
        logger.info(f"Database initialized: {config.database_url}")
    
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._initialize()
        return self._engine
    
    def get_session(self) -> Session:
        if self._session_factory is None:
            self._initialize()
        return self._session_factory()
    
    def get_connection(self) -> Connection:
        return self.engine.connect()
    
    def init_tables(self) -> None:
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS temp_journal (
                    date DATE,
                    set_id VARCHAR(20),
                    entry_id VARCHAR(20),
                    subject_code INTEGER,
                    amount DECIMAL(12,2),
                    remarks TEXT,
                    subject VARCHAR(50),
                    year INTEGER,
                    month INTEGER,
                    source_file VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS journal_entries (
                    date DATE,
                    set_id VARCHAR(20),
                    entry_id VARCHAR(20) PRIMARY KEY,
                    subject_code INTEGER,
                    amount DECIMAL(12,2),
                    remarks TEXT,
                    subject VARCHAR(50),
                    year INTEGER,
                    month INTEGER,
                    confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # confirmed_atカラムが存在しない場合に追加
            self._add_confirmed_at_column_if_missing(conn)
            
            conn.execute(text("""
                CREATE OR REPLACE VIEW account_balances AS
                SELECT
                    subject_code,
                    subject,
                    SUM(amount) as balance,
                    year,
                    month
                FROM journal_entries
                GROUP BY subject_code, subject, year, month
                ORDER BY year, month, subject_code
            """))
            
            conn.execute(text("""
                CREATE OR REPLACE VIEW transaction_sets AS
                SELECT
                    date,
                    set_id,
                    remarks,
                    COUNT(*) as entry_count,
                    SUM(amount) as balance_check,
                    string_agg(subject || ':' || amount::text, ', ' ORDER BY amount DESC) as entries
                FROM journal_entries
                GROUP BY date, set_id, remarks
                ORDER BY date, set_id
            """))
            
            conn.execute(text("DROP VIEW IF EXISTS trial_balance;"))
            conn.execute(text("""
                CREATE VIEW trial_balance AS
                WITH monthly_balances AS (
                    SELECT
                        year,
                        month,
                        subject_code,
                        subject,
                        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as debit_total,
                        SUM(CASE WHEN amount < 0 THEN -amount ELSE 0 END) as credit_total,
                        SUM(amount) as monthly_balance
                    FROM journal_entries
                    WHERE remarks NOT IN ('close', 'loss and benefit')
                    GROUP BY year, month, subject_code, subject
                )
                SELECT
                    m.year,
                    m.month,
                    m.subject_code,
                    m.subject,
                    m.debit_total,
                    m.credit_total,
                    CASE
                        WHEN m.subject_code < 400 THEN
                            SUM(m.monthly_balance) OVER (PARTITION BY m.subject_code ORDER BY m.year, m.month)
                        ELSE
                            m.monthly_balance
                    END as balance
                FROM monthly_balances m
                ORDER BY m.year, m.month, m.subject_code;
            """))
            
            conn.commit()
            logger.info("データベーステーブルとビューを作成しました")
    
    def _add_confirmed_at_column_if_missing(self, conn: Connection) -> None:
        """confirmed_atカラムが存在しない場合に追加"""
        try:
            # カラムが存在するかチェック
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'journal_entries' 
                AND column_name = 'confirmed_at'
            """))
            
            # fetchone()を使用してチェック
            column_exists = result.fetchone() is not None
            
            if not column_exists:
                # カラムが存在しない場合は追加
                conn.execute(text("""
                    ALTER TABLE journal_entries 
                    ADD COLUMN confirmed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """))
                logger.info("confirmed_atカラムを追加しました")
            else:
                logger.info("confirmed_atカラムは既に存在します")
        except Exception as e:
            logger.error(f"confirmed_atカラムの確認・追加に失敗: {e}")
    
    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"データベース接続テストに失敗しました: {e}")
            return False
    
    def close(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("データベース接続を閉じました")

db_manager = DatabaseManager()