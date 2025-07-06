import json
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/household')

# Directory paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / 'config'
DATA_DIR = BASE_DIR / 'data'
TEMP_UPLOADS_DIR = DATA_DIR / 'uploads'
CONFIRMED_DIR = DATA_DIR / 'confirmed'

# Create directories if they don't exist
TEMP_UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
CONFIRMED_DIR.mkdir(parents=True, exist_ok=True)

# Load subject codes from JSON
with open(CONFIG_DIR / 'codes.json', 'r', encoding='utf-8') as f:
    codes_data = json.load(f)

# Convert to the format expected by the application
SUBJECT_CODES = {int(item['id']): name for name, item in codes_data.items()}

# Balance tolerance for validation
BALANCE_TOLERANCE = 0.01

# Logging configuration
def setup_logging() -> None:
    """
    アプリケーション全体のログ設定を行う
    LOG_LEVEL環境変数からログレベルを取得して設定
    """
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # ログレベルの検証
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level not in valid_levels:
        log_level = 'INFO'
    
    # ログフォーマットの設定
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # 基本ログ設定
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        datefmt=date_format,
        force=True  # 既存の設定を上書き
    )
    
    # ledger_ingestパッケージ専用のロガーを作成
    logger = logging.getLogger('ledger_ingest')
    logger.setLevel(getattr(logging, log_level))
    
    # ログレベルが設定されたことを記録
    logger.info(f"ログレベルを {log_level} に設定しました")

def get_logger(name: str) -> logging.Logger:
    """
    モジュール専用のロガーを取得
    
    Args:
        name: モジュール名（通常は __name__ を渡す）
        
    Returns:
        設定済みのロガーインスタンス
    """
    # ledger_ingestプレフィックスを付与
    if not name.startswith('ledger_ingest'):
        if name == '__main__':
            name = 'ledger_ingest.main'
        elif '.' not in name:
            name = f'ledger_ingest.{name}'
    
    return logging.getLogger(name)