# HouseAccount - 家計簿システム

複式簿記ベースの家計簿アプリケーション（機械学習による自動分類機能付き）

## 概要

- PostgreSQLベースの家計簿管理システム
- CSV形式でのデータ取り込み（汎用形式・銀行CSV対応）
- 複式簿記による自動検証
- 機械学習による科目・摘要の自動分類
- 試算表・キャッシュフロー分析
- Apache Supersetによる可視化・ダッシュボード機能

## アーキテクチャ

### 技術スタック
- **Python 3.10+** - メインアプリケーション
- **PostgreSQL** - データベース
- **Apache Superset** - BI・可視化プラットフォーム
- **Redis** - キャッシュ・セッション管理
- **Docker** - コンテナ化・開発環境
- **SQLAlchemy** - ORM
- **scikit-learn** - 機械学習

### サービス構成
- **ledger_ingest** - メインアプリケーション
- **postgres** - データベース
- **redis** - キャッシュサーバー
- **superset** - 可視化・ダッシュボード
- **superset-worker** - Supersetワーカー
- **superset-worker-beat** - Supersetスケジューラー

### 主要モジュール
- `ledger_ingest/main.py` - CLI エントリーポイント
- `ledger_ingest/processor.py` - CSV処理・データベース操作
- `ledger_ingest/bank_predictor.py` - 機械学習による分類
- `ledger_ingest/database.py` - データベース管理
- `ledger_ingest/config.py` - 設定管理

## セットアップ

### 前提条件
- Docker & Docker Compose
- Python 3.10以上（開発時）

### 初期セットアップ
```bash
# 環境変数設定
cp .env.example .env
# .env ファイルを環境に合わせて編集

# サービス起動
make up

# データベース初期化
make init

# 初期データロード（オプション）
make load-initial-data
```

### 開発環境
```bash
# 開発用コンテナ起動
make dev

# テスト実行
make test

# 型チェック
make typecheck

# リント
make lint

# 機械学習モデル訓練
make train
```

## 環境変数

### 必須設定
- `POSTGRES_USER` - PostgreSQLユーザー名
- `POSTGRES_PASSWORD` - PostgreSQLパスワード
- `POSTGRES_DB` - データベース名
- `REDIS_URL` - Redis接続URL
- `SUPERSET_SECRET_KEY` - Supersetシークレットキー

### オプション設定
- `DEBUG` - デバッグモード（default: False）
- `LOG_LEVEL` - ログレベル（default: INFO）
- `ML_MODEL_PATH` - 機械学習モデルパス

## 使用方法

### CLI コマンド
```bash
# CSVファイル処理
python -m ledger_ingest.main process <csv_file>

# 機械学習モデル訓練
python -m ledger_ingest.main train

# データベース初期化
python -m ledger_ingest.main init

# 試算表生成
python -m ledger_ingest.main balance <date>
```

### 銀行CSV対応
- **JCB**: `config/jcb.yml`
- **三菱UFJ**: `config/ufj.yml`
- **SBI**: `config/sbi.yml`

## テスト

### テスト実行
```bash
# 全テスト実行
make test

# 特定テスト実行
pytest tests/test_processor.py

# カバレッジ付きテスト
pytest --cov=ledger_ingest tests/
```

### テストカテゴリ
- **Unit Tests**: `tests/test_*.py`
- **Integration Tests**: `tests/integration/`
- **Database Tests**: `tests/test_database.py`

## データ構造

### 主要テーブル
- `accounts` - 勘定科目マスター
- `transactions` - 取引データ
- `journal_entries` - 仕訳データ
- `ml_training_data` - 機械学習訓練データ

### データディレクトリ
- `data/training/` - 訓練用データ
- `data/processing/` - 処理中データ
- `data/uploads/` - アップロードファイル
- `models/` - 機械学習モデル

## トラブルシューティング

### よくある問題
1. **データベース接続エラー**
   - PostgreSQLサービスが起動していることを確認
   - 環境変数の設定を確認

2. **CSV処理エラー**
   - ファイルエンコーディングを確認（UTF-8推奨）
   - CSVフォーマットが設定ファイルと一致することを確認

3. **機械学習モデルエラー**
   - 訓練データが十分にあることを確認
   - モデルファイルが存在することを確認

### ログ確認
```bash
# アプリケーションログ
docker compose logs ledger_ingest

# データベースログ
docker compose logs postgres

# 全サービスログ
docker compose logs
```

## メモリーズ

### Docker関連
- docker-composeは現在は使われておらず、docker composeというコマンドになっている
- docker-composeのバージョン指定は必要なくなった

### コーディング
- Pylanceのエラーを避けるためにoptionalを使うことは避ける
- Pylanceには忠実に従う
- コードはシンプルでわかりやすいものが望ましい。しかし、エラー処理を怠りすぎることもだめ。

### 開発ワークフロー
- 新機能開発時は必ずテストを作成する
- 型ヒントは必須（mypy準拠）
- 機械学習モデルの変更時は性能評価を実施する
- データベーススキーマ変更時はマイグレーションスクリプトを作成する