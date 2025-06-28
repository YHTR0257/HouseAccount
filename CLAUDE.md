# HouseAccount - 家計簿システム

シンプルな複式簿記ベースの家計簿アプリケーション

## 概要

- PostgreSQLベースの家計簿管理システム
- CSV形式でのデータ取り込み
- 複式簿記による自動検証
- 試算表・キャッシュフロー分析

## セットアップ

```bash
# データベース初期化
python -m src.main init

# 依存関係
pip install -r docker/requirements.txt
```

## 基本操作

```bash
# CSV処理（仕訳取り込み）
python -m src.main process data/sample.csv

# 仕訳確定
python -m src.main confirm

# 試算表表示
python -m src.main trial

# 取引集計
python -m src.main summary

# キャッシュフロー分析
python -m src.main cashflow
```

## CSVフォーマット

```csv
Date,SetID,SubjectCode,Amount,Remarks
2024-06-28,T001,100,-1000,コーヒー購入
2024-06-28,T001,500,1000,コーヒー購入
```

- **SetID**: 取引セット（貸借が平衡する単位）
- **SubjectCode**: 勘定科目コード（config/codes.json参照）
- **Amount**: 金額（正：借方、負：貸方）

## 主要ファイル

- `src/main.py` - メインエントリポイント
- `src/models.py` - データベース管理
- `src/processor.py` - CSV処理・集計
- `src/config.py` - 設定
- `config/codes.json` - 勘定科目マスタ

## データベース構造

- `temp_journal` - 一時仕訳テーブル
- `journal_entries` - 確定仕訳テーブル  
- `trial_balance` - 試算表ビュー
- `account_balances` - 残高集計ビュー
- `transaction_sets` - 取引集計ビュー

## 開発・テスト

```bash
# テスト用サンプルCSV作成済み
data/test_sample.csv

# 開発時のコマンド履歴
python -m src.main init
python -m src.main process data/test_sample.csv
python -m src.main confirm
python -m src.main trial
```