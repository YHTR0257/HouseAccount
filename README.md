# HouseAccount - 家計簿システム

シンプルな複式簿記ベースの家計簿アプリケーション

## 概要

HouseAccountは複式簿記の原理に基づいた家計簿管理システムです。CSVファイルから取引データを読み込み、自動的に仕訳の平衡チェックを行い、試算表や月次レポートを生成できます。

## 特徴

- **複式簿記ベース**: 借方・貸方の平衡チェックによる正確な家計管理
- **CSV取り込み**: 既存の家計データを簡単にインポート
- **自動検証**: 取引セットの平衡を自動チェック（Carry over除く）
- **月次集計**: financial_balance_sheet形式での月次レポート出力
- **柔軟な科目管理**: config/codes.jsonで科目コードをカスタマイズ可能

## システム要件

- Python 3.10+
- PostgreSQL
- Docker & Docker Compose（推奨）

## セットアップ

### 1. 環境構築

```bash
# リポジトリのクローン
git clone <repository-url>
cd HouseAccount

# Docker環境の起動
docker-compose up -d

# 依存関係のインストール
pip install -r docker/requirements.txt
```

### 2. データベース初期化

```bash
python -m src.main init
```

## 使用方法

### 基本的なワークフロー

1. **CSVファイルの準備**
2. **データ処理・検証**
3. **仕訳確定**
4. **レポート出力**

### CSVファイル形式

```csv
Date,ID,SubjectCode,Amount,Remarks,SetID
2024-03-01,,101,-44881,Carry over,99
2024-03-01,,200,45000,Carry over,99
2024-03-02,,500,-850,Coffee,01
2024-03-02,,101,850,Coffee,01
```

#### 必須フィールド

- **Date**: 取引日（YYYY-MM-DD形式またはYYYYMMDD形式）
- **SubjectCode**: 勘定科目コード（config/codes.json参照）
- **Amount**: 金額（正：借方、負：貸方）
- **Remarks**: 摘要
- **SetID**: 取引セットID（同一取引をグループ化）

#### 対応する日付形式

システムは以下の日付形式に自動対応します：

- **YYYY-MM-DD形式**: `2024-03-01`（推奨）
- **YYYYMMDD形式**: `20240301`
- **混合形式**: 同一ファイル内で両形式の混在も可能

```csv
Date,ID,SubjectCode,Amount,Remarks,SetID
2024-03-01,,101,-1000,YYYY-MM-DD format,01
20240302,,500,1000,YYYYMMDD format,02
```

#### 科目コード例

- **100-199**: 資産（現金、銀行預金、投資等）
- **200-399**: 負債（クレジットカード債務、ローン等）
- **400-499**: 収入（給与、その他収入等）
- **500-699**: 支出（食費、交通費、娯楽費等）

### コマンドライン操作

#### 1. CSVファイルの処理

```bash
python -m src.main process data/financial_records.csv
```

このコマンドは：
- CSVファイルを読み込み
- SetIDとEntryIDを自動生成
- 科目名を自動マッピング
- 平衡チェックを実行
- 取引集計を表示

#### 2. 仕訳の確定

```bash
python -m src.main confirm
```

平衡チェックに通った仕訳を正式に確定します。

#### 3. 試算表の表示

```bash
python -m src.main trial
```

#### 4. キャッシュフロー分析

```bash
python -m src.main cashflow
```

現金・預金（科目コード100-102）の動きを分析します。

#### 5. 取引集計

```bash
python -m src.main summary
```

### Python APIの使用

```python
from src.processor import CSVProcessor

# プロセッサーの初期化
processor = CSVProcessor()

# CSVファイルの処理
count = processor.process_csv_for_database('data/financial_records.csv')
print(f"処理件数: {count}")

# 平衡チェック
is_valid, message, errors = processor.validate_sets()
if is_valid:
    print("全セット平衡確認")
    
    # 仕訳確定
    if processor.confirm_entries():
        print("仕訳確定完了")
        
        # 月次バランスシート生成
        balance_sheet = processor.generate_balance_sheet_format()
        print(balance_sheet)
else:
    print(f"エラー: {message}")
    print(errors)
```

### 月末処理の完全ワークフロー

```python
# 一括処理
balance_sheet = processor.process_month_end_complete_workflow('data/financial_records.csv')
```

## データ検証ルール

### 平衡チェック

- 同一日付・同一SetIDの取引は合計が0になる必要があります
- **例外**: "Carry over"を含む取引は繰越処理のため不平衡可
- 許容誤差: ±0.01

### データクリーニング

- Amount列の無効文字（'m'等）は自動除去
- 数値変換できない場合は0に置換

## ファイル構成

```
HouseAccount/
├── src/
│   ├── main.py          # コマンドラインインターフェース
│   ├── processor.py     # CSVProcessorクラス
│   ├── models.py        # データベース管理
│   └── config.py        # 設定管理
├── config/
│   └── codes.json       # 勘定科目マスタ
├── data/
│   ├── financial_records.csv    # サンプルデータ
│   ├── uploads/         # 処理待ちCSV
│   └── confirmed/       # 確定済みCSV
├── tests/               # テストファイル
├── docker/              # Docker設定
└── README.md
```

## データベース構造

### 主要テーブル

- **temp_journal**: 一時仕訳テーブル（検証前）
- **journal_entries**: 確定仕訳テーブル
- **trial_balance**: 試算表ビュー
- **account_balances**: 残高集計ビュー

### テーブル作成

データベースのテーブルは`python -m src.main init`で自動作成されます。

## 出力例

### 試算表

```
subject_code | subject      | debit_total | credit_total | balance
-------------|--------------|-------------|--------------|--------
100          | Cash         | 500000      | 450000       | 50000
101          | UFJ          | 200000      | 180000       | 20000
500          | Food         | 50000       | 0            | -50000
```

### 月次バランスシート

financial_balance_sheet.csv形式で出力：

```
YearMonth | 100   | 101    | 500    | TotalAssets | TotalExpenses
----------|-------|--------|--------|-------------|---------------
2024-03   | 50000 | 20000  | -50000 | 70000       | -50000
2024-04   | 45000 | 25000  | -60000 | 70000       | -60000
```

## トラブルシューティング

### よくある問題

1. **数値エラー**: Amount列に無効な文字が含まれている
   - 自動クリーニング機能で対応済み

2. **平衡エラー**: 取引セットが不平衡
   - データを確認し、借方・貸方の合計を調整

3. **科目コードエラー**: 未定義の科目コード
   - config/codes.jsonに追加するか、既存コードを使用

### ログ確認

処理中のエラーや警告は標準出力に表示されます。

## 開発・テスト

```bash
# テスト実行
python -m pytest tests/ -v

# 個別テスト
python -m pytest tests/test_csv_processor.py -v
```

## ライセンス

[ライセンス情報をここに記載]