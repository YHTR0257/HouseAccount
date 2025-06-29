# HouseAccount - 家計簿システム

シンプルな複式簿記ベースの家計簿アプリケーション

## 概要

- PostgreSQLベースの家計簿管理システム
- CSV形式でのデータ取り込み
- 複式簿記による自動検証
- 機械学習による自動分類（科目コード + 備考予測）
- 試算表・キャッシュフロー分析

## 特徴

- **複式簿記ベース**: 借方・貸方の平衡チェックによる正確な家計管理
- **機械学習分類**: UFJ銀行CSVの自動科目分類・備考予測
- **2つの予測モデル**: subject_code予測 + remarks予測
- **CSV取り込み**: 既存の家計データを簡単にインポート
- **自動検証**: 取引セットの平衡を自動チェック（Carry over除く）
- **月次集計**: financial_balance_sheet形式での月次レポート出力
- **柔軟な科目管理**: config/codes.jsonで科目コードをカスタマイズ可能

## システム要件

- Python 3.10+
- PostgreSQL
- Docker & Docker Compose（推奨）

## セットアップ

```bash
# データベース初期化
python -m src.main init

# 依存関係
pip install -r docker/requirements.txt
```

## 基本操作

### 1. CSV処理（仕訳取り込み）

```bash
# 標準CSV処理
python -m src.main process data/sample.csv

# UFJ銀行CSV処理（機械学習分類付き）
python -m src.main process-ufj data/ufj_bank.csv
```

### 2. 機械学習モデル学習

```bash
# 学習実行（subject_code予測 + remarks予測）
python -m src.main train
```

### 3. 仕訳確定・分析

```bash
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

### 標準CSVフォーマット
```csv
Date,SetID,SubjectCode,Amount,Remarks
2024-06-28,T001,100,-1000,コーヒー購入
2024-06-28,T001,500,1000,コーヒー購入
```

### UFJ銀行CSVフォーマット
UFJ銀行のCSVを直接読み込み、機械学習で自動分類します。

## 機械学習機能

### 2つの予測モデル

1. **subject_code予測モデル**
   - debit（借方科目コード）とcredit（貸方科目コード）のペアを予測
   - 例: `100_101` (現金 → UFJ銀行)

2. **remarks予測モデル**  
   - 取引の備考テキストを予測
   - 例: `ATM cash withdrawal`

### 学習データの管理

- 学習データは `data/train/` ディレクトリに保存
- 学習に使用するカラム: `date`, `abstruct`, `memo`, `combined_text`
- 予測対象カラム: `suggested_debit`, `suggested_credit`, `remarks_classified`
- 学習後、上記カラムのみを含むCSVが自動保存される

### 学習の流れ

1. UFJ CSV処理 → 学習データ蓄積
2. 手動で予測結果を修正（精度向上のため）
3. `python -m src.main train` で再学習
4. 予測精度が向上

## データベース構造

- `temp_journal` - 一時仕訳テーブル
- `journal_entries` - 確定仕訳テーブル  
- `trial_balance` - 試算表ビュー
- `account_balances` - 残高集計ビュー
- `transaction_sets` - 取引集計ビュー

## ファイル構成

### 主要ファイル
- `src/main.py` - メインエントリポイント
- `src/models.py` - データベース管理
- `src/processor.py` - CSV処理・集計
- `src/bank_predictor.py` - 機械学習予測器
- `config/codes.json` - 勘定科目マスタ

### ディレクトリ構成
```
data/
├── train/          # 学習データ（ufj_processed_*.csv, ufj_trained_*.csv）
├── uploads/        # 処理済みCSV
└── sample.csv      # サンプルデータ

models/             # 機械学習モデル
├── ufj_subjectcode_model.pkl      # subject_code予測モデル
├── ufj_subjectcode_encoder.pkl    # subject_codeエンコーダー
├── ufj_remarks_model.pkl          # remarks予測モデル
└── ufj_remarks_encoder.pkl        # remarksエンコーダー

config/
└── codes.json      # 勘定科目マスタ
```

## コマンドオプション

### process系コマンド
```bash
# オプション
--no-clear      # temp_journalテーブルをクリアしない
--no-duplicates # 重複チェックを行わない

# 例
python -m src.main process-ufj data/ufj.csv --no-clear
```

## 開発・テスト

```bash
# テスト用サンプルCSV作成済み
data/test_sample.csv

# 開発時のコマンド履歴
python -m src.main init
python -m src.main process-ufj data/ufj_bank.csv
python -m src.main train
python -m src.main confirm
python -m src.main trial
```

## 機械学習の精度向上のために

1. **学習データの品質向上**
   - `data/train/ufj_processed_*.csv` の予測結果を手動修正
   - `suggested_debit`, `suggested_credit`, `remarks_classified` を正しい値に修正

2. **定期的な再学習**
   - 新しいデータが蓄積されたら `python -m src.main train` で再学習

3. **データの確認**
   - 学習後 `data/train/ufj_trained_*.csv` で学習に使用されたデータを確認可能