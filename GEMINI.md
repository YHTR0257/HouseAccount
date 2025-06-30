# HouseAccount - 家計簿システム

## プロジェクト概要

このプロジェクトは、複式簿記に基づいた家計簿アプリケーションです。主な機能は以下の通りです。

- **データ入力**: CSVファイル（標準形式、またはUFJ銀行、SBI銀行の明細形式）を読み込み、仕訳データとして取り込みます。
- **自動分類**: 機械学習モデルを利用して、UFJ銀行のCSVデータから勘定科目（借方・貸方）と取引内容（備考）を自動で予測・分類します。
- **データ検証**: 複式簿記の原則に基づき、各取引セットの借方と貸方が一致しているかを自動で検証します。
- **データ確定**: 検証済みの仕訳データを確定し、正式な会計帳簿に記録します。
- **分析・レポート**: 試算表、キャッシュフロー計算書、取引集計などのレポートを生成し、家計の状況を可視化します。

## 主要技術スタック

- **バックエンド**: Python
- **データベース**: PostgreSQL
- **機械学習**: scikit-learn, pandas, numpy
- **コンテナ技術**: Docker, Docker Compose
- **その他ライブラリ**: SQLAlchemy (ORM), PyYAML (設定ファイル), pytest (テスト)

## ディレクトリ構成の概要

- `src/`: アプリケーションのコアロジックを格納
    - `main.py`: コマンドラインインターフェースのエントリーポイント
    - `processor.py`: CSVの処理、データ検証、レポート生成などを行う
    - `models.py`: データベースのテーブル定義や接続を管理
    - `bank_predictor.py`: 機械学習モデルの学習と予測を行う
- `data/`: 入力データ、学習データ、処理済みデータなどを格納
    - `train/`: 機械学習の学習データを保存
    - `uploads/`: 処理済みのCSVファイルを保存
- `models/`: 学習済みの機械学習モデル（`.pkl`ファイル）を保存
- `config/`: 勘定科目コード（`codes.json`）や、銀行CSVの処理設定（`.yml`）を格納
- `docker/`: DockerfileとPythonの依存関係ファイル（`requirements.txt`）を格納
- `tests/`: pytestを使用したテストコードを格納
- `scripts/`: 補助的なスクリプトを格納

## セットアップと実行

### 1. 環境構築

Docker Composeを使用して、PostgreSQLデータベースとアプリケーション環境を起動します。

```bash
docker-compose up -d
```

### 2. データベースの初期化

初回起動時に、必要なテーブルを作成します。

```bash
make init
```
または
```bash
docker-compose exec app python -m src.main init
```

### 3. CSVデータの処理

サンプルデータ（`data/test_sample.csv`）を処理して、仕訳を生成します。

```bash
make process FILE=data/test_sample.csv
```
または
```bash
docker-compose exec app python -m src.main process data/test_sample.csv
```

### 4. 仕訳の確定

生成された仕訳を検証し、問題がなければ確定します。

```bash
make confirm
```

### 5. レポートの表示

試算表やキャッシュフローなどのレポートを確認します。

```bash
make trial
make cashflow
make summary
```

## 機械学習機能について

- **目的**: UFJ銀行の取引明細から、勘定科目と備考を自動で予測し、手入力を省力化します。
- **モデル**:
    1. `ufj_subjectcode_model.pkl`: 勘定科目コード（借方・貸方）を予測
    2. `ufj_remarks_model.pkl`: 取引の備考を予測
- **学習プロセス**:
    1. `process-ufj`コマンドでUFJ銀行のCSVを処理すると、予測結果付きのデータが`data/train/`に保存されます。
    2. ユーザーは必要に応じて予測結果を修正します。
    3. `train`コマンドを実行すると、修正されたデータを使ってモデルが再学習され、精度が向上します。

```bash
# UFJ銀行のCSVを処理
make process FILE=data/ufj_bank.csv

# モデルの学習
make train
```

## 開発者向け情報

- **テストの実行**: `make test`
- **Makefile**: `init`, `process`, `confirm`など、よく使うコマンドのショートカットが定義されています。詳細は`Makefile`を参照してください。
- **設定ファイル**:
    - `config/codes.json`: 勘定科目の一覧。カスタマイズ可能です。
    - `config/ufj_process.yml`, `config/sbi_process.yml`: 各銀行のCSVファイルのカラム名と内部名のマッピングを定義しています。
