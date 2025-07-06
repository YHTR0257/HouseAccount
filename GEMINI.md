# HouseAccount - 家計簿システム

## プロジェクト概要

このプロジェクトは、複式簿記に基づいた家計簿アプリケーションです。主な機能は以下の通りです。

- **データ入力**: CSVファイル（標準形式、またはUFJ銀行、SBI銀行、JCBカードの明細形式）を読み込み、仕訳データとして取り込みます。
- **自動分類**: 機械学習モデルを利用して、UFJ銀行やJCBカードのCSVデータから勘定科目（借方・貸方）と取引内容（備考）を自動で予測・分類します。
- **データ検証**: 複式簿記の原則に基づき、各取引セットの借方と貸方が一致しているかを自動で検証します。
- **データ確定**: 検証済みの仕訳データを確定し、正式な会計帳簿に記録します。
- **月次処理**: 月末に締め処理を行い、年度の財務諸表を作成します。
- **分析・レポート**: 試算表、キャッシュフロー計算書、取引集計などのレポートを生成し、家計の状況を可視化します。

## 主要技術スタック

- **バックエンド**: Python
- **データベース**: PostgreSQL
- **機械学習**: scikit-learn, pandas, numpy
- **コンテナ技術**: Docker, Docker Compose
- **その他ライブラリ**: SQLAlchemy (ORM), PyYAML (設定ファイル), pytest (テスト)

## デ���レクトリ構成

| ディレクトリ | 役割 |
|:---|:---|
| `config/` | アプリケーションの設定ファイルを管理します。勘定科目の一覧（`codes.json`）や、各金融機関（UFJ, SBI, JCB）のCSVフォーマット定義（`.yml`ファイル）が格納されています。これにより、新しい金融機関への対応や勘定科目のカスタマイズが容易になります。 |
| `data/` | アプリケーションで扱うデータを集約するディレクトリです。`train/`（機械学習データ）、`uploads/`（処理済みCSV）、`confirmed/`（確定仕訳）、`postgres/`（DBデータ）などのサブディレクトリが含まれます。 |
| `docker/` | Docker関連のファイルを格納します。アプリケーションの実行環境を定義する`Dockerfile`や、Pythonの依存ライブラリをリスト化した`requirements.txt`が含まれます。 |
| `ledger_ingest/` | このアプリケーションの心臓部となるPythonパッケージです。CSVファイルの取り込み、複式簿記のルールに基づいたデータ検証、機械学習による勘定科目の自動予測、データベースとのやり取りなど、主要なビジネスロジックがすべてここに実装されています。 |
| `logs/` | アプリケーションの実行ログを保��するためのディレクトリです。 |
| `models/` | 学習済みの機械学習モデル（`.pkl`形式のファイル）を保存します。`bank_predictor.py`によって生成されたモデルがここに格納され、新しいデータの予測時に利用されます。 |
| `scripts/` | 開発や運用を補助するためのスクリプトを格納します。 |
| `tests/` | `pytest`を使用した自動テストコードを格納します。コードの品質を保証し、リファクタリングを安全に行うために不可欠です。 |
| `superset/` | データ可視化ツール[Apache Superset](https://superset.apache.org/)のソースコードです。家計簿データをダッシュボードやチャートで視覚的に分析するために利用されます。 |
| `superset_home/` | Apache Supersetの設定ファイルやメタデータ（ダッシュボード、チャート、データベース接続情報など）を永続化するためのディレクトリです。 |

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
docker-compose exec app python -m ledger_ingest.main init
```

### 3. CSVデータの処理

サンプルデータ（`data/test_sample.csv`）を処理して、仕訳を生成します。

```bash
make process FILE=data/test_sample.csv
```
または
```bash
docker-compose exec app python -m ledger_ingest.main process data/test_sample.csv
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

### 6. 月次締め処理

指定した年月の締め処理を実行します（例: 2023年12月）。

```bash
make close ARGS=2023-12
```
または
```bash
docker-compose exec app python -m ledger_ingest.main close 2023-12
```

## 機械学習機能について

- **目的**: UFJ銀行やJCBカードの取引明細から��勘定科目と備考を自動で予測し、手入力を省力化します。
- **モデル**:
    1. `ufj_subjectcode_model.pkl`, `jcb_subjectcode_model.pkl`: 勘定科目コード（借方・貸方）を予測
    2. `ufj_remarks_model.pkl`, `jcb_remarks_model.pkl`: 取引の備考を予測
- **学習プロセス**:
    1. `process-ufj`や`process-jcb`コマンドで各CSVを処理すると、予測結果付きのデータが`data/train/`に保存されます。
    2. ユーザーは必要に応じて予測結果を修正します。
    3. `train`コマンドを実行すると、修正されたデータを使ってモデルが再学習され、精度が向上します。

```bash
# UFJ銀行のCSVを処理
make process-ufj FILE=data/ufj_bank.csv

# JCBカードのCSVを処理
make process-jcb FILE=data/jcb_card.csv

# モデルの学習 (ufjまたはjcbを指定)
make train ARGS=ufj
make train ARGS=jcb
```

## 開発者向け情報

- **テストの実行**: `make test`
- **Makefile**: `init`, `process`, `confirm`など、よく使うコマンドのショートカットが定義されています。詳細は`Makefile`を参照してください。
- **設定ファイル**:
    - `config/codes.json`: 勘定科目の一覧。カスタマイズ可能です。
    - `config/ufj_process.yml`, `config/sbi_process.yml`, `config/jcb_process.yml`: 各銀行・カードのCSVファイルのカラム名と内部名のマッピングを定義しています。