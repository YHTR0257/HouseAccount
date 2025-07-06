# HouseAccount - Makefile
# 便利なコマンドのショートカット

.PHONY: help init process confirm trial cashflow summary check-before check-after check-status close

help: ## ヘルプ表示
	@echo "HouseAccount - 利用可能なコマンド:"
	@echo ""
	@echo "基本操作:"
	@echo "  make init                    - データベース初期化"
	@echo "  make process FILE=path.csv   - CSVファイル処理"
	@echo "  make confirm                 - 仕訳確定"
	@echo "  make trial                   - 試算表表示"
	@echo "  make cashflow                - キャッシュフロー分析"
	@echo "  make summary                 - 取引集計"
	@echo "  make close [MONTH=YYYY-MM] [RECLOSE=1] - 月次締切処理"
	@echo "  make status                  - 家計状況確認"
	@echo ""
	@echo "confirm確認用:"
	@echo "  make check-before            - confirm実行前の確認"
	@echo "  make check-after             - confirm実行後の確認"
	@echo "  make check-status            - 現在の状況確認"
	@echo "  make check-all               - 全情報表示"
	@echo ""
	@echo "その他:"
	@echo "  make test                    - テスト実行"
	@echo "  make clean                   - temp_journal クリア"

init: ## データベース初期化
	python -m ledger_ingest.main init

process: ## CSVファイル処理 (例: make process FILE=data/sample.csv)
	@if [ -z "$(FILE)" ]; then \
		echo "エラー: FILEパラメータが必要です"; \
		echo "使用例: make process FILE=data/sample.csv"; \
		exit 1; \
	fi
	python -m ledger_ingest.main process $(FILE)

process-ufj: ## UFJ銀行のCSVファイル処理 (例: make process-ufj FILE=data/ufj.csv)
	@if [ -z "$(FILE)" ]; then \
		echo "エラー: FILEパラメータが必要です"; \
		echo "使用例: make process-ufj FILE=data/ufj.csv"; \
		exit 1; \
	fi
	python -m ledger_ingest.main process-ufj $(FILE)

process-jcb: ## JCBカードのCSVファイル処理 (例: make process-jcb FILE=data/jcb.csv)
	@if [ -z "$(FILE)" ]; then \
		echo "エラー: FILEパラメータが必要です"; \
		echo "使用例: make process-jcb FILE=data/jcb.csv"; \
		exit 1; \
	fi
	python -m ledger_ingest.main process-jcb $(FILE)

confirm: ## 仕訳確定
	python -m ledger_ingest.main confirm

trial: ## 試算表表示
	python -m ledger_ingest.main trial

cashflow: ## キャッシュフロー分析  
	python -m ledger_ingest.main cashflow

summary: ## 取引集計
	python -m ledger_ingest.main summary

check-before: ## confirm実行前の確認
	python scripts/check_confirm.py before

check-after: ## confirm実行後の確認
	python scripts/check_confirm.py after

check-status: ## 現在の状況確認
	python scripts/check_confirm.py status

check-all: ## 全情報表示
	python -m ledger_ingest.query_helper all

test: ## テスト実行
	python -m pytest tests/ -v

close: ## 月次締切処理 (例: make close MONTH=2024-06 RECLOSE=1)
	@if [ -n "$(MONTH)" ] && [ -n "$(RECLOSE)" ]; then \
		python -m ledger_ingest.main close $(MONTH) --reclose; \
	elif [ -n "$(MONTH)" ]; then \
		python -m ledger_ingest.main close $(MONTH); \
	elif [ -n "$(RECLOSE)" ]; then \
		python -m ledger_ingest.main close --reclose; \
	else \
		python -m ledger_ingest.main close; \
	fi

status: ## 家計状況確認
	python -m ledger_ingest.main status

clean: ## temp_journalクリア
	python -c "from ledger_ingest.database import DatabaseManager; from sqlalchemy import text; db = DatabaseManager(); conn = db.get_connection(); conn.execute(text('DELETE FROM temp_journal')); print('temp_journalをクリアしました')"

train: ## 機械学習モデルの学習 (例: make train ARGS=ufj)
	@if [ -z "$(ARGS)" ]; then \
		echo "エラー: ARGSパラメータが必要です (ufj または jcb)"; \
		echo "使用例: make train ARGS=ufj"; \
		exit 1; \
	fi
	python -m ledger_ingest.main train $(ARGS)

