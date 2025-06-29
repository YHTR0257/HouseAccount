# HouseAccount - Makefile
# 便利なコマンドのショートカット

.PHONY: help init process confirm trial cashflow summary check-before check-after check-status

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
	python -m src.main init

process: ## CSVファイル処理 (例: make process FILE=data/sample.csv)
	@if [ -z "$(FILE)" ]; then \
		echo "エラー: FILEパラメータが必要です"; \
		echo "使用例: make process FILE=data/sample.csv"; \
		exit 1; \
	fi
	python -m src.main process $(FILE)

confirm: ## 仕訳確定
	python -m src.main confirm

trial: ## 試算表表示
	python -m src.main trial

cashflow: ## キャッシュフロー分析  
	python -m src.main cashflow

summary: ## 取引集計
	python -m src.main summary

check-before: ## confirm実行前の確認
	python scripts/check_confirm.py before

check-after: ## confirm実行後の確認
	python scripts/check_confirm.py after

check-status: ## 現在の状況確認
	python scripts/check_confirm.py status

check-all: ## 全情報表示
	python -m src.query_helper all

test: ## テスト実行
	python -m pytest tests/ -v

clean: ## temp_journalクリア
	python -c "from src.models import DatabaseManager; from sqlalchemy import text; db = DatabaseManager(); conn = db.get_connection(); conn.execute(text('DELETE FROM temp_journal')); print('temp_journalをクリアしました')"