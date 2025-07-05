#!/bin/bash
set -e

echo "🚀 Starting Superset..."

# 環境変数設定
export FLASK_APP=superset
export SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

# データベース接続待機
echo "Waiting for database..."
while ! nc -z postgres 5432; do 
    sleep 1
done
echo "Database ready!"

# Superset初期化（公式の方法）
echo "Initializing Superset..."
superset db upgrade
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin
superset init

echo "✅ Superset ready at http://localhost:8088"
echo "   Username: admin"
echo "   Password: admin"

# Superset起動
exec superset run -p 8088 --with-threads --reload --debugger --host=0.0.0.0