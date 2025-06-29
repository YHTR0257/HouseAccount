#!/bin/bash
set -e

# Dev Container内かどうかの判定
if [ "$VSCODE_INJECTION" = "1" ] || [ -n "$REMOTE_CONTAINERS" ] || [ -n "$CODESPACES" ] || [ -f "/.dockerenv" ]; then
    echo "🔧 Running in Dev Container mode"
    export PYTHONPATH=/app/src
    echo "PYTHONPATH set to: $PYTHONPATH"
    echo "Keeping container alive for Dev Container..."
    exec sleep infinity
fi

echo "🚀 Starting production mode"

# データベース接続待機
echo "データベース接続待機..."
while ! nc -z postgres 5432; do 
    sleep 1
done

export PYTHONPATH=/workspace/src

# Superset初期化
echo "Superset データベース初期化中..."
superset db upgrade

if ! superset fab list-users | grep -q "admin"; then
    echo "管理者ユーザー作成中..."
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@example.com \
        --password admin
fi

superset init

# 家計簿システム初期化
echo "家計簿システム初期化中..."
cd /app
if python -m src.main init; then
    echo "家計簿システム初期化完了"
else
    echo "Warning: 家計簿システム初期化に失敗しました。Supersetのみ起動します。"
fi

# Superset起動
echo "Superset起動中..."
gunicorn --bind 0.0.0.0:8088 --workers 1 --timeout 60 "superset.app:create_app()"