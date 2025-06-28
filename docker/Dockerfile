FROM apache/superset:latest

USER root

# 必要最小限のパッケージのみインストール
RUN apt-get update && apt-get install -y \
    fonts-noto-cjk \
    netcat-openbsd \
    curl \
    git \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# requirements.txtをコピーしてPythonパッケージのインストール
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Node.js LTSのインストール（Claude Code用）
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# npmのグローバルディレクトリを設定
RUN mkdir -p /home/superset/.npm-global \
    && chown -R superset:superset /home/superset/.npm-global

# 起動スクリプトを作成（root権限で）
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Dev Container内かどうかの判定\n\
if [ "$VSCODE_INJECTION" = "1" ] || [ -n "$REMOTE_CONTAINERS" ] || [ -n "$CODESPACES" ]; then\n\
    echo "🔧 Running in Dev Container mode"\n\
    export PYTHONPATH=/workspace/src\n\
    exec sleep infinity\n\
fi\n\
\n\
echo "🚀 Starting production mode"\n\
echo "データベース接続待機..."\n\
while ! nc -z postgres 5432; do \n\
    sleep 1\n\
done\n\
\n\
export PYTHONPATH=/workspace/src\n\
\n\
echo "Superset データベース初期化中..."\n\
superset db upgrade\n\
\n\
if ! superset fab list-users | grep -q "admin"; then\n\
    echo "管理者ユーザー作成中..."\n\
    superset fab create-admin \\\n\
        --username admin \\\n\
        --firstname Admin \\\n\
        --lastname User \\\n\
        --email admin@example.com \\\n\
        --password admin\n\
fi\n\
\n\
superset init\n\
\n\
echo "家計簿システム初期化中..."\n\
cd /workspace\n\
if python -m src.main init; then\n\
    echo "家計簿システム初期化完了"\n\
else\n\
    echo "Warning: 家計簿システム初期化に失敗しました。Supersetのみ起動します。"\n\
fi\n\
\n\
echo "Superset起動中..."\n\
gunicorn --bind 0.0.0.0:8088 --workers 1 --timeout 60 "superset.app:create_app()"' > /app/start.sh \
    && chmod +x /app/start.sh

USER superset

# npm設定
RUN npm config set prefix '/home/superset/.npm-global'
ENV PATH="/home/superset/.npm-global/bin:$PATH"

WORKDIR /workspace
CMD ["/app/start.sh"]