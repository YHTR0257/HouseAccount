FROM apache/superset:latest

USER root

# 必要パッケージのインストール
RUN apt-get update && apt-get install -y \
    fonts-noto-cjk \
    netcat-openbsd \
    curl \
    git \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# sudoインストール後にsupersetユーザーにsudo権限を付与
RUN echo "superset ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/superset \
    && chmod 0440 /etc/sudoers.d/superset

# Pythonパッケージのインストール
RUN pip install --no-cache-dir psycopg2-binary pandas scikit-learn python-dotenv gunicorn

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
if [ "$VSCODE_INJECTION" = "1" ] || [ -n "$REMOTE_CONTAINERS" ]; then\n\
    echo "🔧 Running in Dev Container mode"\n\
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
if [ -f "src/main.py" ]; then\n\
    python src/main.py init\n\
else\n\
    echo "Warning: src/main.py not found, skipping household system initialization"\n\
fi\n\
\n\
echo "Superset起動中..."\n\
gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 60 "superset.app:create_app()"' > /app/start.sh \
    && chmod +x /app/start.sh

USER superset

# npm設定
RUN npm config set prefix '/home/superset/.npm-global'
ENV PATH="/home/superset/.npm-global/bin:$PATH"

WORKDIR /workspace
CMD ["/app/start.sh"]