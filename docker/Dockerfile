FROM apache/superset:latest

USER root

# 必要なパッケージをまとめてインストール
RUN apt-get update && apt-get install -y \
    fonts-noto-cjk \
    netcat-openbsd \
    curl \
    git \
    sudo \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Pythonパッケージのインストール
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# superset ユーザーに sudo 権限を付与
RUN usermod -aG sudo superset \
    && echo 'superset ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# 起動スクリプトを作成
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# superset_home ディレクトリの権限を事前に設定
RUN mkdir -p /app/superset_home \
    && chown -R superset:superset /app/superset_home \
    && chmod 755 /app/superset_home

USER superset

WORKDIR /app
CMD ["/app/start.sh"]