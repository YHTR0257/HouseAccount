FROM apache/superset:latest

USER root
WORKDIR /app

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
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# superset ユーザーに sudo 権限を付与
RUN usermod -aG sudo superset \
    && echo 'superset ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# 起動スクリプトを作成
COPY start.sh .
RUN chmod +x start.sh \
    && chown superset:superset start.sh

# superset_home ディレクトリの権限を事前に設定
RUN mkdir -p superset_home \
    && chown -R superset:superset superset_home \
    && chmod 755 superset_home

USER superset

CMD ["./docker/start.sh"]