#!/bin/bash

echo "=== Superset環境診断 ==="
echo

echo "1. Python環境確認"
echo "Python version: $(python --version)"
echo "Python path: $(which python)"
echo "Python executable: $(python -c 'import sys; print(sys.executable)')"
echo

echo "2. pip環境確認"
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"
echo

echo "3. インストール済みパッケージ確認"
echo "--- superset関連パッケージ ---"
pip list | grep -i superset || echo "Supersetパッケージが見つかりません"
echo

echo "4. Pythonパス設定確認"
echo "PYTHONPATH: ${PYTHONPATH:-未設定}"
echo "sys.path:"
python -c "import sys; [print(f'  {p}') for p in sys.path]"
echo

echo "5. supersetコマンドの場所確認"
echo "superset command: $(which superset 2>/dev/null || echo '見つかりません')"
echo "superset script content:"
if [ -f "/usr/local/bin/superset" ]; then
    echo "--- /usr/local/bin/superset の内容 ---"
    head -10 /usr/local/bin/superset
    echo
fi

echo "6. 手動インポートテスト"
echo "--- Supersetモジュールのインポートテスト ---"
python -c "
try:
    import superset
    print('✓ superset モジュールのインポート成功')
    print(f'  Location: {superset.__file__}')
    print(f'  Version: {superset.__version__}')
except ImportError as e:
    print('✗ superset モジュールのインポート失敗')
    print(f'  Error: {e}')
except Exception as e:
    print(f'✗ その他のエラー: {e}')
"

echo
echo "7. 環境変数確認"
echo "USER: ${USER:-未設定}"
echo "HOME: ${HOME:-未設定}"
echo "PATH: ${PATH}"
echo

echo "8. Docker環境確認"
if [ -f "/.dockerenv" ]; then
    echo "Docker環境: Yes"
    echo "Container ID: $(hostname)"
else
    echo "Docker環境: No"
fi