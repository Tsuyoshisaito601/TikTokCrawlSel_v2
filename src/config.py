import os
from dotenv import load_dotenv

# .envファイルの読み込み
load_dotenv()

# データベース設定
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', ''),
    'user': os.getenv('MYSQL_USER', ''),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', '')
}

# クローリング設定
CRAWL_CONFIG = {
    'wait_time': {
        'min': 2,  # 最小待機時間（秒）
        'max': 5   # 最大待機時間（秒）
    },
    'scroll_config': {
        'max_scroll': 10,  # 最大スクロール回数
        'scroll_pause_time': 1.5  # スクロール間の待機時間（秒）
    }
}
