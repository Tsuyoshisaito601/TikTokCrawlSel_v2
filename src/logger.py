import logging
import sys
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logger(name: str) -> logging.Logger:
    """
    ロガーの設定を行う
    
    Args:
        name: ロガーの名前
    
    Returns:
        設定済みのロガーインスタンス
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # フォーマッターの作成
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # ログファイルの設定
    log_dir = os.path.join('output', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # ファイルハンドラーの設定
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # コンソールハンドラーの設定
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # ハンドラーの追加
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
