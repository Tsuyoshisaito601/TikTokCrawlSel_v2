import sys
import os

# srcディレクトリをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawler.tiktok_crawler import TikTokCrawler
from src.logger import setup_logger
from dotenv import load_dotenv
import time

logger = setup_logger(__name__)

def test_login():
    """ログイン機能のテスト"""
    load_dotenv()
    
    try:
        # データベースから取得する代わりに、テスト用のアカウントを直接指定
        username = "wskorng+01@gmail.com"
        password = "<gitwn2%W$j?H93"
        
        # クローラーの初期化
        crawler = TikTokCrawler(username=username, password=password)
        
        try:
            # クローラーを開始してログイン
            crawler.start()
            
            # ログイン成功後、少し待機して画面を確認
            time.sleep(10)
            
        finally:
            # クローラーを停止
            crawler.stop()
            
    except Exception as e:
        logger.error(f"テスト中にエラーが発生: {e}")
        raise

if __name__ == "__main__":
    test_login()
