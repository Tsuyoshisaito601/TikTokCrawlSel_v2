import sys
import os
import time

# srcディレクトリをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawler.selenium_manager import SeleniumManager
from src.logger import setup_logger

logger = setup_logger(__name__)

def test_bot_detection():
    """ボット検知テストを実行"""
    # テスト用のURLリスト
    test_urls = [
        'https://bot.sannysoft.com',
        'https://arh.antoinevastel.com/bots/areyouheadless',
        'https://antoinevastel.com/bots/datadome',
        'https://abrahamjuliot.github.io/creepjs/'
    ]
    
    try:
        # コマンドライン引数から取得
        if len(sys.argv) != 2:
            logger.error("使用方法: python test_selenium.py <サイト番号(0-3)>")
            return
        
        try:
            site_index = int(sys.argv[1])
        except ValueError:
            logger.error("サイト番号は数値で指定してください")
            return
        
        if not 0 <= site_index < len(test_urls):
            logger.error(f"無効なインデックス: {site_index}")
            return
        
        # SeleniumManagerの初期化
        selenium_manager = SeleniumManager()
        
        try:
            # ドライバーの設定
            driver = selenium_manager.setup_driver()
            
            # 指定されたURLにアクセス
            url = test_urls[site_index]
            logger.info(f"テストサイトにアクセス: {url}")
            driver.get(url)
            
            # 結果を確認するための入力待機
            logger.info("結果を確認したらEnterキーを押してください...")
            input()
            
        finally:
            # ブラウザを終了
            selenium_manager.quit_driver()
            
    except Exception as e:
        logger.error(f"テスト中にエラーが発生: {e}")
        raise

if __name__ == "__main__":
    test_bot_detection()
