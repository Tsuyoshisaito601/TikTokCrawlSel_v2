from .database import Database
from ..logger import setup_logger
from dotenv import load_dotenv

logger = setup_logger(__name__)

def clear_all_data():
    """全テーブルのデータを削除"""
    load_dotenv()
    with Database() as db:
        # 外部キー制約を一時的に無効化
        db.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        
        # 各テーブルを空にする
        tables = [
            "crawler_accounts",
            "favorite_users",
            "video_heavy_raw_data",
            "video_light_raw_data",
        ]
        
        for table in tables:
            db.execute_query(f"TRUNCATE TABLE {table}")
            logger.info(f"テーブル {table} のデータを削除しました")
            
        # 外部キー制約を再度有効化
        db.execute_query("SET FOREIGN_KEY_CHECKS = 1")
        logger.info("全テーブルのデータ削除が完了しました")

if __name__ == "__main__":
    clear_all_data()
