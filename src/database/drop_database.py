import mysql.connector
from mysql.connector import Error
from ..config import DB_CONFIG
from ..logger import setup_logger

logger = setup_logger(__name__)

def drop_database():
    """データベースを削除する"""
    try:
        # DB_CONFIGからdatabase設定を除外してコピー
        config = DB_CONFIG.copy()
        database_name = config.pop('database')
        
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # データベースを削除
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        logger.info(f"データベース {database_name} を削除しました")
        
    except Error as e:
        logger.error(f"データベース削除エラー: {e}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == '__main__':
    drop_database()
