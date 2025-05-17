from .database import Database
from ..config import DB_CONFIG
from ..logger import setup_logger

logger = setup_logger(__name__)

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS crawler_accounts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL,
        proxy VARCHAR(255),
        is_alive BOOLEAN NOT NULL DEFAULT TRUE,
        last_crawled_at DATETIME,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_is_alive (is_alive),
        INDEX idx_last_crawled_at (last_crawled_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
CREATE TABLE account_list (
  id INT NOT NULL AUTO_INCREMENT,
  account_url VARCHAR(255) DEFAULT NULL,
  favorite_user_username VARCHAR(255) DEFAULT NULL,
  is_new_account TINYINT(1) DEFAULT NULL,
  last_crawled_at DATETIME DEFAULT NULL,
  account_type VARCHAR(255) DEFAULT NULL,
  favorite_user_is_alive TINYINT(1) NOT NULL DEFAULT 1,
  crawler_account_id INT DEFAULT NULL,
  crawl_priority INT NOT NULL DEFAULT 10,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY account_url (account_url),
  KEY idx_account_url (account_url),
  KEY idx_is_new_account (is_new_account),
  KEY idx_username (favorite_user_username),
  KEY idx_crawler_account (crawler_account_id),
  KEY idx_is_alive (favorite_user_is_alive),
  KEY idx_priority_last_crawled (crawl_priority, last_crawled_at),
  CONSTRAINT fk_crawler_account_id
    FOREIGN KEY (crawler_account_id)
    REFERENCES crawler_accounts (id)
)
ENGINE = InnoDB
AUTO_INCREMENT = 31396
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

    """,
    """
    CREATE TABLE IF NOT EXISTS video_heavy_raw_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        video_url TEXT NOT NULL,
        video_id VARCHAR(255) NOT NULL,
        user_username VARCHAR(255) NOT NULL,
        user_nickname VARCHAR(255) NOT NULL,
        video_thumbnail_url TEXT NOT NULL,
        video_title TEXT NOT NULL,
        post_time_text VARCHAR(255),
        post_time DATETIME,
        audio_url VARCHAR(255),
        audio_info_text VARCHAR(255),
        audio_id VARCHAR(255),
        audio_title VARCHAR(255),
        audio_author_name VARCHAR(255),
        play_count_text VARCHAR(255),
        play_count INT,
        like_count_text VARCHAR(255),
        like_count INT,
        comment_count_text VARCHAR(255),
        comment_count INT,
        collect_count_text VARCHAR(255),
        collect_count INT,
        share_count_text VARCHAR(255),
        share_count INT,
        crawled_at DATETIME NOT NULL,
        crawling_algorithm VARCHAR(50) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_video_id (video_id),
        INDEX idx_user_username (user_username),
        INDEX idx_post_time (post_time),
        INDEX idx_crawled_at (crawled_at),
        INDEX idx_algorithm (crawling_algorithm)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS video_light_raw_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        video_url TEXT NOT NULL,
        video_id VARCHAR(255) NOT NULL,
        user_username VARCHAR(255) NOT NULL,
        video_thumbnail_url TEXT,
        video_alt_info_text TEXT,
        play_count_text VARCHAR(255),
        play_count INT,
        like_count_text VARCHAR(255),
        like_count INT,
        crawled_at DATETIME NOT NULL,
        crawling_algorithm VARCHAR(50) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_video_id (video_id),
        INDEX idx_user_username (user_username),
        INDEX idx_crawled_at (crawled_at),
        INDEX idx_algorithm (crawling_algorithm)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS video_play_count_raw_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        video_url TEXT NOT NULL,
        video_id VARCHAR(255) NOT NULL,
        user_username VARCHAR(255) NOT NULL,
        play_count_text VARCHAR(255),
        play_count INT,
        crawled_at DATETIME NOT NULL,
        crawling_algorithm VARCHAR(50) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_video_id (video_id),
        INDEX idx_user_username (user_username),
        INDEX idx_crawled_at (crawled_at),
        INDEX idx_algorithm (crawling_algorithm)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
]

def create_database():
    """データベースを作成する"""
    # DB_CONFIGからdatabase設定を除外してコピー
    config = DB_CONFIG.copy()
    database_name = config.pop('database')
    
    with Database(config=config) as db:
        # データベースが存在しない場合は作成
        db.execute_query(f"CREATE DATABASE IF NOT EXISTS {database_name} "
                      f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        logger.info(f"データベース {database_name} を作成しました")

def create_tables():
    """テーブルを作成する"""
    with Database() as db:
        for create_table_sql in CREATE_TABLES_SQL:
            db.execute_query(create_table_sql)
            logger.info(f"テーブルを作成しました: {create_table_sql.split('CREATE TABLE IF NOT EXISTS')[1].split('(')[0].strip()}")
        logger.info("全てのテーブルの作成が完了しました")
        

def init_database():
    """データベースとテーブルを初期化する"""
    create_database()
    create_tables()

if __name__ == '__main__':
    init_database()
