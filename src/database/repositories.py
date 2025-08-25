from datetime import datetime
from typing import List, Optional, Set, Dict, Tuple
from .database import Database
from .models import CrawlerAccount, FavoriteUser, VideoHeavyRawData, VideoLightRawData, VideoPlayCountRawData
from ..logger import setup_logger

logger = setup_logger(__name__)

class CrawlerAccountRepository:
    def __init__(self, db: Database):
        self.db = db

    def get_an_available_crawler_account(self) -> Optional[CrawlerAccount]:
        """利用可能なクローラーアカウントを1つ取得(使ってない順)"""
        query = """
            SELECT id, username, password, proxy, is_alive, last_crawled_at
            FROM crawler_accounts
            WHERE is_alive = TRUE
            ORDER BY 
                CASE 
                    WHEN last_crawled_at IS NULL THEN 1
                    ELSE 0
                END DESC,
                last_crawled_at ASC
            LIMIT 1
        """
        cursor = self.db.execute_query(query)
        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        return CrawlerAccount(
            id=row[0],
            username=row[1],
            password=row[2],
            proxy=row[3],
            is_alive=row[4],
            last_crawled_at=row[5]
        )

    def update_crawler_account_last_crawled(self, crawler_account_id: int, last_crawled_at: datetime):
        """クローラーアカウントの最終クロール時間を更新"""
        query = """
            UPDATE crawler_accounts
            SET last_crawled_at = %s
            WHERE id = %s
        """
        self.db.execute_query(query, (last_crawled_at, crawler_account_id))

    def update_play_count_crawler_account_last_crawled(self, crawler_account_id: int, last_crawled_at: datetime):
        """再生数クローラーアカウントの最終クロール時間を更新"""
        query = """
            UPDATE play_count_crawler_accounts
            SET last_crawled_at = %s
            WHERE id = %s
        """
        self.db.execute_query(query, (last_crawled_at, crawler_account_id))

    def get_crawler_account_by_id(self, crawler_account_id: int) -> Optional[CrawlerAccount]:
        """指定されたIDのクローラーアカウントを取得する
        
        Args:
            crawler_account_id: 取得するクローラーアカウントのID
        
        Returns:
            CrawlerAccountオブジェクト。見つからない場合はNone。
        """
        query = """
            SELECT id, username, password, proxy, is_alive, last_crawled_at
            FROM crawler_accounts
            WHERE id = %s
            AND is_alive = TRUE
            LIMIT 1
        """
        cursor = self.db.execute_query(query, (crawler_account_id,))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        return CrawlerAccount(
            id=row[0],
            username=row[1],
            password=row[2],
            proxy=row[3],
            is_alive=row[4],
            last_crawled_at=row[5]
        )

    def get_play_count_crawler_account(self, play_count_crawler_id: int) -> Optional[CrawlerAccount]:
        """指定されたplay_count_crawler_idを持つクローラーアカウントを取得する"""
        query = """
            SELECT id, username, password, proxy, is_alive, last_crawled_at
            FROM play_count_crawler_accounts
            WHERE id = %s
            AND is_alive = TRUE
            LIMIT 1
        """
        cursor = self.db.execute_query(query, (play_count_crawler_id,))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        return CrawlerAccount(
            id=row[0],
            username=row[1],
            password=row[2],
            proxy=row[3],
            is_alive=row[4],
            last_crawled_at=row[5]
        )

class FavoriteUserRepository:
    def __init__(self, db: Database):
        self.db = db

    def get_favorite_users(self, crawler_account_id: int, limit: int = 200) -> List[FavoriteUser]:
        """クロール対象のお気に入りアカウントを取得"""
        query = """
            SELECT id, favorite_user_username, crawler_account_id,
                   favorite_user_is_alive, crawl_priority, last_crawled_at, is_new_account,nickname,
                   play_count_crawler_id
            FROM account_list
            WHERE crawler_account_id = %s
            AND favorite_user_is_alive = TRUE
            ORDER BY 
                CASE 
                    WHEN last_crawled_at IS NULL THEN 1
                    ELSE 0
                END DESC,
                crawl_priority DESC,
                last_crawled_at ASC
            LIMIT %s
        """
        cursor = self.db.execute_query(query, (crawler_account_id, limit))
        rows = cursor.fetchall()
        cursor.close()

        return [
            FavoriteUser(
                id=row[0],
                favorite_user_username=row[1],
                crawler_account_id=row[2],
                favorite_user_is_alive=row[3],
                crawl_priority=row[4],
                last_crawled_at=row[5],
                is_new_account=row[6],
                nickname=row[7],
                play_count_crawler_id=row[8]
            )
            for row in rows
        ]
    
    def get_favorite_users_by_play_count_crawler_id(self, play_count_crawler_id: int, limit: int = 1000) -> List[FavoriteUser]:
        """クロール対象のお気に入りアカウントを取得"""
        query = """
            SELECT id, favorite_user_username, crawler_account_id,
                   favorite_user_is_alive, crawl_priority, last_crawled_at, is_new_account,nickname,
                   play_count_crawler_id
            FROM account_list
            WHERE play_count_crawler_id = %s
            AND favorite_user_is_alive = TRUE
            ORDER BY 
                CASE 
                    WHEN last_crawled_at IS NULL THEN 1
                    ELSE 0
                END DESC,
                crawl_priority DESC,
                last_crawled_at ASC
            LIMIT %s
        """
        cursor = self.db.execute_query(query, (play_count_crawler_id, limit))
        rows = cursor.fetchall()
        cursor.close()

        return [
            FavoriteUser(
                id=row[0],
                favorite_user_username=row[1],
                crawler_account_id=row[2],
                favorite_user_is_alive=row[3],
                crawl_priority=row[4],
                last_crawled_at=row[5],
                is_new_account=row[6],
                nickname=row[7],
                play_count_crawler_id=row[8]
            )
            for row in rows
        ]

    def save_favorite_user_nickname(self, user_username: str, user_nickname: str):
        """お気に入りアカウントのニックネームを保存"""
        query = """
            INSERT INTO account_list (favorite_user_username, nickname)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                nickname = VALUES(nickname)
        """
        self.db.execute_query(query, (user_username, user_nickname))

    def update_favorite_user_last_crawled(self, username: str, last_crawled_at: datetime):
        """お気に入りアカウントの最終クロール時間を更新"""
        query = """
            UPDATE account_list
            SET last_crawled_at = %s
            WHERE favorite_user_username = %s
        """
        self.db.execute_query(query, (last_crawled_at, username))

    def update_favorite_user_is_alive(self, username: str, is_alive: bool):
        """お気に入りアカウントの生存状態を更新
        
        Args:
            username: 更新対象のアカウントのユーザー名
            is_alive: アカウントが存在するかどうか
        """
        query = """
            UPDATE account_list
            SET favorite_user_is_alive = %s
            WHERE favorite_user_username = %s
        """
        self.db.execute_query(query, (is_alive, username))

    def update_favorite_user_is_new_account(self, username: str, is_new_account: bool):
        """お気に入りアカウントの新規アカウントフラグを更新
        
        Args:
            username: 更新対象のアカウントのユーザー名
            is_new_account: 新規アカウントかどうか
        """
        query = """
            UPDATE account_list
            SET is_new_account = %s
            WHERE favorite_user_username = %s
        """
        self.db.execute_query(query, (is_new_account, username))

    def upsert_account_follower_history(self, account_id, collection_date, follower_text, follower_count):
        """
        account_follower_history へ当日分をUPSERT
        """
        query = """
            INSERT INTO account_follower_history (
                account_id, collection_date, follower_text, follower_count
            ) VALUES (
                %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                follower_text = VALUES(follower_text),
                follower_count = VALUES(follower_count)
        """
        self.db.execute_query(query, (account_id, collection_date, follower_text, follower_count))


class VideoRepository:
    def __init__(self, db: Database):
        self.db = db

    def save_video_heavy_data(self, data: VideoHeavyRawData):
        """動画の詳細情報を保存"""
        query = """
            INSERT INTO video_heavy_raw_data (
                video_id, video_url, video_thumbnail_url, video_title,
                user_username, user_nickname, post_time_text, post_time,
                audio_info_text, audio_id, audio_title, audio_author_name,
                like_count_text, like_count,
                comment_count_text, comment_count, collect_count_text, collect_count,
                share_count_text, share_count, crawling_algorithm, crawled_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                video_url = VALUES(video_url),
                video_thumbnail_url = VALUES(video_thumbnail_url),
                video_title = VALUES(video_title),
                user_username = VALUES(user_username),
                user_nickname = VALUES(user_nickname),
                post_time_text = VALUES(post_time_text),
                post_time = VALUES(post_time),
                audio_info_text = VALUES(audio_info_text),
                audio_id = VALUES(audio_id),
                audio_title = VALUES(audio_title),
                audio_author_name = VALUES(audio_author_name),
                like_count_text = VALUES(like_count_text),
                like_count = VALUES(like_count),
                comment_count_text = VALUES(comment_count_text),
                comment_count = VALUES(comment_count),
                collect_count_text = VALUES(collect_count_text),
                collect_count = VALUES(collect_count),
                share_count_text = VALUES(share_count_text),
                share_count = VALUES(share_count),
                crawling_algorithm = VALUES(crawling_algorithm),
                crawled_at = VALUES(crawled_at)
        """
        self.db.execute_query(query, (
            data.video_id, data.video_url, data.video_thumbnail_url, data.video_title,
            data.user_username, data.user_nickname, data.post_time_text, data.post_time,
            data.audio_info_text, data.audio_id, data.audio_title, data.audio_author_name,
            data.like_count_text, data.like_count,
            data.comment_count_text, data.comment_count, data.collect_count_text, data.collect_count,
            data.share_count_text, data.share_count, data.crawling_algorithm, data.crawled_at
        ))

    def save_video_light_data(self, data: VideoLightRawData):
        """動画の基本情報を保存"""
        query = """
            INSERT INTO video_light_raw_data (
                video_url, video_id, user_username,
                video_thumbnail_url, video_alt_info_text,
                like_count_text, like_count,
                crawling_algorithm, crawled_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                video_url = VALUES(video_url),
                video_id = VALUES(video_id),
                user_username = VALUES(user_username),
                video_thumbnail_url = VALUES(video_thumbnail_url),
                video_alt_info_text = VALUES(video_alt_info_text),
                like_count_text = VALUES(like_count_text),
                like_count = VALUES(like_count),
                crawling_algorithm = VALUES(crawling_algorithm),
                crawled_at = VALUES(crawled_at)
        """
        self.db.execute_query(query, (
            data.video_url, data.video_id, data.user_username,
            data.video_thumbnail_url, data.video_alt_info_text,
            data.like_count_text, data.like_count,
            data.crawling_algorithm, data.crawled_at
        ))

    def save_video_play_count_data(self, data: VideoPlayCountRawData):
        """動画の再生数の軽いデータを保存"""
        query = """
            INSERT INTO video_play_count_raw_data (
                video_url, video_id, user_username, play_count_text, play_count, crawling_algorithm, crawled_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                play_count_text = VALUES(play_count_text),
                play_count = VALUES(play_count),
                crawling_algorithm = VALUES(crawling_algorithm),
                crawled_at = VALUES(crawled_at)
        """ 
        self.db.execute_query(query, (
            data.video_url, data.video_id, data.user_username, data.play_count_text, data.play_count,
            data.crawling_algorithm, data.crawled_at
        ))
    

    def get_existing_heavy_data_video_ids(self, user_username: str) -> Set[str]:
        """指定されたユーザーの動画のうち重いデータを既に取ってあるものの動画IDの集合を取得"""
        query = "SELECT video_id FROM video_heavy_raw_data WHERE user_username = %s"
        cursor = self.db.execute_query(query, (user_username,))
        rows = cursor.fetchall()
        cursor.close()
        return {row[0] for row in rows}

    def get_videos_needing_update(self, user_username: str) -> List[Dict[str, str]]:
        """指定されたユーザーの動画のうち、needs_update=1のものの動画URLとサムネイルURLを取得

        Args:
            user_username: 対象ユーザーのユーザー名

        Returns:
            動画URLとサムネイルURLの辞書のリスト
        """
        query = """
            SELECT video_url, video_thumbnail_url,video_alt_info_text
            FROM video_light_raw_data
            WHERE user_username = %s
            AND needs_update = 1
            AND is_alive = 1
        """
        cursor = self.db.execute_query(query, (user_username,))
        rows = cursor.fetchall()
        cursor.close()
        
        return [
            {
                "video_url": row[0],
                "video_thumbnail_url": row[1],
                "video_alt_info_text": row[2]
            }
            for row in rows
        ]
    
    def update_video_light_data_is_alive(self, video_id: str, is_alive: bool):
        """動画の生存状態を更新"""
        query = """
            UPDATE video_light_raw_data
            SET is_alive = %s
            WHERE video_id = %s
        """
        self.db.execute_query(query, (is_alive, video_id))
