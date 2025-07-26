from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime, timedelta
import random
import time
from typing import Optional, List, Dict, Tuple
import json
import os
from ..database.models import CrawlerAccount, FavoriteUser, VideoHeavyRawData, VideoLightRawData, VideoPlayCountRawData
from ..database.repositories import CrawlerAccountRepository, FavoriteUserRepository, VideoRepository
from ..database.database import Database
from .selenium_manager import SeleniumManager
from ..logger import setup_logger
import grpc
from google.cloud import pubsub_v1
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.actions.pointer_input import PointerInput
from selenium.webdriver.common.actions import interaction

logger = setup_logger(__name__)
pubsub_emulator_host = os.getenv('PUBSUB_EMULATOR_HOST')
project_id = os.getenv('PROJECT_ID') 



def extract_thumbnail_essence(thumbnail_url: str) -> str:
    """サムネイルURLから一意な識別子を抽出する
    例: https://p19-sign.tiktokcdn-us.com/obj/tos-useast5-p-0068-tx/oMnASW5J5CYEMAiRxDhIPnOAAfE1gGfD1UiBia?lk3s=81f88b70&...
    → oMnASW5J5CYEMAiRxDhIPnOAAfE1gGfD1UiBia
    """
    try:
        path = thumbnail_url.split("?")[0]
        file_name = path.split("/")[-1]
        file_name = file_name.split(".")[0]
        file_name = file_name.split("~")[0]
        return file_name
    except Exception:
        logger.warning(f"サムネイルURLからエッセンスの抽出に失敗: {thumbnail_url}", exc_info=True)
        return thumbnail_url  # 失敗した場合は元のURLを返す


def parse_tiktok_time(time_text: str, base_time: datetime) -> Optional[datetime]:
    """投稿時間のテキストを解析する
    
    Args:
        time_text: 解析する時間文字列 (e.g. "3-25", "1日前", "2時間前")
        base_time: 基準となる時刻
    
    Returns:
        解析結果の日時。解析できない場合はNone。
    """
    if not time_text:
        return None

    try:
        if time_text.endswith("秒前"):
            seconds = int(time_text.replace("秒前", ""))
            return base_time - timedelta(seconds=seconds)

        if time_text.endswith("分前"):
            minutes = int(time_text.replace("分前", ""))
            return base_time - timedelta(minutes=minutes)

        if time_text.endswith("時間前"):
            hours = int(time_text.replace("時間前", ""))
            return base_time - timedelta(hours=hours)

        if time_text.endswith("日前"):
            days = int(time_text.replace("日前", ""))
            return base_time - timedelta(days=days)

        if time_text.endswith("1週間前"):
            days = 7
            return base_time - timedelta(days=days)
        # 「M-D」形式の場合
        if "-" in time_text and len(time_text.split("-")) == 2:
            month, day = map(int, time_text.split("-"))
            year = base_time.year
            # 月が現在より大きい場合は前年と判断
            if month > base_time.month:
                year -= 1
            return datetime(year, month, day,
                          base_time.hour, base_time.minute,
                          tzinfo=base_time.tzinfo)

        # 「YYYY-MM-DD」形式の場合
        if "-" in time_text and len(time_text.split("-")) == 3:
            year, month, day = map(int, time_text.split("-"))
            return datetime(year, month, day,
                          base_time.hour, base_time.minute,
                          tzinfo=base_time.tzinfo)

        return None

    except:
        logger.warning(f"日付文字列の解析に失敗: {time_text}", exc_info=True)
        return None


def parse_tiktok_video_url(url: str) -> Tuple[str, str]: # NOT NULL なのでエラー起きたらエラー投げる
    """TikTokのURLからvideo_idとuser_usernameを抽出する
    
    Args:
        url: 解析するURL (e.g. "https://www.tiktok.com/@username/video/1234567890")
    
    Returns:
        (video_id, user_username)のタプル。
        例: ("1234567890", "username")
    """
    try:
        path = url.split("?")[0]
        parts = path.split("/")
        video_id = parts[-1]
        user_username = parts[-3].strip("@")
        return video_id, user_username
    
    except Exception:
        logger.exception(f"URLの解析に失敗: {url}")
        raise


def parse_tiktok_number(text: str) -> Optional[int]:
    """TikTok形式の数値文字列を解析する
    
    Args:
        text: 解析する文字列 (e.g. "1,234", "1.5K", "3.78M")
    
    Returns:
        解析結果の整数値。解析できない場合はNone。
        "1,234" -> 1234
        "1.5K" -> 1500
        "3.78M" -> 3780000
    """
    if not text:
        return None

    try:
        # カンマを削除
        text = text.replace(",", "")

        # 単位がない場合はそのまま整数変換
        if text.replace(".", "").isdigit():
            return int(float(text))

        # 単位ごとの倍率
        multipliers = {
            "K": 1000,       # 千
            "M": 1000000,    # 百万
            "G": 1000000000, # 十億
            "B": 1000000000  # 十億
        }

        # 最後の文字を単位として取得
        unit = text[-1].upper()
        if unit in multipliers:
            # 数値部分を取得して浮動小数点に変換
            number = float(text[:-1])
            # 倍率をかけて整数化
            return int(number * multipliers[unit])
        
        raise ValueError(f"数値文字列の解析に失敗: {text}")

    except:
        logger.warning(f"数値文字列の解析に失敗: {text}")
        return None


class TikTokCrawler:
    BASE_URL = "https://www.tiktok.com"
    
    # TikTokクローラーの初期化
    # Args:
    #     crawler_account_repo: クローラーアカウントリポジトリ
    #     favorite_user_repo: お気に入りユーザーリポジトリ
    #     video_repo: 動画リポジトリ
    #     crawler_account_id: 使用するクローラーアカウントのID（Noneの場合は自動選択）
    #     sadcaptcha_api_key: SADCAPTCHA APIキー
    # Returns:
    #     TikTokCrawlerインスタンス
    def __init__(self, crawler_account_repo: CrawlerAccountRepository,
                 favorite_user_repo: FavoriteUserRepository,
                 video_repo: VideoRepository,
                 crawler_account_id: Optional[int] = None,
                 sadcaptcha_api_key: Optional[str] = None,
                 engagement_type: str = "like",
                 device_type: str = "pc"):
        self.crawler_account_repo = crawler_account_repo
        self.favorite_user_repo = favorite_user_repo
        self.video_repo = video_repo
        self.crawler_account_id = crawler_account_id
        self.crawler_account: Optional[CrawlerAccount] = None
        self.selenium_manager = None
        self.driver = None
        self.wait = None
        self.sadcaptcha_api_key = "fd31d51515ed18cadec7d4a522894997"
        self.login_restart_attempted = False        # ← 追加
        self.engagement_type = engagement_type
        self.device_type = device_type
    def __enter__(self):
        # クローラーアカウントを取得
        if self.crawler_account_id is not None:
            if self.engagement_type == "play":
                # 再生数クロールの場合は、指定されたIDをplay_count_crawler_idとして使用
                self.crawler_account = self.crawler_account_repo.get_play_count_crawler_account(self.crawler_account_id)
            else:
                # 通常のクロールの場合
                self.crawler_account = self.crawler_account_repo.get_crawler_account_by_id(self.crawler_account_id)
            
            if not self.crawler_account:
                account_type = "play_count_crawler_id" if self.engagement_type == "play" else "id"
                raise Exception(f"指定されたクローラーアカウント（{account_type}: {self.crawler_account_id}）が見つかりません")
        else:
            self.crawler_account = self.crawler_account_repo.get_an_available_crawler_account()
            if not self.crawler_account:
                raise Exception("利用可能なクローラーアカウントがありません")
        
        # Seleniumの設定
        self.selenium_manager = SeleniumManager(self.crawler_account.proxy, self.sadcaptcha_api_key, self.device_type)
        self.driver = self.selenium_manager.setup_driver()
        self.wait = WebDriverWait(self.driver, 15)  # タイムアウトを15秒に変更

        # ログイン
        self._login()

        # 最終クロール時間を更新（engagement_typeに応じて適切なテーブルを更新）
        if self.engagement_type == "play":
            self.crawler_account_repo.update_play_count_crawler_account_last_crawled(
                self.crawler_account.id,
                datetime.now()
            )
        else:
            self.crawler_account_repo.update_crawler_account_last_crawled(
                self.crawler_account.id,
                datetime.now()
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.selenium_manager:
            self.selenium_manager.quit_driver()
            

    def _random_sleep(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        time.sleep(random.uniform(min_seconds, max_seconds))

    def _check_and_handle_captcha(self):
        """CAPTCHAをチェックして処理する"""
        if self.selenium_manager.check_and_solve_captcha():
            self._random_sleep(2.0, 4.0)  # CAPTCHA解決後の待機
            return True
        return False

    # クロール用アカウントself.crawler_accountでTikTokにログインする
    def _login(self):
        logger.info(f"クロール用アカウント{self.crawler_account.username}でTikTokにログイン中...")
        self.driver.get(f"{self.BASE_URL}/login/phone-or-email/email")
        
        
        self._random_sleep(2.0, 4.0)

        # ログインフォームの要素を待機
        username_input = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))
        )
        self._random_sleep(1.0, 2.0)

        # メールアドレスを入力
        username_input.send_keys(self.crawler_account.username)
        self._random_sleep(1.5, 2.5)

        # パスワード入力欄を探す
        password_input = self.driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        password_input.send_keys(self.crawler_account.password)
        self._random_sleep(1.0, 2.0)

        # ログインボタンを探してクリック
        login_button = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
        )
        self._random_sleep(5.0, 7.0)
        login_button.click()

        # CAPTCHAチェック
        self._check_and_handle_captcha()

        # ログイン完了を待機
        # プロフィールアイコンが表示されるまで待機（60秒待機）
        try:
            login_wait = WebDriverWait(self.driver, 60)  # 絵合わせ認証が出てきたら人力で解いてね
            login_wait.until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='profile-icon']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "button[aria-label='プロフィール']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".TUXButton[aria-label='プロフィール']"))
                )
            )
            logger.info(f"クロール用アカウント{self.crawler_account.username}でTikTokへのログインに成功しました")
        except TimeoutException:
            logger.warning("60 秒以内にプロフィールアイコンが見つかりません。CAPTCHA を再チェックします…")
            # CAPTCHA を再確認してもう一度だけ待つ
            self._check_and_handle_captcha()
            try:
                WebDriverWait(self.driver, 60).until(   # 2 回目。失敗すれば例外を上げて上位で処理
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='profile-icon']"))
                )

                logger.info(f"クロール用アカウント{self.crawler_account.username}でTikTokへのログインに成功しました")
            except TimeoutException:
                if not self.login_restart_attempted:
                    logger.warning("再ログインのためブラウザを再起動します…")
                    self.login_restart_attempted = True

                    # ドライバを閉じて新規起動
                    self.selenium_manager.quit_driver()
                    self.driver = self.selenium_manager.setup_driver()
                    self.wait   = WebDriverWait(self.driver, 15)

                    return self._login()           # ← ここで再ログイン
                else:
                    # それでもダメなら例外を上へ
                    raise
    # TikTokユーザーが見つからない（アカウントが削除されている等）場合の例外
    # ユーザー単位の関数で最も大きいところで処置完了するように設計しましょう
    class TikTokUserNotFoundException(Exception):
        pass

    # TikTok動画が見つからない（動画が削除されている等）場合の例外
    # 動画単位の関数で最も大きいところで処置完了するように設計しましょう
    class TikTokVideoNotFoundException(Exception):
        pass

    # ユーザーページに移動する
    # Condition: 自由
    # Args:
    #     username: ユーザー名
    #     device_type: デバイスタイプ（"pc" または "mobile"）
    def navigate_to_user_page(self, username: str):
        logger.debug(f"ユーザー @{username} のページに移動中...")
        self.driver.get(f"{self.BASE_URL}/@{username}")
        self.driver.execute_script("document.body.style.width=''")   # ← 各ページ後に一発
        self._random_sleep(2.0, 4.0)
        

        # user-page要素の存在を待機
        self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='user-page']"))
        )

        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='user-post-item'],div[data-e2e='user-item-list']"))
            )
        
        except TimeoutException:
            try:
                # アカウント削除確認用の要素を探す
                error_container = self.driver.find_element(By.CSS_SELECTOR, "div[class*='-DivErrorContainer']")
                error_text = error_container.find_element(By.CSS_SELECTOR, "p[class*='-PTitle']").text

                if error_text == "このアカウントは見つかりませんでした":
                    logger.info(f"ユーザー @{username} は削除されたようです。データベースのis_aliveをFalseに更新します。")
                    self.favorite_user_repo.update_favorite_user_is_alive(username, False)
                    raise self.TikTokUserNotFoundException(f"ユーザー @{username} は存在しません")
                elif error_text == "このアカウントは非公開です":
                    logger.info(f"ユーザー @{username} は非公開アカウントです。データベースのis_aliveをFalseに更新します。")
                    self.favorite_user_repo.update_favorite_user_is_alive(username, False)
                    raise self.TikTokUserNotFoundException(f"ユーザー @{username} は非公開アカウントです")
                elif "コンテンツはありません" in error_text:
                    logger.info(f"ユーザー @{username} はコンテンツのないアカウントです。処理を続行します。")
                    # コンテンツがない場合は処理を続行する（例外を発生させない）
            except NoSuchElementException:
                # 削除確認要素や非公開確認要素が見つからない場合は正常なユーザーページとして処理を続行
                pass        
        # ユーザーページの読み込みを確認
        logger.debug(f"ユーザー @{username} のページに移動しました")
            
    # ユーザーページをスクロールする
    # Condition: ユーザーページが開かれていること
    # Args:
    #     need_items_count: 目標の画像要素数
    #     max_scroll_attempts: 最大スクロール回数
    # Returns: 目標の画像要素数に達したかどうか
    # やたら丁寧な実装になっている理由は、そうしないとちゃんとサムネがロードされずthumbnail_urlがこんなふうになるから
    #   "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
    def scroll_user_page(self, need_items_count: int = 100, max_scroll_attempts: int = None) -> bool:
        logger.debug(f"{need_items_count}件の画像要素を目標にユーザーページをスクロールします...")

        last_load_failed = False
        for _ in range(max_scroll_attempts or need_items_count // 2):
            self._random_sleep(1.0, 2.0)
            # 現在の要素と画像の数を取得
            current_items = self.driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='user-post-item'],div[data-e2e='video-item']")
            current_items_count = len(current_items)
            current_images_count = len([item for item in current_items
                if item.find_elements(By.CSS_SELECTOR, "img[src*='tiktokcdn']")])

            # 必要な数に達したら終了
            logger.debug(f"{current_items_count}件の画像要素を確認しました。")
            if current_items_count >= need_items_count:
                logger.debug(f"目標以上の{current_items_count}件の画像要素を確認しました。スクロールを完了します。")
                return True
            
            # スクロール処理をデバイスタイプに応じて分ける
            try:
                # PC向けのスクロール
                final_target_scroll = self.driver.execute_script("return document.body.scrollHeight - 200;")
                current_scroll = self.driver.execute_script("return window.pageYOffset;")
                scroll_step = (final_target_scroll - current_scroll) / 3
                
                for i in range(3):
                    target_scroll = current_scroll + scroll_step * (i + 1)
                    self.driver.execute_script(f"window.scrollTo({{top: {target_scroll}, left: 0, behavior: 'smooth'}});")
                    self._random_sleep(0.3, 0.7)
            
            except Exception as e:
                logger.debug(f"スクロール中にエラーが発生しました: {e}")
                logger.debug(f"これ以上スクロールできません。スクロールを中止します。{current_items_count}件の画像要素を確認しました")
                return False
            
            try:
                wait = WebDriverWait(self.driver, 10)
                #新しい要素がロードされるまで待機
                wait.until(lambda driver: len(driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='user-post-item'],div[data-e2e='video-item']")) > current_items_count)
                
                # 新しい画像がロードされるまで待機
                wait.until(lambda driver: len([
                    item for item in driver.find_elements(By.CSS_SELECTOR, "div[data-e2e='user-post-item'],div[data-e2e='video-item']")
                    if item.find_elements(By.CSS_SELECTOR, "img[src*='tiktokcdn']")
                ]) > current_images_count)
                last_load_failed = False
                
            except TimeoutException:
                if last_load_failed:
                    logger.warning(f"新しい画像要素のロードが2回続けてタイムアウトしました。{current_items_count}件の画像要素を確認しました。スクロールを中止します。", exc_info=True)
                    return False
                logger.warning(f"新しい画像要素のロードがタイムアウトしました。{current_items_count}件の画像要素を確認しました。もう1度だけスクロールを試みます。")
                last_load_failed = True
                
        logger.debug(f"スクロール回数の上限に達しました。{current_items_count}件の画像要素を確認しました。スクロールを完了します。")
        return True

    # 動画の軽いデータを取得する
    # Condition: ユーザーページが開かれていること
    # Args:
    #     max_videos: 取得する動画の最大数
    # Returns: 動画の軽いデータの前半(辞書型)のリスト
    def get_video_light_like_datas_from_user_page(self, max_videos: int = 100) -> List[Dict[str, str]]:
        logger.debug(f"動画の軽いデータの前半を取得中...")
        video_stats = []
        self._random_sleep(30.0, 35.0)
        
        self.scroll_user_page(max_videos)
        video_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-e2e='user-post-item']")

        logger.debug(f"動画の軽いデータの前半を求めて{len(video_elements[:max_videos])}本の動画要素を走査します")
        for video_element in video_elements[:max_videos]:
            try:
                # 動画のURLを取得
                video_link = video_element.find_element(By.TAG_NAME, "a")
                video_url = video_link.get_attribute("href")

                # URLからvideo_idとuser_usernameを抽出
                video_id, user_username = parse_tiktok_video_url(video_url)
                
                # サムネイル画像と動画の代替テキストを取得
                thumbnail_element = video_element.find_element(By.CSS_SELECTOR, "img")
                thumbnail_url = thumbnail_element.get_attribute("src")
                video_alt_info_text = thumbnail_element.get_attribute("alt")
                
                # いいね数を取得（表示形式のまま）
                like_count_element = video_element.find_element(By.CSS_SELECTOR, "[data-e2e='video-views']") # video-viewsといいながらいいね数
                like_count_text = like_count_element.text
                
                video_stats.append({
                    "video_url": video_url,
                    "video_id": video_id,
                    "user_username": user_username,
                    "video_thumbnail_url": thumbnail_url,
                    "video_alt_info_text": video_alt_info_text,
                    "like_count_text": like_count_text,
                    "crawling_algorithm": "selenium-human-like-1"
                })
            
            except NoSuchElementException:
                logger.warning(f"動画の軽いデータの前半の取得のうち1件に失敗", exc_info=True)
                continue
        
        logger.debug(f"動画の軽いデータの前半を取得しました: {len(video_stats)}件")
        return video_stats
    
    # 動画の再生数の軽いデータを取得する
    # Condition: ユーザーページが開かれていること
    # Args:
    #     max_videos: 取得する動画の最大数
    # Returns: 動画の再生数の軽いデータの前半(辞書型)のリスト
    def get_video_play_count_datas_from_user_page(self, max_videos: int = 100) -> List[Dict[str, str]]:
        logger.debug(f"動画の再生数の軽いデータの前半を取得中...")
        video_stats = []
        self._random_sleep(15.0, 20.0)
        
        self.scroll_user_page(max_videos)
        video_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-e2e='user-post-item']")

        logger.debug(f"動画の再生数を求めて{len(video_elements[:max_videos])}本の動画要素を走査します")
        for video_element in video_elements[:max_videos]:
            try:
                # 動画のURLを取得
                video_link = video_element.find_element(By.TAG_NAME, "a")
                video_url = video_link.get_attribute("href")

                # URLからvideo_idとuser_usernameを抽出
                video_id, user_username = parse_tiktok_video_url(video_url)
                
                # いいね数を取得（表示形式のまま）
                play_count_element = video_element.find_element(By.CSS_SELECTOR, "[data-e2e='video-views']") # video-viewsといいながらいいね数
                play_count_text = play_count_element.text
                
                video_stats.append({
                    "video_url": video_url,
                    "video_id": video_id,
                    "user_username": user_username,
                    "play_count_text": play_count_text,
                    "crawling_algorithm": "selenium-human-like-1"
                })
            
            except NoSuchElementException:
                logger.warning(f"動画の軽いデータの前半の取得のうち1件に失敗", exc_info=True)
                continue
        logger.debug(f"動画の再生数を取得しました: {len(video_stats)}件")
        return video_stats
    

    # ピン留めされていない動画の中で最も新しいもののURLを取得する
    # Condition: ユーザーページが開かれていること
    # Returns: 動画のURL
    # この関数が必要な理由は、単に最も上にある動画だとピン留めされた動画の可能性があり、その場合video_page_creator_videos_tabが新着動画のそれではなくなるため。
    def get_latest_video_url_from_user_page(self) -> str:
        logger.debug(f"ピン留めされていない動画の中で最も新しいもののURLを取得中...")
        
        # 動画要素を全て取得
        video_elements = self.driver.find_elements(By.CSS_SELECTOR, "[data-e2e='user-post-item']")
        
        for video_element in video_elements:
            try:
                video_element.find_element(By.CSS_SELECTOR, "[data-e2e='video-card-badge']")
                continue
            except NoSuchElementException:
                video_url = video_element.find_element(By.TAG_NAME, "a").get_attribute("href")
                logger.debug(f"ピン留めされていない動画の中で最も新しいもののURLを取得しました: {video_url}")
                return video_url

        video_element = video_elements[0]
        video_url = video_element.find_element(By.TAG_NAME, "a").get_attribute("href")
        logger.warning(f"ピン留めされていない動画が見つかりませんでした。代わりにピン留めされている適当な動画のURLを返します。: {video_url}")
        return video_url

    # 動画ページに移動する
    # Condition: [link_should_be_in_page==Trueの場合] ユーザーページが開かれていること
    #            [link_should_be_in_page==Falseの場合] 自由
    # Args:
    #     video_url: 動画のURL
    #     link_should_be_in_page: 動画ページへのリンクがページに含まれているはずか
    def navigate_to_video_page(self, video_url: str, link_should_be_in_page: bool = True) -> bool:
        logger.debug(f"動画ページに移動中...: {video_url}")
        direct_access = False
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if link_should_be_in_page:
                    try:
                        video_link = self.driver.find_element(By.CSS_SELECTOR, f"a[href='{video_url}']")
                        self.driver.execute_script("arguments[0].click();", video_link)
                    except NoSuchElementException:
                        logger.warning(f"動画ページへのリンクが見つからなかったので直接アクセスします: {video_url}")
                        self.driver.get(video_url)
                        direct_access = True
                else:
                    self.driver.get(video_url)
                    direct_access = True
                
                # ページが読み込まれるまで十分に待機
                self._random_sleep(3.0, 5.0)
                
                # 動画削除済み確認用の要素を探す
                try:
                    error_container = self.driver.find_element(By.CSS_SELECTOR, "div[class*='-DivErrorContainer']")
                    error_text = error_container.find_element(By.CSS_SELECTOR, "p[class*='-PTitle']").text

                    if error_text == "動画は現在ご利用できません":
                        video_id, _ = parse_tiktok_video_url(video_url)
                        logger.info(f"動画 {video_url} は削除されたようです。データベースのis_aliveをFalseに更新します。")
                        self.video_repo.update_video_light_data_is_alive(video_id, False)
                        raise self.TikTokVideoNotFoundException(f"動画 {video_url} は存在しません")
                except NoSuchElementException:
                    # 削除確認要素が見つからない場合は正常な動画ページとして処理を続行
                    pass
                
                # 複数の可能性のある要素のいずれかが表示されるまで待機
                try:
                    if direct_access:
                        WebDriverWait(self.driver, 20).until(
                            EC.any_of(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='detail-video']")),
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='browse-video']")),
                                EC.presence_of_element_located((By.CSS_SELECTOR, "video")),
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='DivVideoContainer']"))
                            )
                        )
                    else:
                        WebDriverWait(self.driver, 20).until(
                            EC.any_of(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='video-desc']")),
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='user-title']")),
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='browse-username']"))
                                
                            )
                        )
                    
                    logger.debug(f"動画ページに移動しました: {video_url}")
                    return direct_access
                    
                except TimeoutException:
                    # 特定の要素が見つからなくても、ページが読み込まれているか確認
                    if "tiktok.com" in self.driver.current_url:
                        logger.warning(f"ページは読み込まれましたが、期待する要素が見つかりませんでした。処理を続行します: {video_url}")
                        return direct_access
                    raise
                
            except self.TikTokVideoNotFoundException:
                raise
            except Exception as e:
                retry_count += 1
                logger.warning(f"動画ページへの移動に失敗 (試行 {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count >= max_retries:
                    logger.error("最大試行回数に達しました。エラーを再発生させます。")
                    raise
                
                # ブラウザをリフレッシュして再試行
                self._random_sleep(5.0, 8.0)
        
        return direct_access

    # 動画ページの重いデータを取得する
    # Condition: 動画ページが開かれていること
    # Returns: 動画の重いデータ
    def get_video_heavy_data_from_video_page(self) -> Dict[str, str]:
        logger.debug(f"動画の重いデータを取得中...")
    
        video_url = self.driver.current_url
        video_title = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='browse-video-desc'],[data-e2e='video-desc']").text
        # より堅牢な投稿時間要素の取得
        try:
            # 新しい形式: StyledLinkクラスのaタグ
            post_time_element = self.driver.find_element(By.CSS_SELECTOR, "a[class*='StyledLink']")
            full_text = post_time_element.text
        except NoSuchElementException:
            try:
                # 投稿情報部分を直接取得
                post_time_element = self.driver.find_element(By.CSS_SELECTOR, "span[class*='SpanOtherInfos']")
                full_text = post_time_element.text
            except NoSuchElementException:
                try:
                    # 古い形式をフォールバック
                    post_time_element = self.driver.find_element(By.CSS_SELECTOR, "a[class*='StyledAuthorAnchor'], [data-e2e='browser-nickname'] span:last-child")
                    full_text = post_time_element.text
                except NoSuchElementException:
                    post_time_text = ""
                    full_text = ""
        # audio_url = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='browse-music'] a,[data-e2e='video-music']").get_attribute("href")
        try:
            # まず新しい形式を試す
            audio_element = self.driver.find_element(By.CSS_SELECTOR, 
                "[data-e2e='video-music']")
            audio_info_text = audio_element.get_attribute("aria-label").replace("Watch more videos with music ", "")
        except (NoSuchElementException, AttributeError):
            try:
                # 次に古い形式を試す
                audio_info_text = self.driver.find_element(By.CSS_SELECTOR, 
                    "[data-e2e='browse-music'] .css-pvx3oa-DivMusicText, [data-e2e='browse-music-title']").text
            except NoSuchElementException:
                audio_info_text = ""
        like_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='browse-like-count'],strong[data-e2e='like-count']").text
        comment_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='browse-comment-count'],strong[data-e2e='comment-count']").text
        collect_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='undefined-count']").text
        
        # 取得したテキストを処理
        full_text = post_time_element.text

        # 複数の区切りパターンに対応
        if " · " in full_text:
            # スペース付き中黒で区切られている場合
            post_time_text = full_text.split(" · ")[-1]
        elif "\n·\n" in full_text:
            # 改行付き中黒で区切られている場合
            post_time_text = full_text.split("\n·\n")[-1]
        elif "·" in full_text:
            # 単なる中黒がある場合
            post_time_text = full_text.split("·")[-1].strip()
        else:
            # 上記のいずれにも該当しない場合
            post_time_text = full_text
        logger.debug(f"動画の重いデータを取得しました: {video_url}")

        return {
            "video_url": video_url,
            "video_title": video_title,
            "post_time_text": post_time_text,
            "audio_info_text": audio_info_text,
            "like_count_text": like_count_text,
            "comment_count_text": comment_count_text,
            "collect_count_text": collect_count_text,
            "crawling_algorithm": "selenium-human-like-1"
        }
    def get_video_heavy_data_from_direct_access(self) -> Dict[str, str]:
        logger.debug(f"動画の重いデータを取得中...")
    
        video_url = self.driver.current_url
        video_title = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='browse-video-desc']").text
        
        # より堅牢な投稿時間要素の取得（direct_access版）
        try:
            # 新しい形式: StyledLinkクラスのaタグ
            post_time_element = self.driver.find_element(By.CSS_SELECTOR, "a[class*='StyledLink']")
            full_text = post_time_element.text
            # 投稿日時部分を抽出（" · "で区切る）
            if " · " in full_text:
                post_time_text = full_text.split(" · ")[-1]
            elif "·" in full_text:
                post_time_text = full_text.split("·")[-1].strip()
            else:
                post_time_text = full_text
        except NoSuchElementException:
            try:
                # 投稿情報部分を直接取得
                post_time_element = self.driver.find_element(By.CSS_SELECTOR, "span[class*='SpanOtherInfos']")
                full_text = post_time_element.text
                if " · " in full_text:
                    post_time_text = full_text.split(" · ")[-1]
                else:
                    post_time_text = full_text
            except NoSuchElementException:
                try:
                    # 古い形式をフォールバック
                    post_time_text = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='browser-nickname'] span:last-child").text
                except NoSuchElementException:
                    post_time_text = ""
        like_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='like-count']").text
        comment_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='comment-count']").text
        collect_count_text = self.driver.find_element(By.CSS_SELECTOR, "strong[data-e2e='undefined-count']").text
        
        logger.debug(f"動画の重いデータを取得しました: {video_url}")

        return {
            "video_url": video_url,
            "video_title": video_title,
            "post_time_text": post_time_text,
            "audio_info_text": audio_info_text,
            "like_count_text": like_count_text,
            "comment_count_text": comment_count_text,
            "collect_count_text": collect_count_text,
            "crawling_algorithm": "selenium-human-like-1"
        }

    # 動画ページからユーザーページに移動する
    # Condition: もともとユーザーページからのクリックで動画ページが開かれていること
    def navigate_to_user_page_from_video_page(self):
        logger.debug("動画ページの閉じるボタンをクリックしてユーザーページに戻ります...")
        
        max_retries = 3
        retry_count = 0
        current_url = self.driver.current_url
        
        # 現在のURLからユーザー名を抽出（バックアップ用）
        try:
            user_username = None
            if '@' in current_url:
                user_username = current_url.split('@')[1].split('/')[0]
            elif '/user/' in current_url:
                user_username = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='browse-username']").text.strip('@')
        except Exception:
            logger.warning("URLからユーザー名の抽出に失敗しました", exc_info=True)
        
        while retry_count < max_retries:
            try:
                # 複数の可能性のある閉じるボタンを探す
                try:
                    close_button = WebDriverWait(self.driver, 8).until(
                        EC.any_of(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, ".TUXButton[aria-label='exit']")),
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='exit']")),
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-e2e='browse-close']")),
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-e2e='browse-user-avatar']")),
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "[class*='CloseButton']")),
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='Close']"))
                        )
                    )
                    
                    # JavaScriptでのクリックを試行
                    self.driver.execute_script("arguments[0].click();", close_button)
                    self._random_sleep(2.0, 3.0)
                    
                    # ユーザーページの動画一覧が表示されるか確認
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='user-post-item']"))
                    )
                    logger.debug("ユーザーページに戻りました")
                    return
                    
                except Exception as e:
                    logger.warning(f"閉じるボタンのクリックまたはユーザーページの表示確認に失敗: {str(e)}")
                    
                    # ボタンが見つからない場合やクリックに失敗した場合は、直接URLで移動
                    if user_username:
                        self.driver.get(f"{self.BASE_URL}/@{user_username}")
                        self._random_sleep(3.0, 5.0)
                        
                        # ユーザーページの確認
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-e2e='user-page']"))
                        )
                        logger.debug(f"URLで直接ユーザーページ @{user_username} に移動しました")
                        return
                    else:
                        # ブラウザの戻るボタンを使用
                        self.driver.execute_script("window.history.go(-1)")
                        self._random_sleep(2.0, 3.0)
                        
                        # ページが変わったことを確認
                        if self.driver.current_url != current_url:
                            logger.debug("ブラウザの戻るボタンでユーザーページに移動しました")
                            return
                        raise
            
            except Exception as e:
                retry_count += 1
                logger.warning(f"ユーザーページへの移動に失敗 (試行 {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count >= max_retries:
                    # 最終手段: 直接ユーザーページへアクセス
                    if user_username:
                        logger.warning(f"最大試行回数に達しました。直接ユーザーページへアクセスします: @{user_username}")
                        self.navigate_to_user_page(user_username)
                        return
                    else:
                        logger.error("ユーザーページへの移動に失敗し、ユーザー名も取得できませんでした")
                        raise
                
                self._random_sleep(3.0, 5.0)
        
        logger.error("リトライ回数の上限に達しました")

    # 動画ページの「クリエイターの動画」タブに移動する
    # Condition: 動画ページが開かれていること
    def navigate_to_video_page_creator_videos_tab(self):
        logger.debug("動画ページの「クリエイターの動画」タブに移動中...")
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 画面が確実に読み込まれるまで少し待機
                self._random_sleep(2.0, 3.0)
                
                # 2番目のタブ（クリエイターの動画）を待機して取得
                creator_videos_tab = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "[class*='DivTabMenuContainer'] [class*='DivTabItemContainer']:nth-child(2) [class*='DivTabItem']"))
                )
                
                # JavaScriptを使用してクリック実行（より安定した方法）
                self.driver.execute_script("arguments[0].click();", creator_videos_tab)
                
                # クリック後に少し待機
                self._random_sleep(2.0, 3.0)
                
                # クリック成功を確認する要素を待機
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='DivVideoListContainer']"))
                )
                
                logger.debug("動画ページの「クリエイターの動画」タブに移動しました")
                return
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"クリエイターの動画タブへの移動に失敗 (試行 {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count >= max_retries:
                    logger.error("最大試行回数に達しました。エラーを再発生させます。")
                    raise
                
                # ページを少しリフレッシュする
                self.driver.execute_script("document.body.style.zoom='0.99'")
                self.driver.execute_script("document.body.style.zoom='1'")
                self._random_sleep(3.0, 5.0)


    # 動画ページの「クリエイターの動画」タブをスクロールする
    # Condition: 動画ページの「クリエイターの動画」タブが開かれていること
    # Args:
    #     need_items_count: 目標の画像要素数
    #     max_scroll_attempts: 最大スクロール回数
    # Returns: 目標の画像要素数か最大スクロール回数に達したかどうか
    # やたら丁寧な実装になっている理由は、そうしないとちゃんとサムネがロードされずthumbnail_urlがこんなふうになるから
    #   "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
    def scroll_video_page_creator_videos_tab(self, need_items_count: int = 100, max_scroll_attempts: int = None) -> bool:
        logger.debug(f"{need_items_count}件の画像要素を目標にクリエイターの動画タブをスクロールします...")

        last_load_failed = False
        for _ in range(max_scroll_attempts or need_items_count // 2):
            self._random_sleep(1.0, 2.0)
            # 現在の要素と画像の数を取得
            current_items = self.driver.find_elements(By.CSS_SELECTOR, "[class='css-eqiq8z-DivItemContainer eadndt66']")
            current_items_count = len(current_items)
            current_images_count = len([item for item in current_items
                if item.find_elements(By.CSS_SELECTOR, "img[src*='tiktokcdn']")])
            
            # 必要な数に達したら終了
            logger.debug(f"{current_items_count}件の画像要素を確認しました。")
            if current_items_count >= need_items_count:
                logger.debug(f"目標以上の{current_items_count}件の画像要素を確認しました。スクロールを完了します。")
                return True
            
            # スクロールを3回に分けて実行
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, "div[class*='css-1xyzrsf-DivVideoListContainer e1o3lsy81']")
                final_target_scroll = self.driver.execute_script("return arguments[0].scrollHeight - 200;", element)
                current_scroll = self.driver.execute_script("return arguments[0].scrollTop;", element)
                scroll_step = (final_target_scroll - current_scroll) / 3
                
                for i in range(3):
                    target_scroll = current_scroll + scroll_step * (i + 1)
                    self.driver.execute_script(f"arguments[0].scrollTo({{top: {target_scroll}, left: 0, behavior: 'smooth'}});", element)
                    self._random_sleep(0.3, 0.7)  # 各スクロールの間に少し待機

            except TimeoutException:
                logger.debug(f"これ以上スクロールできません。スクロールを中止します。{current_items_count}件の画像要素を確認しました")
                return False
            
            try:
                wait = WebDriverWait(self.driver, 10)
                # 新しい要素がロードされるのを待つ
                wait.until(lambda driver: len(driver.find_elements(
                    By.CSS_SELECTOR, "[class='css-eqiq8z-DivItemContainer eadndt66']")) > current_items_count)
                
                # 新しい画像がロードされるまで待機
                wait.until(lambda driver: len([
                    item for item in driver.find_elements(By.CSS_SELECTOR, "[class='css-eqiq8z-DivItemContainer eadndt66']")
                    if item.find_elements(By.CSS_SELECTOR, "img[src*='tiktokcdn']")
                ]) > current_images_count)
                last_load_failed = False
                
            except TimeoutException:
                if last_load_failed:
                    logger.warning(f"新しい画像要素のロードが2回続けてタイムアウトしました。スクロールを中止します。{current_items_count}件の画像要素を確認しました", exc_info=True)
                    return False
                logger.warning(f"新しい画像要素のロードがタイムアウトしました。もう1度だけスクロールを試みます。{current_items_count}件の画像要素を確認しています")
                last_load_failed = True
                
        logger.debug(f"スクロール回数の上限に達しました。{current_items_count}件の画像要素を確認しました。スクロールを完了します。")
        return True


    # 動画ページの「クリエイターの動画」タブから動画の軽いデータの後半を取得する
    # Condition: 動画ページの「クリエイターの動画」タブが開かれていること
    # Args:
    #     max_videos: 取得する動画の最大数
    # Returns: 動画の軽いデータの後半(辞書型)のリスト
    def get_video_light_play_datas_from_video_page_creator_videos_tab(self, max_videos: int = 100) -> List[Dict[str, str]]:
        logger.debug(f"動画の軽いデータの後半を取得中...")

        video_stats = []
        self._random_sleep(25.0, 30.0)
        # クリエイターの動画一覧をスクロール
        self.scroll_video_page_creator_videos_tab(max_videos)
        video_elements = self.driver.find_elements(By.CSS_SELECTOR, "[class='css-eqiq8z-DivItemContainer eadndt66']")

        logger.debug(f"動画の軽いデータの後半を求めて{len(video_elements)}本の動画要素を走査します")
        for video_element in video_elements:
            try:
                # サムネイル画像を取得
                thumbnail_element = video_element.find_element(By.CSS_SELECTOR, "img[class*='ImgCover']")
                thumbnail_url = thumbnail_element.get_attribute("src")
                
                # 再生数を取得（表示形式のまま）
                play_count_element = video_element.find_element(By.CSS_SELECTOR, "div[class*='DivPlayCount']")
                play_count_text = play_count_element.text
                
                video_stats.append({
                    "video_thumbnail_url": thumbnail_url,
                    "play_count_text": play_count_text
                })
                # logger.debug(f"動画の再生数情報を取得: {thumbnail_url} -> {play_count_text}")
                
            except NoSuchElementException:
                logger.warning(f"動画の軽いデータの後半の取得のうち1件に失敗", exc_info=True)
                continue
        
        logger.debug(f"動画の軽いデータの後半を取得しました: {len(video_stats)}件")
        return video_stats


    # 動画の重いデータをパースおよび保存する
    # Condition: 自由
    # Args:
    #     heavy_data: 動画の重いデータ(辞書型)
    #     thumbnail_url: 動画のサムネイル画像のURL
    def parse_and_save_video_heavy_data(self, heavy_data: Dict, thumbnail_url: str, video_alt_info_text: Optional[str] = None, user_username: str = None, user_nickname: str = None):
        logger.debug(f"動画の重いデータをパースおよび保存中...: {heavy_data['video_url']}")

        video_id, _ = parse_tiktok_video_url(heavy_data["video_url"])

        # audio_info_textから音声情報を抽出
        audio_title = None
        audio_author_name = None
        if heavy_data.get("audio_info_text"):
            parts = heavy_data["audio_info_text"].split(" - ")
            if len(parts) >= 2:
                # 最後の部分をaudio_author_nameとし、それ以外を全てaudio_titleとする
                audio_author_name = parts[-1]
                audio_title = " - ".join(parts[:-1])

        post_time = parse_tiktok_time(heavy_data.get("post_time_text"), datetime.now())

        data = VideoHeavyRawData(
            id=None,
            video_url=heavy_data["video_url"],
            video_id=video_id,
            user_username=user_username,
            user_nickname=user_nickname,
            video_thumbnail_url=thumbnail_url,
            video_title=heavy_data["video_title"],
            post_time_text=heavy_data.get("post_time_text"),
            post_time=post_time,
            audio_info_text=heavy_data.get("audio_info_text"),
            audio_id=None,  # ここでは取得できない(できるかも)
            audio_title=audio_title,
            audio_author_name=audio_author_name,
            like_count_text=heavy_data.get("like_count_text"),
            like_count=parse_tiktok_number(heavy_data.get("like_count_text")),
            comment_count_text=heavy_data.get("comment_count_text"),
            comment_count=parse_tiktok_number(heavy_data.get("comment_count_text")),
            collect_count_text=heavy_data.get("collect_count_text"),
            collect_count=parse_tiktok_number(heavy_data.get("collect_count_text")),
            share_count_text=None,  # ここでは取得できない
            share_count=None,  # ここでは取得できない
            crawled_at=datetime.now(),
            crawling_algorithm=heavy_data["crawling_algorithm"]
        )
        
        self.video_repo.save_video_heavy_data(data)
        logger.info(f"動画の重いデータをパースおよび保存しました: {data.video_url}")

        publisher = pubsub_v1.PublisherClient()
        
        topic_path = publisher.topic_path(project_id, "video-master-sync")

        message_data = {
            "video_id": video_id,
            "video_url": heavy_data["video_url"],
            "user_username": user_username,
            "user_nickname": user_nickname,
            "video_thumbnail_url": thumbnail_url,
            "video_title": video_alt_info_text,
            "post_time": post_time.isoformat() if post_time else None,
            "audio_title": audio_title,
            "like_count": parse_tiktok_number(heavy_data.get("like_count_text")),
            "comment_count": parse_tiktok_number(heavy_data.get("comment_count_text")),
            "save_count": parse_tiktok_number(heavy_data.get("collect_count_text"))
        }

        # メッセージをJSON形式にエンコード
        message_str = json.dumps(message_data)
        message_bytes = message_str.encode("utf-8")

        try:
            future = publisher.publish(topic_path, message_bytes)
            message_id = future.result()
            logger.info(f"Pub/Subメッセージを送信しました。Message ID: {message_id}")
        except Exception as e:
            logger.error(f"Pub/Subメッセージの送信に失敗しました: {e}", exc_info=True)


    def _save_debug_csv(self, data: List[Dict], prefix: str) -> None:
        """デバッグ用にデータをCSVファイルとして出力する"""
        import csv
        import os
        from datetime import datetime

        if not data:
            return

        debug_dir = os.path.join("output", "debug")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(debug_dir, f"{prefix}_{timestamp}.csv")

        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        logger.debug(f"{prefix}を{csv_path}に出力しました: {len(data)}件")


    # ユーザー名とニックネームのデータを取得して保存する
    def get_and_save_user_name_datas(self, user_username: str) -> str:
        logger.debug(f"ユーザー名のデータを取得して保存中...")
        user_nickname = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='user-subtitle']").text
        self.favorite_user_repo.save_favorite_user_nickname(user_username, user_nickname)
        logger.debug(f"ユーザー名のデータを取得して保存しました: {user_username}, {user_nickname}")
        return user_nickname


    # 動画の軽いデータをパースおよび保存する
    # Condition: 自由
    # Args:
    #     light_like_datas: 動画の軽いデータの前半(辞書型)のリスト
    #     light_play_datas: 動画の軽いデータの後半(辞書型)のリスト
    def parse_and_save_video_light_datas(self, light_like_datas: List[Dict]):
        logger.debug(f"動画の軽いデータをパースおよび保存中...")

        # デバッグ用：CSVファイルに出力
        self._save_debug_csv(light_like_datas, "light_like_datas")
        # self._save_debug_csv(light_play_datas, "light_play_datas")

        # サムネイルURLのエッセンスをキーに、再生数をマッピング
        # play_count_map = {}
        # for play_data in light_play_datas:
        #     thumbnail_essence = extract_thumbnail_essence(play_data["video_thumbnail_url"])
        #     play_count_map[thumbnail_essence] = play_data["play_count_text"]
        
        # いいね数データを処理し、再生数を追加
        # play_count_not_found = 0
        for like_data in light_like_datas:
            # thumbnail_essence = extract_thumbnail_essence(like_data["video_thumbnail_url"])
            # play_count_text = play_count_map.get(thumbnail_essence)
            # if not play_count_text:
            #     play_count_not_found += 1

            data = VideoLightRawData(
                id=None,
                video_url=like_data["video_url"],
                video_id=like_data["video_id"],
                user_username=like_data["user_username"],
                video_thumbnail_url=like_data["video_thumbnail_url"],
                video_alt_info_text=like_data["video_alt_info_text"],
                like_count_text=like_data["like_count_text"],
                like_count=parse_tiktok_number(like_data["like_count_text"]),
                crawling_algorithm=like_data["crawling_algorithm"],
                crawled_at=datetime.now()
            )

            # logger.debug(f"動画の軽いデータを保存します: {data.video_id} -> {data.play_count}, {data.like_count}")
            self.video_repo.save_video_light_data(data)
            
        logger.info(f"動画の軽いデータをパースおよび保存しました: {len(light_like_datas)}件")


    # 動画の再生数の軽いデータをパースおよび保存する
    # Condition: 自由
    # Args:
    #     light_play_datas: 動画の再生数の軽いデータの前半(辞書型)のリスト
    def parse_and_save_play_count_datas(self, light_play_datas: List[Dict]):
        logger.debug(f"動画の再生数の軽いデータをパースおよび保存中...")    

        for play_data in light_play_datas:
            data = VideoPlayCountRawData(
                id=None,
                video_url=play_data["video_url"],
                video_id=play_data["video_id"],
                user_username=play_data["user_username"],
                play_count_text=play_data["play_count_text"],
                play_count=parse_tiktok_number(play_data["play_count_text"]),
                crawling_algorithm=play_data["crawling_algorithm"],
                crawled_at=datetime.now()
            )
            self.video_repo.save_video_play_count_data(data)
            publisher = pubsub_v1.PublisherClient()
        
            topic_path = publisher.topic_path(project_id, "video-master-sync")

            message_data = {
                "video_id": play_data["video_id"],
                "video_url": play_data["video_url"],
                "user_username": play_data["user_username"],
                "play_count": parse_tiktok_number(play_data["play_count_text"])
            }

            # メッセージをJSON形式にエンコード
            message_str = json.dumps(message_data)
            message_bytes = message_str.encode("utf-8")

            try:
                future = publisher.publish(topic_path, message_bytes)
                message_id = future.result()
                logger.info(f"Pub/Subメッセージを送信しました。Message ID: {message_id}")
            except Exception as e:
                logger.error(f"Pub/Subメッセージの送信に失敗しました: {e}", exc_info=True)
        logger.info(f"動画の再生数の軽いデータをパースおよび保存しました: {len(light_play_datas)}件")

    # ユーザーの動画データをクロールする
    # Condition: 自由
    # Args:
    #     user: お気に入りユーザー(FavoriteUser)
    #     light_or_heavy: "light"(軽いデータのみ), "heavy"(重いデータのみ), "both"(軽重両方)
    #     max_videos_per_user: 1ユーザーあたりの動画数
    #     recrawl: 既に重いデータを取得済みの動画を再取得するかどうか
    def crawl_user(self, user: FavoriteUser, light_or_heavy: str = "both", max_videos_per_user: int = 100, recrawl: bool = False):
        logger.info(f"ユーザー @{user.favorite_user_username} の{light_or_heavy}データのクロールを開始")

        try:
            self.navigate_to_user_page(user.favorite_user_username)
        except self.TikTokUserNotFoundException:
            logger.info(f"ユーザー @{user.favorite_user_username} は存在しないので、このユーザーに対するクロールを中断します")
            return False # ユーザー単位でしか問題にならないエラーなのでここで処置完了としてよい
        except Exception:
            raise
        light_like_datas = self.get_video_light_like_datas_from_user_page(max_videos_per_user)

        if light_or_heavy == "light":
            logger.info(f"ユーザー @{user.favorite_user_username} の軽いデータのクロールを開始")

            first_url = self.get_latest_video_url_from_user_page()
            self.navigate_to_video_page(first_url)
            self.navigate_to_video_page_creator_videos_tab()
            light_play_datas = self.get_video_light_play_datas_from_video_page_creator_videos_tab(max_videos_per_user+10) # 一対一対応してるか怪しいしほんのりバッファ
            
            self.parse_and_save_video_light_datas(light_like_datas, light_play_datas)
            self.navigate_to_user_page_from_video_page()
            logger.info(f"ユーザー @{user.favorite_user_username} の軽いデータのクロールを完了しました。")

        
        if light_or_heavy == "heavy":
            
            logger.info(f"ユーザー @{user.favorite_user_username} の重いデータのクロールを開始")
            if not recrawl:
                existing_video_ids = self.video_repo.get_existing_heavy_data_video_ids(user.favorite_user_username)
                light_like_datas = [light_like_data for light_like_data in light_like_datas if light_like_data["video_id"] not in existing_video_ids]

            # ここで最新20件に限定
            light_like_datas = light_like_datas[:20]  # 追加
                # 再生数データをマッピング
            play_count_map = {}
            for play_data in light_play_datas:
                thumbnail_essence = extract_thumbnail_essence(play_data["video_thumbnail_url"])
                play_count_text = play_data["play_count_text"]
                play_count_map[thumbnail_essence] = {
                    "play_count_text": play_count_text,
                    "play_count": parse_tiktok_number(play_count_text)
                }
    
            # light_like_datasに再生数を追加
            for light_like_data in light_like_datas:
                thumbnail_essence = extract_thumbnail_essence(light_like_data["video_thumbnail_url"])
                if thumbnail_essence in play_count_map:
                    light_like_data.update(play_count_map[thumbnail_essence])

            logger.info(f"動画 {len(light_like_datas)}件に対し重いデータのクロールを行います")
            for light_like_data in light_like_datas:
                try:
                    self.navigate_to_video_page(light_like_data["video_url"])
                    try:
                        heavy_data = self.get_video_heavy_data_from_video_page()
                        self.parse_and_save_video_heavy_data(heavy_data, light_like_data["video_thumbnail_url"],light_like_data.get("video_alt_info_text"),user.favorite_user_username,user.nickname)
                        self._random_sleep(10.0, 20.0) # こんくらいは見たほうがいいんじゃないかな未検証だけど
                    except Exception:
                        logger.exception(f"動画ページを開いた状態でエラーが発生しました。動画ページを閉じてユーザーページに戻ります。")
                        raise
                    finally:
                        self.navigate_to_user_page_from_video_page()
                except KeyboardInterrupt:
                    raise
                except Exception:
                    logger.exception(f"動画 {light_like_data['video_url']} の重いデータのクロール中に失敗。スキップします")
                    continue

            logger.info(f"ユーザー @{user.favorite_user_username} の重いデータのクロールを完了しました")

        logger.debug(f"ユーザー @{user.favorite_user_username} のlast_crawledを更新します")
        self.favorite_user_repo.update_favorite_user_last_crawled(
            user.favorite_user_username,
            datetime.now()
        )

        if light_or_heavy == "both":
            if user.is_new_account == True:
                logger.info(f"ユーザー @{user.favorite_user_username} の軽いデータののクロールを開始")
                max_videos_per_batch = 100  # 1回のバッチで取得する動画数
                # target_date = datetime(2025, 1, 1)  # この日付より前の動画が見つかるまでクロール
                processed_urls = set()  # 処理済みのURLを記録

                while True:
                    # 最新の動画URLを取得してビデオページに移動
                    # first_url = self.get_latest_video_url_from_user_page()
                    # self.navigate_to_video_page(first_url)
                    # self.navigate_to_video_page_creator_videos_tab()
                    
                    # # 軽いデータを取得
                    # light_play_datas = self.get_video_light_play_datas_from_video_page_creator_videos_tab(max_videos_per_batch + 10)
                    self.parse_and_save_video_light_datas(light_like_datas)
                    # self.navigate_to_user_page_from_video_page()
                    
                    logger.info(f"バッチの軽いデータのクロールを完了しました。ユーザー名のクロールを開始します。")
                    current_nickname = self.get_and_save_user_name_datas(user.favorite_user_username)

                    

                    # play_count_map = {}
                    # for play_data in light_play_datas:
                    #     thumbnail_essence = extract_thumbnail_essence(play_data["video_thumbnail_url"])
                    #     play_count_text = play_data["play_count_text"]
                    #     play_count_map[thumbnail_essence] = {
                    #         "play_count_text": play_count_text,
                    #         "play_count": parse_tiktok_number(play_count_text)
                    #     }
            
                    # # light_like_datasに再生数を追加
                    # for light_like_data in light_like_datas:
                    #     thumbnail_essence = extract_thumbnail_essence(light_like_data["video_thumbnail_url"])
                    #     if thumbnail_essence in play_count_map:
                    #         light_like_data.update(play_count_map[thumbnail_essence])
                            # 重いデータを取得
                    # last_post_time = None
                    light_like_datas.sort(key=lambda d: int(d["video_id"]), reverse=True)

                    # ③ 先頭 max_videos_per_batch 件だけをバッチ対象にスライス
                    batch_datas = light_like_datas[:max_videos_per_batch]
                    for light_like_data in batch_datas:
                        if light_like_data["video_url"] in processed_urls:
                            continue
                        try:
                            direct_access = self.navigate_to_video_page(light_like_data["video_url"])
                            if direct_access:
                                heavy_data = self.get_video_heavy_data_from_direct_access()
                            else:                            
                                heavy_data = self.get_video_heavy_data_from_video_page()



                            self.parse_and_save_video_heavy_data(heavy_data, light_like_data["video_thumbnail_url"],light_like_data.get("video_alt_info_text"),user.favorite_user_username,current_nickname)
                            
                            # 投稿日時を取得して記録
                            # post_time = parse_tiktok_time(heavy_data.get("post_time_text"), datetime.now())
                            # if post_time:
                            #     last_post_time = post_time
                            
                            processed_urls.add(light_like_data["video_url"])
                            self._random_sleep(10.0, 20.0)
                                    
                                
                        except KeyboardInterrupt:
                            raise
                        except Exception:
                            logger.exception(f"動画 {light_like_data['video_url']} の重いデータのクロール中に失敗。スキップします")
                            continue
                        finally:
                            try:
                                self.navigate_to_user_page_from_video_page()
                            except Exception:
                                logger.warning("ユーザーページへの戻りに失敗 (無視して続行)")
                    
                    # 最後に処理した動画の投稿日時が2025/1/1より前なら終了
                    # if last_post_time and last_post_time < target_date:
                    #     logger.info(f"目標日付（{target_date}）より前の動画を処理したため、クロールを終了します")
                    #     self.favorite_user_repo.update_favorite_user_is_new_account(
                    #         user.favorite_user_username,
                    #         False
                    #     )
                    #     break

                    if len(batch_datas) <= max_videos_per_batch:
                        logger.info(f"取得できた動画が{len(light_like_datas)}件と目標の{max_videos_per_batch}件未満のため、全ての動画を取得済みと判断してクロールを終了します")
                        self.favorite_user_repo.update_favorite_user_is_new_account(
                            user.favorite_user_username,
                            False
                        )
                        break
                        
                    # # まだ2025/1/1より後の動画なら、さらに古い動画を取得するためにスクロール
                    # logger.info(f"まだ目標日付（{target_date}）より後の動画（最終投稿日時: {last_post_time}）のため、さらに古い動画を取得します")
                    # max_videos_per_batch += 50
                    # light_like_datas = self.get_video_light_like_datas_from_user_page(max_videos_per_batch)

                    # ①投稿が50個未満の場合はストップ


                    # ②既に処理済みのURLを除外
                    light_like_datas = [data for data in light_like_datas if data["video_url"] not in processed_urls]
                    if not light_like_datas:
                        logger.info("未処理の動画が残っていないため、クロールを終了します")
                        break

            else:
                # 取得実績有アカウントの処理

                # 最新の動画URLを取得してビデオページに移動
                # first_url = self.get_latest_video_url_from_user_page()
                # self.navigate_to_video_page(first_url)
                # self.navigate_to_video_page_creator_videos_tab()
                # max_videos_per_batch = 50
                # 軽いデータを取得
                # light_play_datas = self.get_video_light_play_datas_from_video_page_creator_videos_tab(max_videos_per_batch + 10)
                self.parse_and_save_video_light_datas(light_like_datas)
                # self.navigate_to_user_page_from_video_page()
                    
                # logger.info(f"バッチの軽いデータのクロールを完了しました。重いデータのクロールを開始します。")
                # needs_update=1の動画を取得
                logger.info(f"ユーザー @{user.favorite_user_username} の更新が必要な動画を取得します")
                videos_needing_update = self.video_repo.get_videos_needing_update(user.favorite_user_username)
                logger.info(f"{len(videos_needing_update)}件の動画の更新が必要です")

                # 重いデータを取得
                logger.info(f"更新が必要な動画の重いデータのクロールを開始します")
                for video in videos_needing_update:
                    try:
                        direct_access = self.navigate_to_video_page(video["video_url"])
                        if direct_access:
                            heavy_data = self.get_video_heavy_data_from_direct_access()
                        else:                            
                            heavy_data = self.get_video_heavy_data_from_video_page()
                        self.parse_and_save_video_heavy_data(heavy_data, video["video_thumbnail_url"],video.get("video_alt_info_text"),user.favorite_user_username,user.nickname)
                        self._random_sleep(10.0, 20.0)
                    except KeyboardInterrupt:
                        raise
                    except Exception:
                        logger.exception(f"動画 {video['video_url']} の重いデータのクロール中に失敗。スキップします")
                        continue
                    finally:
                        try:
                            self.navigate_to_user_page_from_video_page()
                        except Exception:
                            logger.warning("ユーザーページへの戻りに失敗 (無視して続行)")

            logger.info(f"ユーザー @{user.favorite_user_username} の重いデータのクロールを完了しました")

        logger.debug(f"ユーザー @{user.favorite_user_username} のlast_crawledを更新します")
        self.favorite_user_repo.update_favorite_user_last_crawled(
            user.favorite_user_username,
            datetime.now()
        )
        logger.debug(f"ユーザー @{user.favorite_user_username} のlast_crawledを更新しました")

        logger.info(f"ユーザー @{user.favorite_user_username} の{light_or_heavy}データのクロールを完了しました")


    # お気に入りユーザーたちの動画データをクロールする
    # Condition: 自由
    # Args:
    #     light_or_heavy: "light"(軽いデータのみ), "heavy"(重いデータのみ), "both"(軽重両方)
    #     max_videos_per_user: 1ユーザーあたりの動画数
    #     max_users: 1クロール対象のユーザー数
    #     recrawl: 既に重いデータを取得済みの動画を再取得するかどうか
    def crawl_favorite_users(self, light_or_heavy: str = "both", max_videos_per_user: int = 100, max_users: int = 10, recrawl: bool = True, engagement_type: str = "like"):
        logger.info(f"クロール対象のお気に入りユーザー{max_users}件に対し{light_or_heavy}データのクロールを行います")
        if engagement_type == "play":
            favorite_users = self.favorite_user_repo.get_favorite_users_by_play_count_crawler_id(
                self.crawler_account.id,
                limit=max_users
            )
            for user in favorite_users:
                try:
                    self.crawl_play_count(user,max_videos_per_user=max_videos_per_user)
                except KeyboardInterrupt:
                    raise
                except Exception:
                    logger.exception(f"ユーザー @{user.favorite_user_username} の再生数データのクロール中に失敗。スキップします")
                    continue
        else:
            favorite_users = self.favorite_user_repo.get_favorite_users(
                self.crawler_account.id,
                limit=max_users
            )

            for user in favorite_users:
                try:
                    self.crawl_user(user, light_or_heavy=light_or_heavy, max_videos_per_user=max_videos_per_user, recrawl=recrawl)
                except KeyboardInterrupt:
                    raise
                except Exception:
                    logger.exception(f"ユーザー @{user.favorite_user_username} の{light_or_heavy}データのクロール中に失敗。スキップします")
                    continue
        
        logger.info(f"クロール対象のお気に入りユーザー{len(favorite_users)}件に対し{light_or_heavy}データのクロールを完了しました")

    def crawl_play_count(self, user: FavoriteUser, max_videos_per_user: int = 100):
        logger.info(f"ユーザー @{user.favorite_user_username} の軽いデータのクロールを開始")
        try:
            self.navigate_to_user_page(user.favorite_user_username)
        except self.TikTokUserNotFoundException:
            logger.info(f"ユーザー @{user.favorite_user_username} は存在しないので、このユーザーに対するクロールを中断します")
            return False # ユーザー単位でしか問題にならないエラーなのでここで処置完了としてよい
        except Exception:
            raise
        
        light_play_datas = self.get_video_play_count_datas_from_user_page(max_videos_per_user)

        logger.info(f"ユーザー @{user.favorite_user_username} の再生数の取得が完了")

        self.parse_and_save_play_count_datas(light_play_datas)
        logger.info(f"ユーザー @{user.favorite_user_username} の再生数のクロールを完了しました。")


def main():
    # コマンドライン引数の処理
    import argparse
    parser = argparse.ArgumentParser(description="TikTok動画データ収集クローラー")
    
    # 必須の引数
    parser.add_argument(
        "mode",
        choices=["light", "heavy", "both"],
        help="クロールモード。light: 軽いデータのみ、heavy: 重いデータのみ、both: 両方"
    )
    
    # オプションの引数
    parser.add_argument(
        "--crawler-account-id",
        type=int,
        help="使用するクローラーアカウントのID"
    )
    parser.add_argument(
        "--max-videos-per-user",
        type=int,
        default=100,
        help="1ユーザーあたりの最大取得動画数（デフォルト: 100）"
    )
    parser.add_argument(
        "--max-users",
        type=int,
        default=50,
        help="クロール対象の最大ユーザー数（デフォルト: 10）"
    )
    parser.add_argument(
        "--recrawl",
        action="store_true",
        help="既にクロール済みの動画を再クロールする (デフォルト: False)"
    )
    parser.add_argument(
        "--engagement-type",
        choices=["like", "play"],
        default="like",
        help="クロール対象のデータの種類 (デフォルト: like)"
    )
    parser.add_argument(
        "--device-type",
        choices=["pc", "vps"],
        default="pc",
        help="デバイスタイプ (デフォルト: pc)"
    )
    args = parser.parse_args()

    # データベース接続の初期化
    with Database() as db:
        # 各リポジトリの初期化
        crawler_account_repo = CrawlerAccountRepository(db)
        favorite_user_repo = FavoriteUserRepository(db)
        video_repo = VideoRepository(db)
            
        # クローラーの初期化と実行
        with TikTokCrawler(
            crawler_account_repo=crawler_account_repo,
            favorite_user_repo=favorite_user_repo,
            video_repo=video_repo,
            crawler_account_id=args.crawler_account_id,
            sadcaptcha_api_key=os.getenv("SADCAPTCHA_API_KEY"),  # APIキーを設定
            engagement_type=args.engagement_type,
            device_type=args.device_type
        ) as crawler:
            crawler.crawl_favorite_users(
                light_or_heavy=args.mode,
                max_videos_per_user=args.max_videos_per_user,
                max_users=args.max_users,
                recrawl=args.recrawl,
                engagement_type=args.engagement_type
            )

if __name__ == "__main__":
    import sys
    try:
        main()
    except KeyboardInterrupt:
        logger.info("ユーザーにより中断されました")
        sys.exit(130)  # 128 + SIGINT(2)
    except Exception:
        logger.exception("予期しないエラーが発生しました")
        sys.exit(1)

