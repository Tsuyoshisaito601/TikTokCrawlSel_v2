import os
import json
import random
import re
import time
from datetime import date, datetime, timedelta
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import pubsub_v1
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from ..database.database import Database
from ..database.models import CrawlerAccount, FavoriteUser, InstaHeavyRawData, InstaLightRawData
from ..database.repositories import (
    InstaCrawlerAccountRepository,
    InstaFavoriteUserRepository,
    InstaVideoRepository,
)
from ..logger import setup_logger
from .selenium_manager import SeleniumManager

logger = setup_logger(__name__)
project_id = os.getenv("PROJECT_ID")
RUN_MINUTES = 60
REST_MINUTES = 30


def parse_insta_time(time_text: str, base_time: datetime) -> Optional[datetime]:
    """Instagramの日時表記をパースする"""
    if not time_text:
        return None

    text = time_text.strip()
    try:
        if text.endswith("秒前"):
            seconds = int(re.sub(r"秒前$", "", text))
            return base_time - timedelta(seconds=seconds)
        if text.endswith("分前"):
            minutes = int(re.sub(r"分前$", "", text))
            return base_time - timedelta(minutes=minutes)
        if text.endswith("時間前"):
            hours = int(re.sub(r"時間前$", "", text))
            return base_time - timedelta(hours=hours)
        if text.endswith("日前"):
            days = int(re.sub(r"日前$", "", text))
            return base_time - timedelta(days=days)
        if text.endswith("週間前"):
            weeks = int(re.sub(r"週間前$", "", text))
            return base_time - timedelta(days=7 * weeks)

        # 2024年5月17日 形式
        m = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
        if m:
            year, month, day = map(int, m.groups())
            return datetime(
                year,
                month,
                day,
                base_time.hour,
                base_time.minute,
                tzinfo=base_time.tzinfo,
            )

        # YYYY-MM-DD 形式
        if "-" in text and len(text.split("-")) == 3:
            year, month, day = map(int, text.split("-"))
            return datetime(
                year, month, day, base_time.hour, base_time.minute, tzinfo=base_time.tzinfo
            )

        return None
    except Exception:
        logger.warning(f"日時のパースに失敗: {time_text}", exc_info=True)
        return None


def parse_insta_video_url(url: str) -> Tuple[str, str]:
    """InstagramのURLからvideo_idとuser_usernameを抽出する"""
    try:
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split("/") if p]
        # 例: /username/p/video_id
        video_id = path_parts[-1]
        user_username = path_parts[0] if len(path_parts) >= 1 else ""
        return video_id, user_username
    except Exception:
        logger.exception(f"URLのパースに失敗: {url}")
        raise


def parse_insta_number(text: str) -> Optional[int]:
    """Instagramの「万」「億」付き数字を整数へ変換する"""
    if not text:
        return None

    try:
        clean_text = text.replace(",", "").strip()
        if not clean_text:
            return None
        if clean_text[-1] == "万":
            return int(float(clean_text[:-1]) * 10_000)
        if clean_text[-1] == "億":
            return int(float(clean_text[:-1]) * 100_000_000)
        if clean_text.replace(".", "").isdigit():
            return int(float(clean_text))
    except Exception:
        logger.warning(f"数字のパースに失敗: {text}", exc_info=True)
        return None

    return None


class InstaCrawler:
    BASE_URL = "https://www.instagram.com"
    PROFILE_CONTENT_SELECTOR = (
        "div.x1lliihq.x1n2onr6.xh8yej3.x4gyw5p.x14z9mp.xhe4ym4.xaudc5v.x1j53mea"
    )
    REEL_ITEM_CONTAINER_SELECTOR = (
        "div.x1qjc9v5.x972fbf.x10w94by.x1qhh985.x14e42zd.x9f619.x78zum5.xdt5ytf"
    )
    REEL_VIEW_COUNT_SELECTOR = (
        "div._aaj_ span.html-span.xdj266r.x14z9mp.xat24cr.x1lziwak.xexx8yu."
        "xyri2b.x18d9i69.x1c1uobl.x1hl2dhg.x16tdsg8.x1vvkbs"
    )
    REEL_THUMBNAIL_SELECTOR = "div.x1n2onr6.x1lvsgvq.xiy17q3.x18d0r48"
    PINNED_SVG_SELECTOR = "svg[title*='ピン留め']"
    VIDEO_POST_TIME_SELECTOR = "time.x1p4m5qa"
    VIDEO_AUDIO_INFO_SELECTOR = (
        "a[href^='/reels/audio/'] span.x6ikm8r.x10wlt62.xlyipyv.xuxw1ft"
    )
    VIDEO_TITLE_SELECTOR = (
        "h1._ap3a._aaco._aacu._aacx._aad7._aade, "
        "h1._ap3a, "
        "h1[data-testid='post-caption-title'], "
        "span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.xt0psk2.x1i0vuye.xvs91rp.xo1l8bm.x5n08af.x10wh9bi.xpm28yp.x8viiok.x1o7cslx.x126k92a"
    )
    VIDEO_COMMENTS_SELECTOR = "ul._a9ym li._a9zj"
    USER_NICKNAME_SELECTOR = (
        "span.x1lliihq.x1plvlek.xryxfnj.x1n2onr6.xyejjpt.x15dsfln.x193iq5w."
        "xeuugli.x1fj9vlw.x13faqbe.x1vvkbs.x1s928wv.xhkezso.x1gmr53x.x1cpjm7i."
        "x1fgarty.x1943h6x.x1i0vuye.xvs91rp.xo1l8bm.x5n08af.x10wh9bi.xpm28yp."
        "x8viiok.x1o7cslx"
    )

    # 閉じるボタン用セレクター（追加）
    CLOSE_BUTTON_SELECTOR = "svg[aria-label='閉じる']"

    class InstaUserNotFoundException(Exception):
        pass

    def __init__(
        self,
        crawler_account_repo: InstaCrawlerAccountRepository,
        favorite_user_repo: InstaFavoriteUserRepository,
        video_repo: InstaVideoRepository,
        crawler_account_id: Optional[int] = None,
        sadcaptcha_api_key: Optional[str] = None,
        engagement_type: str = "like",
        device_type: str = "pc",
        use_profile: bool = False,
        chrome_user_data_dir: Optional[str] = None,
        chrome_profile_directory: Optional[str] = None,
        skip_login: bool = False,
        use_proxy: bool = True,
    ):
        self.crawler_account_repo = crawler_account_repo
        self.favorite_user_repo = favorite_user_repo
        self.video_repo = video_repo
        self.crawler_account_id = crawler_account_id
        self.crawler_account: Optional[CrawlerAccount] = None
        self.selenium_manager = None
        self.driver = None
        self.wait = None
        self.sadcaptcha_api_key = sadcaptcha_api_key
        self.login_restart_attempted = False
        self.engagement_type = engagement_type
        self.device_type = device_type
        self.use_profile = use_profile
        self.chrome_user_data_dir = chrome_user_data_dir
        self.chrome_profile_directory = chrome_profile_directory
        self.skip_login = skip_login
        self.use_proxy = use_proxy

        self.publisher: Optional[pubsub_v1.PublisherClient] = None
        self._publisher_topic_path: Optional[str] = None

    def __enter__(self):
        try:
            if not self.skip_login:
                if self.crawler_account_id is not None:
                    self.crawler_account = self.crawler_account_repo.get_crawler_account_by_id(
                        self.crawler_account_id
                    )
                    if not self.crawler_account:
                        raise Exception(
                            f"指定されたクローラーアカウントが見つかりません: id={self.crawler_account_id}"
                        )
                else:
                    self.crawler_account = self.crawler_account_repo.get_an_available_crawler_account()
                    if not self.crawler_account:
                        raise Exception("利用可能なクローラーアカウントが存在しません")
                proxy = self.crawler_account.proxy if self.use_proxy else None
            else:
                proxy = None

            self.selenium_manager = SeleniumManager(
                proxy,
                self.sadcaptcha_api_key,
                self.device_type,
                use_profile=self.use_profile,
                user_data_dir=self.chrome_user_data_dir,
                profile_directory=self.chrome_profile_directory,
            )
            self.driver = self.selenium_manager.setup_driver()
            self.wait = WebDriverWait(self.driver, 15)

            if not self.skip_login:
                self._login()
                self.crawler_account_repo.update_crawler_account_last_crawled(
                    self.crawler_account.id, datetime.now()
                )
            else:
                self.driver.get(self.BASE_URL)
                self._random_sleep(2.0, 3.0)
            return self
        except Exception:
            self._cleanup_resources()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_resources()

    def _cleanup_resources(self):
        if self.selenium_manager:
            try:
                self.selenium_manager.quit_driver()
            except Exception:
                logger.exception("Chromeドライバー終了時にエラーが発生しました")
            finally:
                self.selenium_manager = None
                self.driver = None
                self.wait = None
        if self.publisher:
            try:
                close = getattr(self.publisher, "close", None)
                if callable(close):
                    close()
                else:
                    transport = getattr(self.publisher, "transport", None)
                    if transport and hasattr(transport, "close"):
                        transport.close()
            except Exception:
                logger.exception("Pub/Subクライアント終了時にエラーが発生しました")
            finally:
                self.publisher = None
                self._publisher_topic_path = None

    def _init_publisher(self):
        if self.publisher:
            return
        self.publisher = pubsub_v1.PublisherClient()
        # Instagram用: insta_video_master向けのトピックを利用
        self._publisher_topic_path = self.publisher.topic_path(
            project_id, "insta-video-master-sync"
        )

    def _publish_video_master_sync(self, message_data: Dict):
        if not project_id:
            logger.warning("PROJECT_ID が設定されていないため、Pub/Sub 送信をスキップします")
            return

        try:
            if not self.publisher:
                self._init_publisher()
            if not self._publisher_topic_path:
                logger.warning("Pub/Sub トピックパスが未設定のため送信をスキップします")
                return

            message_data.setdefault("target_table", "insta_video_master")
            message_bytes = json.dumps(message_data).encode("utf-8")
            future = self.publisher.publish(self._publisher_topic_path, message_bytes)
            message_id = future.result()
            logger.info(f"Pub/Sub メッセージを送信しました: {message_id}")
        except Exception as e:
            logger.error(f"Pub/Sub 送信に失敗しました: {e}", exc_info=True)

    def _random_sleep(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        time.sleep(random.uniform(min_seconds, max_seconds))

    def _parse_datetime_attr(self, datetime_attr: Optional[str]) -> Optional[date]:
        if not datetime_attr:
            return None
        try:
            iso = datetime_attr.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(iso)
            return parsed.date()
        except Exception:
            logger.warning(f"datetime属性のパースに失敗: {datetime_attr}", exc_info=True)
            return None

    def _login(self):
        logger.info(
            f"クローラーアカウント {self.crawler_account.username} でInstagramにログインします..."
        )
        if self.use_profile:
            logger.info("事前構成済みのChromeプロファイルでログイン状態を確認します")
            self.driver.get(self.BASE_URL)
            self._random_sleep(2.0, 4.0)
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "img[alt*='プロフィール写真'], img[alt$='のプロフィール写真']")
                    )
                )
                logger.info("既存プロフィールでログイン済みでした")
                return
            except TimeoutException:
                logger.info("ログイン状態を確認できなかったためフォーム入力を実施します")

        self.driver.get(f"{self.BASE_URL}/accounts/login/")
        self._random_sleep(2.0, 4.0)

        username_input = self.wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "input[name='username'][aria-label*='電話番号'],"
                    "input[name='username'][aria-label*='ユーザーネーム'],"
                    "input[name='username'][aria-label*='メールアドレス'],"
                    "input[name='username']",
                )
            )
        )
        self._random_sleep(1.5, 2.5)
        username_input.clear()
        username_input.send_keys(self.crawler_account.username)

        self._random_sleep(1.5, 2.5)
        password_input = self.wait.until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "input[name='password'][aria-label*='パスワード'],"
                    "input[name='password']",
                )
            )
        )
        password_input.clear()
        password_input.send_keys(self.crawler_account.password)

        self._random_sleep(1.5, 2.5)
        login_button = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'], div button[type='submit']"))
        )
        self._random_sleep(2.0, 3.0)
        login_button.click()

        try:
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "img[alt*='プロフィール写真'], img[alt$='のプロフィール写真']")
                )
            )
            logger.info("Instagramへのログインに成功しました")
        except TimeoutException:
            logger.error("Instagramへのログインに失敗しました")
            raise

    def navigate_to_user_page(self, username: str):
        logger.debug(f"ユーザー @{username} のページへ遷移します")
        self.driver.get(f"{self.BASE_URL}/{username}/")
        self._random_sleep(2.0, 4.0)
        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, self.PROFILE_CONTENT_SELECTOR)
                )
            )
        except TimeoutException:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "ご利用いただけません" in body_text or "リンクが切れている" in body_text:
                raise self.InstaUserNotFoundException(f"ユーザー @{username} は存在しません")
            raise
        logger.debug(f"ユーザーページ @{username} の読み込みが完了しました")

    def navigate_to_reels_page(self, username: str):
        reels_path = f"/{username}/reels/"
        reels_url = f"{self.BASE_URL}{reels_path}"
        logger.debug(f"リールページ @{username} に遷移します")
        self._random_sleep(1.5, 2.5)
        try:
            reels_tab = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"a[href$='{reels_path}']"))
            )
            reels_tab.click()
            self._random_sleep(1.5, 2.5)
        except TimeoutException:
            logger.info("リールタブが見つからないため、URLに直接遷移します")
            self.driver.get(reels_url)

        if not self.driver.current_url.rstrip("/").endswith(reels_path.rstrip("/")):
            self.driver.get(reels_url)

        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR)
                )
            )
        except TimeoutException:
            logger.warning("リールタブのコンテンツが読み込めませんでした")
        self._random_sleep(1.5, 2.5)

    def scroll_user_page(self, need_items_count: int = 100, max_scroll_attempts: int = None) -> bool:
        logger.debug(f"{need_items_count} 件以上の投稿を目標にユーザーページをスクロールします")
        attempts = max_scroll_attempts or max(need_items_count // 3, 5)
        for _ in range(attempts):
            posts = self.driver.find_elements(By.CSS_SELECTOR, "article a[href*='/p/']")
            if len(posts) >= need_items_count:
                return True
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self._random_sleep(1.5, 2.5)
        return len(self.driver.find_elements(By.CSS_SELECTOR, "article a[href*='/p/']")) >= need_items_count

    def scroll_reels_page(self, need_items_count: int = 100, max_scroll_attempts: int = None) -> bool:
        logger.debug(f"{need_items_count} 件以上のリールを目標にスクロールします")
        attempts = max_scroll_attempts or max(need_items_count // 3, 5)
        for _ in range(attempts):
            items = self.driver.find_elements(By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR)
            if len(items) >= need_items_count:
                return True
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self._random_sleep(1.5, 2.5)
        return len(self.driver.find_elements(By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR)) >= need_items_count


    def get_video_heavy_data_from_video_page(
        self, fetch_comments: bool = True, comment_limit: int = 20
    ) -> Dict[str, Optional[str]]:
        logger.debug("リール詳細ページから詳細情報を取得します")
        heavy_data: Dict[str, Optional[str]] = {
            "video_url": self.driver.current_url,
            "post_time_text": None,
            "post_time_iso": None,
            "audio_info_text": None,
            "video_title": None,
            "comments_json": None,
        }

        try:
            time_elem = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.VIDEO_POST_TIME_SELECTOR))
            )
            heavy_data["post_time_text"] = time_elem.text
            heavy_data["post_time_iso"] = time_elem.get_attribute("datetime")
        except TimeoutException:
            logger.warning("投稿日タイムスタンプの取得に失敗しました", exc_info=True)

        try:
            title_elem = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.VIDEO_TITLE_SELECTOR))
            )
            # ハッシュタグを含む全文を innerText/textContent で取得（改行も保持）
            raw_inner = title_elem.get_attribute("innerText")
            raw_text_content = title_elem.get_attribute("textContent")
            raw_text = title_elem.text
            heavy_data["video_title"] = (
                title_elem.get_attribute("innerText")
                or title_elem.get_attribute("textContent")
                or title_elem.text
            )
            logger.debug(
                "video_title取得: innerText=%s textContent=%s text=%s",
                raw_inner,
                raw_text_content,
                raw_text,
            )
        except TimeoutException:
            logger.debug("動画タイトルが見つかりませんでした")

        try:
            audio_elem = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.VIDEO_AUDIO_INFO_SELECTOR))
            )
            heavy_data["audio_info_text"] = audio_elem.text
        except TimeoutException:
            logger.debug("音源情報が見つかりませんでした")

        if fetch_comments:
            comments: List[str] = []
            try:
                comment_elems = self.driver.find_elements(By.CSS_SELECTOR, self.VIDEO_COMMENTS_SELECTOR)
                for elem in comment_elems[:comment_limit]:
                    text = elem.text.strip()
                    if text:
                        comments.append(text)
                if comments:
                    heavy_data["comments_json"] = json.dumps(comments, ensure_ascii=False)
            except Exception:
                logger.warning("コメント取得でエラーが発生しました", exc_info=True)

        return heavy_data

    def _scroll_to_element(self, element):
        """要素が見える位置までスクロールする"""
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                element
            )
            self._random_sleep(0.5, 1.0)
        except Exception:
            logger.warning("要素へのスクロールに失敗しました", exc_info=True)

    def _click_reel_item_by_index(self, index: int) -> bool:
        """リールページで指定インデックスのリール要素をクリックして詳細ページに遷移する"""
        try:
            reel_items = self.driver.find_elements(By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR)
            if index >= len(reel_items):
                logger.warning(f"リール要素が見つかりません: index={index}, 現在の要素数={len(reel_items)}")
                return False
            
            target_elem = reel_items[index]
            
            # 要素の位置までスクロール
            self._scroll_to_element(target_elem)
            self._random_sleep(0.8, 1.5)
            
            # リール内のリンク要素をクリック
            link_elem = target_elem.find_element(By.CSS_SELECTOR, "a[href*='/reel/']")
            link_elem.click()
            self._random_sleep(2.0, 3.5)
            
            # 詳細ページへの遷移を確認（閉じるボタンが表示されることで確認）
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.CLOSE_BUTTON_SELECTOR))
            )
            return True
        except TimeoutException:
            logger.warning(f"リール詳細ページへの遷移がタイムアウトしました: index={index}")
            return False
        except Exception:
            logger.warning(f"リール要素のクリックに失敗しました: index={index}", exc_info=True)
            return False

    def _click_close_button_to_return(self, username: str):
        """閉じるボタン（×）をクリックしてリールページに戻る"""
        try:
            # 閉じるボタン（svg）を探す
            close_svg = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, self.CLOSE_BUTTON_SELECTOR))
            )
            
            # svgの親要素（div[role="button"]）を取得してクリック
            close_button = close_svg.find_element(By.XPATH, "./ancestor::div[@role='button']")
            close_button.click()
            self._random_sleep(1.5, 2.5)
            
            # リールページに戻れたか確認
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR))
            )
            logger.debug("閉じるボタンでリールページに戻りました")
            
        except TimeoutException:
            logger.warning("閉じるボタンが見つからないか、リールページへの復帰がタイムアウトしました")
            # フォールバック: URL直接遷移
            self._fallback_navigate_to_reels_page(username)
        except Exception:
            logger.warning("閉じるボタンでの遷移に失敗しました", exc_info=True)
            self._fallback_navigate_to_reels_page(username)

    def _fallback_navigate_to_reels_page(self, username: str):
        """フォールバック: URL遷移でリールページに戻る"""
        logger.debug("URL遷移でリールページに戻ります")
        self.driver.get(f"{self.BASE_URL}/{username}/reels/")
        self._random_sleep(2.0, 3.0)
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR))
            )
        except TimeoutException:
            logger.warning("リールページへのURL遷移後、要素が見つかりませんでした")

    def collect_reel_heavy_data_map(
        self, light_like_datas: List[Dict], user_username: str = None,
        fetch_comments: bool = True, comment_limit: int = 20,
        skip_video_ids: Optional[Set[str]] = None
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """
        リール詳細データを収集する。
        各リールについて、ユーザーリールページからスクロール＋クリックで遷移し、
        詳細取得後は閉じるボタンでリールページに戻る。
        """
        heavy_map: Dict[str, Dict[str, Optional[str]]] = {}
        skip_video_ids = skip_video_ids or set()
        
        if not user_username and light_like_datas:
            user_username = light_like_datas[0].get("user_username")
        
        if not user_username:
            logger.warning("user_usernameが不明なため、URL遷移方式にフォールバックします")
            return self._collect_reel_heavy_data_map_by_url(
                light_like_datas, fetch_comments, comment_limit, skip_video_ids
            )
        
        # リールページにいることを確認（いなければ遷移）
        reels_path = f"/{user_username}/reels/"
        if reels_path not in self.driver.current_url:
            self.navigate_to_reels_page(user_username)
        
        # スクロールしてすべての対象リールを読み込む
        self.scroll_reels_page(len(light_like_datas))
        
        for index, like_data in enumerate(light_like_datas):
            video_url = like_data.get("video_url")
            video_id = like_data.get("video_id")
            if not video_url or not video_id:
                continue
            if video_id in skip_video_ids:
                continue
            
            try:
                # リールページから対象リールをクリックして遷移
                if self._click_reel_item_by_index(index):
                    heavy_data = self.get_video_heavy_data_from_video_page(
                        fetch_comments=fetch_comments, comment_limit=comment_limit
                    )
                    heavy_map[video_id] = heavy_data
                else:
                    logger.warning(f"リール詳細ページへの遷移に失敗したためスキップ: {video_url}")
                
                # 閉じるボタンをクリックしてリールページに戻る
                self._click_close_button_to_return(user_username)
                
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.warning(f"動画ページ遷移または詳細取得で失敗: {video_url}", exc_info=True)
                # エラー発生時もリールページに戻る試み
                try:
                    self._click_close_button_to_return(user_username)
                except Exception:
                    pass
                continue
        
        return heavy_map

    def _collect_reel_heavy_data_map_by_url(
        self, light_like_datas: List[Dict], fetch_comments: bool = True, comment_limit: int = 20,
        skip_video_ids: Optional[Set[str]] = None
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """フォールバック用: URL遷移方式で詳細データを収集する（旧実装）"""
        heavy_map: Dict[str, Dict[str, Optional[str]]] = {}
        skip_video_ids = skip_video_ids or set()
        for like_data in light_like_datas:
            video_url = like_data.get("video_url")
            video_id = like_data.get("video_id")
            if not video_url or not video_id:
                continue
            if video_id in skip_video_ids:
                continue
            try:
                self.driver.get(video_url)
                self._random_sleep(2.0, 3.5)
                heavy_data = self.get_video_heavy_data_from_video_page(
                    fetch_comments=fetch_comments, comment_limit=comment_limit
                )
                heavy_map[video_id] = heavy_data
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.warning(f"動画ページ遷移または詳細取得で失敗: {video_url}", exc_info=True)
                continue
        return heavy_map

    def _extract_like_count_from_label(self, text: str) -> Optional[str]:
        patterns = [
            r"いいね！\s*([\d.,万億]+)",
            r"([\d.,万億]+)\s*件のいいね",
            r"([\d.,万億]+)\s*件の「?いいね",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if m:
                return m.group(1)
        return None

    def get_video_like_dates_from_user_page(
        self, user_username: str, max_videos: int = 100
    ) -> List[Dict[str, str]]:
        logger.debug("リールのメタデータを取得します")
        video_stats: List[Dict[str, str]] = []

        self.navigate_to_reels_page(user_username)
        self._random_sleep(5.0, 7.0)

        self.scroll_reels_page(max_videos)
        post_elements = self.driver.find_elements(By.CSS_SELECTOR, self.REEL_ITEM_CONTAINER_SELECTOR)

        for post_element in post_elements[:max_videos]:
            try:
                view_count_text = ""
                try:
                    view_elem = post_element.find_element(
                        By.CSS_SELECTOR, self.REEL_VIEW_COUNT_SELECTOR
                    )
                    view_count_text = (view_elem.text or "").strip()
                except NoSuchElementException:
                    pass

                link_elem = post_element.find_element(By.CSS_SELECTOR, "a[href*='/reel/']")
                video_url = link_elem.get_attribute("href") or ""
                if video_url.startswith("/"):
                    video_url = f"{self.BASE_URL}{video_url}"
                video_id, _ = parse_insta_video_url(video_url)

                thumbnail_url = ""
                try:
                    thumb_elem = post_element.find_element(
                        By.CSS_SELECTOR, self.REEL_THUMBNAIL_SELECTOR
                    )
                    style_attr = thumb_elem.get_attribute("style") or ""
                    m = re.search(r'url\("?([^")]+)"?\)', style_attr)
                    if m:
                        thumbnail_url = m.group(1)
                except NoSuchElementException:
                    pass

                is_pinned = False
                try:
                    pinned_icons = post_element.find_elements(
                        By.CSS_SELECTOR, self.PINNED_SVG_SELECTOR
                    )
                    is_pinned = len(pinned_icons) > 0
                except Exception:
                    pass

                video_stats.append(
                    {
                        "video_url": video_url,
                        "video_id": video_id,
                        "user_username": user_username,
                        "video_thumbnail_url": thumbnail_url,
                    "like_count_text": None,
                    "play_count_text": view_count_text,
                    "crawling_algorithm": "instagram-reels-grid-v1",
                    "is_pinned": is_pinned,
                    }
                )
            except NoSuchElementException:
                logger.warning("リールのメタデータ取得で要素不足が発生しました", exc_info=True)
                continue

        logger.debug(f"リールデータ取得件数: {len(video_stats)} 件")
        return video_stats

    def get_and_save_user_name_datas(self, user_username: str) -> str:
        logger.debug("ユーザーの表示名を取得して保存します")
        user_nickname = user_username
        try:
            # 新UIのニックネーム要素を優先
            name_elem = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.USER_NICKNAME_SELECTOR))
            )
            candidate = (name_elem.text or "").strip()
            if candidate:
                user_nickname = candidate
        except TimeoutException:
            logger.debug("新UIのニックネーム要素が見つかりませんでした")
        except Exception:
            logger.debug("ニックネーム要素取得で予期せぬ例外が発生しました", exc_info=True)

        if user_nickname == user_username:
            try:
                # 旧UIのヘッダー要素をフォールバック
                name_elem = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "header h1, header h2"))
                )
                candidate = (name_elem.text or "").strip()
                if candidate:
                    user_nickname = candidate
            except TimeoutException:
                logger.warning("表示名の取得に失敗したため、ユーザーネームを代用します")
            except Exception:
                logger.debug("ヘッダーからのニックネーム取得で例外", exc_info=True)

        self.favorite_user_repo.save_favorite_user_nickname(user_username, user_nickname)
        return user_nickname

    def parse_and_save_video_light_datas(
        self,
        light_like_datas: List[Dict],
        user_nickname: Optional[str] = None,
        save_light: bool = True,
        publish: bool = True,
    ):
        logger.debug("ライトデータをパースして保存します")
        for like_data in light_like_datas:
            video_title_text = like_data.get("video_title") or ""
            audio_title = like_data.get("audio_title") or like_data.get("audio_info_text")
            play_count_text = like_data.get("play_count_text")
            play_count = parse_insta_number(play_count_text)
            crawled_at = datetime.now()

            light_data = InstaLightRawData(
                id=None,
                video_url=like_data["video_url"],
                video_id=like_data["video_id"],
                user_username=like_data["user_username"],
                video_thumbnail_url=like_data["video_thumbnail_url"],
                play_count_text=play_count_text,
                play_count=play_count,
                crawling_algorithm=like_data["crawling_algorithm"],
                crawled_at=crawled_at,
            )
            if save_light:
                self.video_repo.save_insta_light_data(light_data)

            if publish:
                message_data = {
                    "video_id": light_data.video_id,
                    "video_url": light_data.video_url,
                    "user_username": light_data.user_username,
                    "user_nickname": user_nickname,
                    "video_thumbnail_url": light_data.video_thumbnail_url,
                    "video_title": video_title_text,
                    "like_count": None,
                    "play_count": play_count,
                    "audio_info_text": like_data.get("audio_info_text"),
                    "audio_title": audio_title,
                    "post_time_text": like_data.get("post_time_text"),
                    "post_time_iso": like_data.get("post_time_iso"),
                    "comments_json": like_data.get("comments_json"),
                }
                self._publish_video_master_sync(message_data)

        if save_light:
            logger.info(f"ライトデータを保存しました: {len(light_like_datas)} 件")

    def parse_and_save_video_heavy_datas(
        self,
        light_like_datas: List[Dict],
        user_nickname: Optional[str] = None,
        save_heavy: bool = True,
        publish: bool = True,
    ):
        logger.debug("ヘビーデータをパースして保存します")
        for like_data in light_like_datas:
            video_title_text = like_data.get("video_title") or ""
            post_time_iso = like_data.get("post_time_iso")
            post_time_value = self._parse_datetime_attr(post_time_iso)
            audio_title = like_data.get("audio_title") or like_data.get("audio_info_text")
            play_count_text = like_data.get("play_count_text")
            play_count = parse_insta_number(play_count_text)

            heavy_data = InstaHeavyRawData(
                id=None,
                video_url=like_data["video_url"],
                video_id=like_data["video_id"],
                video_title=video_title_text,
                post_time_text=like_data.get("post_time_text") or "",
                post_time=post_time_value,
                crawling_algorithm=like_data["crawling_algorithm"],
                crawled_at=datetime.now(),
                comments_json=like_data.get("comments_json"),
                audio_title=audio_title,
            )
            if save_heavy:
                self.video_repo.save_insta_heavy_data(heavy_data)

            if publish:
                message_data = {
                    "video_id": heavy_data.video_id,
                    "url": heavy_data.video_url,
                    "username": like_data.get("user_username"),
                    "nickname": user_nickname,
                    "play_count": play_count,
                    "thumbnail_url": like_data.get("video_thumbnail_url"),
                    "video_title": video_title_text,
                    "post_time": post_time_iso,
                    "audio_title": audio_title,
                    "comments_json": like_data.get("comments_json"),
                }
                self._publish_video_master_sync(message_data)

        if save_heavy:
            logger.info(f"ヘビーデータを保存しました: {len(light_like_datas)} 件")

    def get_user_followers_count_from_user_page(self) -> Tuple[Optional[str], Optional[int]]:
        logger.debug("フォロワー数を取得します")
        try:
            elem = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href$='/followers/'] span"))
            )
            raw_text = (elem.get_attribute("title") or elem.text or "").strip()
            # "フォロワー〇〇人" のような装飾を除去して数値化
            cleaned_text = re.sub(r"[フォロワー人,\\s]", "", raw_text)
            return raw_text, parse_insta_number(cleaned_text)
        except TimeoutException:
            logger.warning("フォロワー数の取得に失敗しました", exc_info=True)
        except NoSuchElementException:
            logger.warning("フォロワー数要素が見つかりませんでした", exc_info=True)
        return None, None

    def crawl_user(
        self, user: FavoriteUser, max_videos_per_user: int = 100, mode: str = "both"
    ):
        logger.info(f"ユーザー @{user.favorite_user_username} のライトデータクロールを開始します")
        try:
            self.navigate_to_user_page(user.favorite_user_username)
        except self.InstaUserNotFoundException:
            logger.info(f"ユーザー @{user.favorite_user_username} は存在しないためスキップします")
            self.favorite_user_repo.update_favorite_user_is_alive(user.favorite_user_username, False)
            return False

        followers_text, followers_count = self.get_user_followers_count_from_user_page()
        if followers_text is not None or followers_count is not None:
            collection_date = (datetime.now() - timedelta(days=1)).date()
            try:
                self.favorite_user_repo.upsert_account_follower_history(
                    account_id=user.id,
                    collection_date=collection_date,
                    follower_text=followers_text,
                    follower_count=followers_count,
                )
            except Exception:
                logger.exception("フォロワー履歴の保存に失敗しました")

        user_nickname = self.get_and_save_user_name_datas(user.favorite_user_username)
        light_like_datas = self.get_video_like_dates_from_user_page(
            user.favorite_user_username, max_videos=max_videos_per_user
        )
        if mode == "light":
            self.parse_and_save_video_light_datas(
                light_like_datas,
                user_nickname=user_nickname,
                publish=True,
            )
            self.favorite_user_repo.update_favorite_user_last_crawled(
                user.favorite_user_username, datetime.now()
            )
            logger.info(f"ユーザー @{user.favorite_user_username} のクロールが完了しました")
            return True

        if mode == "both":
            self.parse_and_save_video_light_datas(
                light_like_datas,
                user_nickname=user_nickname,
                publish=False,
            )
        skip_video_ids: Set[str] = set()
        missing_title_ids: List[str] = []
        for like_data in light_like_datas:
            video_id = like_data.get("video_id")
            if not video_id:
                continue
            if (like_data.get("video_title") or "").strip():
                skip_video_ids.add(video_id)
            else:
                missing_title_ids.append(video_id)

        if missing_title_ids:
            existing_ids = self.video_repo.get_insta_video_ids_with_title(missing_title_ids)
            skip_video_ids.update(existing_ids)

        heavy_like_datas = [
            like_data
            for like_data in light_like_datas
            if like_data.get("video_id") and like_data["video_id"] not in skip_video_ids
        ]

        if heavy_like_datas:
            # user_username引数を追加
            heavy_data_map = self.collect_reel_heavy_data_map(
                light_like_datas,
                user_username=user.favorite_user_username,
                fetch_comments=True,
                skip_video_ids=skip_video_ids,
            )
            for like_data in heavy_like_datas:
                heavy = heavy_data_map.get(like_data["video_id"])
                if heavy:
                    like_data.update(
                        {
                            "video_title": heavy.get("video_title"),
                            "audio_info_text": heavy.get("audio_info_text"),
                            "audio_title": heavy.get("audio_info_text"),
                            "post_time_text": heavy.get("post_time_text"),
                            "post_time_iso": heavy.get("post_time_iso"),
                            "comments_json": heavy.get("comments_json"),
                        }
                    )
            self.parse_and_save_video_heavy_datas(
                heavy_like_datas,
                user_nickname=user_nickname,
                publish=False,
            )
        else:
            logger.info("video_titleが既に保存済みのため、heavyデータ取得をスキップします")

        self.favorite_user_repo.update_favorite_user_last_crawled(
            user.favorite_user_username, datetime.now()
        )
        logger.info(f"ユーザー @{user.favorite_user_username} のクロールが完了しました")
        return True

    def crawl_favorite_users(
        self,
        max_videos_per_user: int = 100,
        max_users: int = 10,
        mode: str = "both",
        run_deadline: Optional[float] = None,
    ) -> Tuple[int, bool]:
        logger.info(f"{max_users}件のユーザーに対してライトデータをクロールします")
        favorite_users = self.favorite_user_repo.get_favorite_users(
            self.crawler_account.id, limit=max_users
        )
        if not favorite_users:
            logger.info("クロール対象ユーザーが存在しません")
            return 0, False
        processed = 0
        for user in favorite_users:
            try:
                self.crawl_user(user, max_videos_per_user=max_videos_per_user, mode=mode)
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.exception(
                    f"ユーザー @{user.favorite_user_username} のクロール中にエラーが発生しました"
                )
            finally:
                processed += 1
            if run_deadline is not None and time.monotonic() >= run_deadline:
                logger.info("稼働時間の上限に達したため、このユーザーまでで停止します")
                return processed, True
        logger.info(f"{len(favorite_users)}件のユーザーに対するクロールが完了しました")
        return processed, False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Instagramライトデータクローラー")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["light", "heavy", "both", "test"],
        default="both",
        help="light: ライトのみ, heavy: ヘビーのみ, both: 両方, test: ログインのみで停止",
    )
    parser.add_argument(
        "--crawler-account-id",
        type=int,
        help="利用するクローラーアカウントID（未指定の場合は自動割当）",
    )
    parser.add_argument(
        "--max-videos-per-user",
        type=int,
        default=100,
        help="1ユーザーあたりの最大取得件数（デフォルト: 100）",
    )
    parser.add_argument(
        "--max-users",
        type=int,
        default=50,
        help="クロール対象ユーザー数（デフォルト: 50）",
    )
    parser.add_argument(
        "--device-type",
        choices=["pc", "vps"],
        default="pc",
        help="デバイスタイプ（pc または vps）",
    )
    parser.add_argument(
        "--use-profile",
        action="store_true",
        help="既存のChromeプロファイルを使用してログイン状態を再利用する",
    )
    parser.add_argument(
        "--chrome-user-data-dir",
        help="--use-profile 指定時に利用する Chrome ユーザーデータディレクトリ",
    )
    parser.add_argument(
        "--chrome-profile-directory",
        help="--use-profile 指定時に利用するプロファイルディレクトリ名 (例: Default, Profile 1)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Instagramホーム到達までのログイン確認のみを行い、到達後すぐ終了する",
    )
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="プロキシを使わず直接接続する",
    )
    args = parser.parse_args()

    mode = args.mode

    if args.use_profile and not args.chrome_user_data_dir:
        parser.error("--use-profile を使う場合は --chrome-user-data-dir を指定してください")
    if not args.use_profile:
        args.chrome_user_data_dir = None
        args.chrome_profile_directory = None

    with Database() as db:
        crawler_account_repo = InstaCrawlerAccountRepository(db)
        favorite_user_repo = InstaFavoriteUserRepository(db)
        video_repo = InstaVideoRepository(db)

        if args.test or mode == "test":
            crawler = InstaCrawler(
                crawler_account_repo=crawler_account_repo,
                favorite_user_repo=favorite_user_repo,
                video_repo=video_repo,
                crawler_account_id=args.crawler_account_id,
                sadcaptcha_api_key=os.getenv("SADCAPTCHA_API_KEY"),
                device_type=args.device_type,
                use_profile=args.use_profile,
                chrome_user_data_dir=args.chrome_user_data_dir,
                chrome_profile_directory=args.chrome_profile_directory,
                skip_login=True,
                use_proxy=not args.no_proxy,
            )
            try:
                crawler.__enter__()
                logger.info("testモード: Instagramホームに到達しました。このまま調査してください。終了するには Enter を押してください。")
                input()
            finally:
                crawler.__exit__(None, None, None)
        else:
            if RUN_MINUTES <= 0 or REST_MINUTES <= 0:
                raise ValueError("RUN_MINUTES と REST_MINUTES は1以上に設定してください")
            run_seconds = RUN_MINUTES * 60
            rest_seconds = REST_MINUTES * 60
            while True:
                with InstaCrawler(
                    crawler_account_repo=crawler_account_repo,
                    favorite_user_repo=favorite_user_repo,
                    video_repo=video_repo,
                    crawler_account_id=args.crawler_account_id,
                    sadcaptcha_api_key=os.getenv("SADCAPTCHA_API_KEY"),
                    device_type=args.device_type,
                    use_profile=args.use_profile,
                    chrome_user_data_dir=args.chrome_user_data_dir,
                    chrome_profile_directory=args.chrome_profile_directory,
                    use_proxy=not args.no_proxy,
                ) as crawler:
                    run_deadline = time.monotonic() + run_seconds
                    crawler.crawl_favorite_users(
                        max_videos_per_user=args.max_videos_per_user,
                        max_users=args.max_users,
                        mode=mode,
                        run_deadline=run_deadline,
                    )
                logger.info(f"{REST_MINUTES}分休憩します")
                time.sleep(rest_seconds)


if __name__ == "__main__":
    import sys

    try:
        main()
    except KeyboardInterrupt:
        logger.info("ユーザーによって中断されました")
        sys.exit(130)
    except Exception:
        logger.exception("予期しないエラーが発生しました")
        sys.exit(1)
