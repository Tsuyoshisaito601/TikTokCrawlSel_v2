from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
import undetected_chromedriver as uc
from tiktok_captcha_solver import SeleniumSolver  # CAPTCHAソルバー用
from tiktok_captcha_solver.captchatype import CaptchaType
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    ElementClickInterceptedException,
)
from concurrent.futures import ThreadPoolExecutor
from ..logger import setup_logger
import tempfile, shutil, os, time, logging

logger = setup_logger(__name__)

class SeleniumManager:
    def __init__(self, proxy: str = None, sadcaptcha_api_key: str = None):
        self.driver = None
        self.solver = None  
        self.proxy = proxy
        self.sadcaptcha_api_key = sadcaptcha_api_key

    def setup_driver(self):
        try:
            profile_dir = tempfile.mkdtemp(prefix="sel_profile_")
            # 共通のオプション設定
            options = uc.ChromeOptions()
            if self.proxy:
                options.add_argument(f'--proxy-server={self.proxy}')
            
            # その他の設定
            options.add_argument(f"--user-data-dir={profile_dir}")
            options.add_argument('--no-sandbox')
            options.add_argument('--use-angle=gl')
            options.add_argument('--enable-features=Vulkan,VaapiVideoDecoder')
            options.add_argument('--disable-vulkan-surface')
            options.add_argument('--enable-gpu-rasterization')
            options.add_argument('--enable-zero-copy')
            options.add_argument('--ignore-gpu-blocklist')
            options.add_argument('--enable-hardware-overlays')
            options.add_argument('--mute-audio')
            options.add_argument('--start-maximized')
            extra_flags = [
                # JS タイマー／Renderer を背景でも止めない
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',

                # 自動タブ破棄（メモリ不足時にプロセス kill）の無効化
                '--disable-tab-discarding',

                # バッテリーセーバーや Energy Saver モードを無効化
                '--battery-saver-mode=disable',

                # オートプレイ規制を緩和（音付きでもユーザー操作不要）
                '--autoplay-policy=no-user-gesture-required',

                # Chromium “features” として実装された追加抑制を止める
                '--disable-features='
                'CalculateBackgroundVideoPlaybackMinFrameRate,'
                'PauseBackgroundTabsMediaToggle,'
                'IntensiveWakeUpThrottling'
            ]
            for f in extra_flags:
                options.add_argument(f)

            self.driver = uc.Chrome(options=options)

            self.driver.execute_cdp_cmd(
                "Emulation.setDeviceMetricsOverride",
                {
                    "width": 1920,
                    "height": 1080,
                    "deviceScaleFactor": 1,
                    "mobile": False,          # ← これが最重要
                    "screenWidth": 1920,      # ↓ 省略可だが揃えておくと安全
                    "screenHeight": 1080,
                    "positionX": 0,
                    "positionY": 0,
                },
            )    

            if self.sadcaptcha_api_key:
                # CAPTCHA Solver使用時
                logger.info("CAPTCHA Solver付きのドライバーを作成します")
                self.solver = SeleniumSolver(
                    self.driver,
                    self.sadcaptcha_api_key  # オプションを渡す
                )
            else:
                # 通常のSeleniumドライバーを使用
                service = Service()
                self.driver = webdriver.Chrome(service=service, options=options)
            
            # 共通の設定
            stealth(
                self.driver,
                languages=["ja-JP", "ja"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="WebKit",
                renderer="WebKit WebGL",
                fix_hairline=True,
            )
            
            # WebDriver検出防止のJavaScript
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            
            logger.info("Chromeドライバーの設定が完了しました")
            return self.driver
        
        except Exception as e:
            logger.error(f"Chromeドライバーの設定中にエラーが発生しました: {e}")
            raise

    def check_and_solve_captcha(self):
        """CAPTCHAが存在するかチェックし、存在する場合は解決を試みる"""
        TIMEOUT_PER_ATTEMPT = 10          # solve_* 1 回あたりの制限秒数
        SLEEP_BETWEEN_ATTEMPTS = 3
        MAX_ATTEMPTS = 10
        if not self.solver:
            return False

        try:
            present = self.solver.captcha_is_present()
            logger.debug(f"captcha_present={present}")
            if not present:
                return False 
            

            for attempt in range(1,MAX_ATTEMPTS+1):
                logger.info(f"[{attempt}/{MAX_ATTEMPTS}] CAPTCHA 解決を試行中…")
                captcha_type = self.solver.identify_captcha()

                def _solve():                
                    if captcha_type == CaptchaType.ROTATE_V2:
                        return self.solver.solve_rotate_v2()
                    elif captcha_type == CaptchaType.SHAPES_V1:
                        return self.solver.solve_shapes()
                    elif captcha_type == CaptchaType.SHAPES_V2:
                        return self.solver.solve_shapes_v2()
                    elif captcha_type == CaptchaType.ROTATE_V1:
                        return self.solver.solve_rotate()
                    elif captcha_type == CaptchaType.ICON_V1:
                        return self.solver.solve_icon()
                    elif captcha_type == CaptchaType.ICON_V2:
                        return self.solver.solve_icon_v2()
                    elif captcha_type == CaptchaType.PUZZLE_V2:
                        return self.solver.solve_puzzle_v2()
                    elif captcha_type == CaptchaType.PUZZLE_V1:
                        return self.solver.solve_puzzle()


                with ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(_solve)
                    try:
                        ok = future.result(timeout=TIMEOUT_PER_ATTEMPT)
                    except TimeoutError:
                        logger.error(f"CAPTCHA解決をリフレッシュします。")
                        ok = False
                        try:
                            refresh_btn = self.driver.find_element(By.ID, "captcha_refresh_button")
                            refresh_btn.click()
                            logger.debug("リフレッシュボタンをクリックしました")
                        except (NoSuchElementException, ElementClickInterceptedException) as e:
                            logger.debug(f"リフレッシュボタンが押せませんでした: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"CAPTCHA解決中にエラーが発生しました: {e}")
                        return False
                    logger.debug(f"solve_{captcha_type}() => {ok}")

                if ok or not self.solver.captcha_is_present(timeout=3):
                    logger.info("CAPTCHAの解決が完了しました")
                    return True
        
                logger.info(f"CAPTCHA まだ残存。{SLEEP_BETWEEN_ATTEMPTS}s 待って再試行")
                time.sleep(SLEEP_BETWEEN_ATTEMPTS)   
            logger.warning("最大試行回数に達しました。CAPTCHA 解決失敗")
            return False         
        except Exception as e:
            logger.error(f"CAPTCHA 解決中に例外が発生: %s", e)
            return False

    def quit_driver(self):
        if self.driver:
            self.driver.quit()
            logger.info("Chromeドライバーを終了しました")
