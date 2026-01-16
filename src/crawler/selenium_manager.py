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
from typing import Optional
from ..logger import setup_logger
import tempfile, shutil, os, time, logging, subprocess

logger = setup_logger(__name__)

class SeleniumManager:
    def __init__(self, proxy: str = None, sadcaptcha_api_key: str = None, device_type: str = "pc", use_profile: bool = False, user_data_dir: Optional[str] = None, profile_directory: Optional[str] = None):
        self.driver = None
        self.solver = None
        self.proxy = proxy
        self.sadcaptcha_api_key = sadcaptcha_api_key
        self.device_type = device_type
        self.use_profile = use_profile
        self.user_data_dir = user_data_dir
        self.profile_directory = profile_directory
        self._temp_profile_dir: Optional[str] = None
        self._chrome_debugger_address: Optional[str] = None
        self._chrome_pid: Optional[int] = None
        self._chrome_log_path: Optional[str] = None

    def setup_driver(self):
        try:
            device_str = "モバイル用" if self.device_type == "mobile" else ""
            if self.use_profile:
                if not self.user_data_dir:
                    raise ValueError("user_data_dir must be provided when use_profile is True")
                profile_dir = self.user_data_dir
                self._temp_profile_dir = None
            else:
                profile_dir = tempfile.mkdtemp(prefix="sel_profile_")
                self._temp_profile_dir = profile_dir
            # 既存のオプション設定
            options = uc.ChromeOptions()
            if self.proxy:
                options.add_argument(f'--proxy-server={self.proxy}')
            
            # 基本設定
            options.add_argument(f"--user-data-dir={profile_dir}")
            if self.use_profile and self.profile_directory:
                options.add_argument(f"--profile-directory={self.profile_directory}")
            self._chrome_log_path = os.path.join(profile_dir, "chrome_debug.log")
            options.add_argument("--enable-logging")
            options.add_argument("--v=1")
            options.add_argument(f"--log-file={self._chrome_log_path}")
            logger.info("Chrome log file configured. path=%s", self._chrome_log_path)
            options.add_argument('--no-sandbox')
            options.add_argument('--mute-audio')
            options.add_argument('--use-angle=gl')
            options.add_argument('--enable-features=Vulkan,VaapiVideoDecoder')
            options.add_argument('--disable-vulkan-surface')
            options.add_argument('--enable-gpu-rasterization')
            options.add_argument('--enable-zero-copy')
            options.add_argument('--ignore-gpu-blocklist')
            options.add_argument('--enable-hardware-overlays')
            options.add_argument('--start-maximized')
            
            # PC用の追加フラグ
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

                # Chromium "features" として実装された追加抑制を止める
                '--disable-features='
                'CalculateBackgroundVideoPlaybackMinFrameRate,'
                'PauseBackgroundTabsMediaToggle,'
                'IntensiveWakeUpThrottling'
            ]
            for f in extra_flags:
                options.add_argument(f)

            # 以下のオプションを追加することを推奨
            options.add_argument("--disable-blink-features=AutomationControlled")

            self.driver = uc.Chrome(options=options)
            self._cache_chrome_process_info()
            self._log_chrome_process_snapshot("after_start")

            # PC用のエミュレーション設定
            if self.device_type == "pc":
                self.driver.execute_cdp_cmd(
                    "Emulation.setDeviceMetricsOverride",
                    {
                        "width": 1920,
                        "height": 1080,
                        "deviceScaleFactor": 1,
                        "mobile": False,
                        "screenWidth": 1920,
                        "screenHeight": 1080,
                        "positionX": 0,
                        "positionY": 0,
                    },
                )    
            elif self.device_type == "vps":
                self.driver.execute_cdp_cmd(
                    "Emulation.setDeviceMetricsOverride",
                    {
                        "width": 1024,
                        "height": 576,
                        "deviceScaleFactor": 1,
                        "mobile": False,
                        "screenWidth": 1024,
                        "screenHeight": 576,
                        "positionX": 0,
                        "positionY": 0,
                    },
                )   

            if self.sadcaptcha_api_key:
                # CAPTCHA Solver使用時
                device_str = "モバイル用" if self.device_type == "mobile" else ""
                logger.info(f"{device_str}CAPTCHA Solver付きのドライバーを作成します")
                self.solver = SeleniumSolver(
                    self.driver,
                    self.sadcaptcha_api_key  # オプションを渡す
                )
            

            stealth(
                self.driver,
                languages=["ja-JP", "ja"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="WebKit",
                renderer="WebKit WebGL",
                fix_hairline=True,
            )
            
            # より包括的なウェブドライバー検出回避
            js_script = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: function() { return []; }});
            Object.defineProperty(navigator, 'languages', {get: function() { return ['ja-JP', 'ja']; }});
            Object.defineProperty(navigator, 'maxTouchPoints', {get: function() { return 5; }});
            window.chrome = undefined;
            """
            self.driver.execute_script(js_script)
            
            logger.info(f"{device_str}Chromeドライバーの設定が完了しました")
            return self.driver
        
        except Exception as e:
            logger.error(f"{device_str}Chromeドライバーの設定中にエラーが発生しました: {e}")
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
            self._log_chrome_process_snapshot("before_quit")
            self.driver.quit()
            logger.info("Chromeドライバーを終了しました")
            self._log_chrome_process_snapshot("after_quit")
        if self._temp_profile_dir and os.path.isdir(self._temp_profile_dir):
            shutil.rmtree(self._temp_profile_dir, ignore_errors=True)
            self._temp_profile_dir = None

    def _cache_chrome_process_info(self) -> None:
        self._chrome_debugger_address = self._get_debugger_address()
        self._chrome_pid = self._pid_from_debugger_address(self._chrome_debugger_address)
        chromedriver_pid = self._get_chromedriver_pid()
        logger.info(
            "Chrome process detected. pid=%s debugger_address=%s chromedriver_pid=%s",
            self._chrome_pid,
            self._chrome_debugger_address,
            chromedriver_pid,
        )

    def _get_debugger_address(self) -> Optional[str]:
        try:
            chrome_options = self.driver.capabilities.get("goog:chromeOptions", {})
            return chrome_options.get("debuggerAddress")
        except Exception:
            return None

    def _get_chromedriver_pid(self) -> Optional[int]:
        try:
            service = getattr(self.driver, "service", None)
            process = getattr(service, "process", None)
            return getattr(process, "pid", None)
        except Exception:
            return None

    def _pid_from_debugger_address(self, debugger_address: Optional[str]) -> Optional[int]:
        if os.name != "nt" or not debugger_address:
            return None
        try:
            port_str = debugger_address.rsplit(":", 1)[-1]
            port = int(port_str)
        except (ValueError, AttributeError):
            return None
        try:
            output = subprocess.check_output(
                "netstat -ano -p tcp",
                shell=True,
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return None
        target = f":{port} "
        for line in output.splitlines():
            if target in line and "LISTENING" in line:
                parts = line.split()
                if parts:
                    pid_str = parts[-1]
                    if pid_str.isdigit():
                        return int(pid_str)
        return None

    def _get_process_command_line(self, pid: Optional[int]) -> Optional[str]:
        if os.name != "nt" or not pid:
            return None
        try:
            cmd = [
                "powershell",
                "-NoProfile",
                "-Command",
                f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine",
            ]
            output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
            return output or None
        except Exception:
            return None

    def _process_exists(self, pid: Optional[int]) -> Optional[bool]:
        if os.name != "nt" or not pid:
            return None
        try:
            output = subprocess.check_output(
                ["tasklist", "/FI", f"PID eq {pid}"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            return str(pid) in output
        except Exception:
            return None

    def _log_chrome_process_snapshot(self, stage: str) -> None:
        if not self._chrome_pid and self.driver:
            self._cache_chrome_process_info()
        cmdline = self._get_process_command_line(self._chrome_pid)
        exists = self._process_exists(self._chrome_pid)
        logger.info(
            "Chrome process snapshot. stage=%s pid=%s exists=%s debugger_address=%s cmdline=%s",
            stage,
            self._chrome_pid,
            exists,
            self._chrome_debugger_address,
            cmdline,
        )
