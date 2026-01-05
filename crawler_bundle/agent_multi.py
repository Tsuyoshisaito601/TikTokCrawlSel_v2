import json
import os
import sys
import time
import subprocess
import threading
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from google.cloud import pubsub_v1

DEFAULT_STATE_DIR = r"\Users\Administrator\TikTokCrawlSel_v2\crawler_state"
THRESHOLD_SEC = 48 * 3600  # 48 hours

def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def setup_logger(subscription: str, log_dir: str) -> logging.Logger:
    ensure_dir(log_dir)
    logger = logging.getLogger(f"agent_multi.{subscription}")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_path = os.path.join(log_dir, f"agent_multi-{subscription}.log")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info("Logger initialized. log_path=%s", log_path)
    return logger

def _base_state_dir(state_dir: Optional[str], working_dir: str) -> str:
    if state_dir:
        return state_dir
    # fallback to DEFAULT_STATE_DIR; if it's blank for some reason, use working_dir\.state
    base = DEFAULT_STATE_DIR or os.path.join(working_dir, ".state")
    return base

def state_file_path(subscription_name: str, state_dir: Optional[str], working_dir: str) -> str:
    base = _base_state_dir(state_dir, working_dir)
    ensure_dir(base)
    return os.path.join(base, f"{subscription_name}.last")

def lock_file_path(subscription_name: str, state_dir: Optional[str], working_dir: str) -> str:
    base = _base_state_dir(state_dir, working_dir)
    ensure_dir(base)
    return os.path.join(base, f"{subscription_name}.lock")

def read_last_run(subscription_name: str, state_dir: Optional[str], working_dir: str) -> float:
    f = state_file_path(subscription_name, state_dir, working_dir)
    try:
        with open(f, "r", encoding="utf-8") as fp:
            return float(fp.read().strip())
    except Exception:
        return 0.0

def should_run(subscription_name: str, state_dir: Optional[str], working_dir: str) -> bool:
    last = read_last_run(subscription_name, state_dir, working_dir)
    now = time.time()
    return (now - last) >= THRESHOLD_SEC

def mark_success(subscription_name: str, state_dir: Optional[str], working_dir: str) -> None:
    f = state_file_path(subscription_name, state_dir, working_dir)
    tmp = f + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fp:
        fp.write(str(time.time()))
    os.replace(tmp, f)

def acquire_lock(subscription_name: str, state_dir: Optional[str], working_dir: str) -> bool:
    lock_path = lock_file_path(subscription_name, state_dir, working_dir)
    try:
        with open(lock_path, "x", encoding="utf-8") as fp:
            fp.write(str(os.getpid()))
        return True
    except FileExistsError:
        return False

def release_lock(subscription_name: str, state_dir: Optional[str], working_dir: str) -> None:
    try:
        os.remove(lock_file_path(subscription_name, state_dir, working_dir))
    except FileNotFoundError:
        pass

def build_command(python_path: str, body: Dict[str, Any], extra_args: List[str]) -> List[str]:
    base = [python_path, "-m", "src.crawler.tiktok_crawler"]
    args = body.get("args", [])
    return base + list(args) + list(extra_args or [])

def worker(project_id: str, subcfg: Dict[str, Any], credentials_path: Optional[str] = None):
    subscription = subcfg["subscription_name"]
    working_dir = subcfg["working_dir"]
    python_path = subcfg["python_path"]
    extra_args = subcfg.get("extra_args", [])
    state_dir = subcfg.get("state_dir")  # optional
    log_dir = subcfg.get("log_dir") or os.path.join(working_dir, "logs")
    logger = setup_logger(subscription, log_dir)

    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(project_id, subscription)
    logger.info("Worker initialized. subscription=%s working_dir=%s python_path=%s", subscription, working_dir, python_path)

    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            body = json.loads(message.data.decode("utf-8")) if message.data else {}
        except Exception:
            raw = message.data.decode("utf-8", errors="replace") if message.data else ""
            body = {}
            logger.warning("JSON decode failed. message_id=%s raw=%s", message.message_id, raw)

        logger.info(
            "Message received. message_id=%s attributes=%s data_len=%s body=%s",
            message.message_id,
            message.attributes,
            len(message.data or b""),
            json.dumps(body, ensure_ascii=True),
        )

        last = read_last_run(subscription, state_dir, working_dir)
        now = time.time()
        age = now - last if last else None
        if age is not None and age < THRESHOLD_SEC:
            logger.info("Skip (within 48h). last_run_age_sec=%.1f ACK.", age)
            message.ack()
            return

        if not acquire_lock(subscription, state_dir, working_dir):
            logger.warning("Lock exists. NACK for retry.")
            message.nack()
            return

        cmd = build_command(python_path, body, extra_args)
        try:
            cmd_display = " ".join([f'\"{c}\"' if " " in str(c) else str(c) for c in cmd])
            logger.info("Executing: %s", cmd_display)
            start = time.time()
            result = subprocess.run(cmd, check=True, cwd=working_dir, capture_output=True, text=True)
            elapsed = time.time() - start
            if result.stdout:
                logger.info("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.warning("stderr:\n%s", result.stderr.rstrip())
            mark_success(subscription, state_dir, working_dir)
            message.ack()
            logger.info("Done and ACKed. elapsed_sec=%.2f", elapsed)
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start if "start" in locals() else None
            if elapsed is not None:
                logger.error("Subprocess failed. returncode=%s elapsed_sec=%.2f NACK.", e.returncode, elapsed)
            else:
                logger.error("Subprocess failed. returncode=%s NACK.", e.returncode)
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.rstrip())
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.rstrip())
            message.nack()
        except Exception as e:
            logger.exception("Unexpected error. NACK.")
            message.nack()
        finally:
            release_lock(subscription, state_dir, working_dir)
            logger.info("Lock released.")

    streaming_pull_future = subscriber.subscribe(
        sub_path,
        callback=callback,
        flow_control=pubsub_v1.types.FlowControl(max_messages=1)
    )
    logger.info("Listening on %s ...", sub_path)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
    except Exception as e:
        logger.error("Stream error: %s", e)
        streaming_pull_future.cancel()

def main():
    cfg_path = Path(__file__).with_name("agent_config.json")
    if not cfg_path.exists():
        print(f"[FATAL] Config not found: {cfg_path}", file=sys.stderr)
        sys.exit(1)
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    project_id = cfg["project_id"]
    credentials_path = cfg.get("credentials_path")
    subs = cfg.get("subscriptions")

    if not subs:
        # fallback to single-subscription keys
        subs = [{
            "subscription_name": cfg["subscription_name"],
            "working_dir": cfg["working_dir"],
            "python_path": cfg["python_path"],
            "extra_args": cfg.get("extra_args", []),
            "state_dir": cfg.get("state_dir")
        }]

    threads = []
    for subcfg in subs:
        t = threading.Thread(target=worker, args=(project_id, subcfg, credentials_path), daemon=True)
        t.start()
        threads.append(t)

    print(f"[MAIN] Started {len(threads)} worker(s). Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("[MAIN] Shutting down...")

if __name__ == "__main__":
    main()
