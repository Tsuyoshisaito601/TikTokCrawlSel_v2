import json
import os
import sys
import time
import subprocess
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional

from google.cloud import pubsub_v1

DEFAULT_STATE_DIR = r"\Users\Administrator\TikTokCrawlSel_v2\crawler_state"
THRESHOLD_SEC = 48 * 3600  # 48 hours

def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

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

def should_run(subscription_name: str, state_dir: Optional[str], working_dir: str) -> bool:
    f = state_file_path(subscription_name, state_dir, working_dir)
    try:
        with open(f, "r", encoding="utf-8") as fp:
            last = float(fp.read().strip())
    except Exception:
        last = 0.0
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

    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(project_id, subscription)

    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            body = json.loads(message.data.decode("utf-8")) if message.data else {}
        except Exception:
            body = {}

        if not should_run(subscription, state_dir, working_dir):
            print(f"[{subscription}] Skip (within 48h). ACK.")
            message.ack()
            return

        if not acquire_lock(subscription, state_dir, working_dir):
            print(f"[{subscription}] Lock exists. NACK for retry.")
            message.nack()
            return

        cmd = build_command(python_path, body, extra_args)
        try:
            print(f"[{subscription}] Executing: " + " ".join([f'\"{c}\"' if " " in str(c) else str(c) for c in cmd]))
            subprocess.run(cmd, check=True, cwd=working_dir)
            mark_success(subscription, state_dir, working_dir)
            message.ack()
            print(f"[{subscription}] Done and ACKed.")
        except subprocess.CalledProcessError as e:
            print(f"[{subscription}] Subprocess failed: {e}. NACK.", file=sys.stderr)
            message.nack()
        except Exception as e:
            print(f"[{subscription}] Unexpected error: {e}. NACK.", file=sys.stderr)
            message.nack()
        finally:
            release_lock(subscription, state_dir, working_dir)

    streaming_pull_future = subscriber.subscribe(
        sub_path,
        callback=callback,
        flow_control=pubsub_v1.types.FlowControl(max_messages=1)
    )
    print(f"[{subscription}] Listening on {sub_path} ...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
    except Exception as e:
        print(f"[{subscription}] Stream error: {e}", file=sys.stderr)
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