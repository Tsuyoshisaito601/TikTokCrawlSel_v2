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

def build_command(python_path: str, body: Dict[str, Any], extra_args: List[str]) -> List[str]:
    base = [python_path, "-m", "src.crawler.tiktok_crawler"]
    args = body.get("args", [])
    return base + list(args) + list(extra_args or [])

def worker(project_id: str, subcfg: Dict[str, Any], credentials_path: Optional[str] = None):
    subscription = subcfg["subscription_name"]
    working_dir = subcfg["working_dir"]
    python_path = subcfg["python_path"]
    extra_args = subcfg.get("extra_args", [])
    log_dir = subcfg.get("log_dir") or os.path.join(working_dir, "logs")
    logger = setup_logger(subscription, log_dir)

    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    flow_control = pubsub_v1.types.FlowControl(
        max_messages=1,
        max_lease_duration=24 * 3600,
    )
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(project_id, subscription)
    logger.info("Worker initialized. subscription=%s working_dir=%s python_path=%s", subscription, working_dir, python_path)

    def callback(message: pubsub_v1.subscriber.message.Message):
        msg_id = message.message_id
        logger.info("Callback start. message_id=%s", msg_id)
        try:
            body = json.loads(message.data.decode("utf-8")) if message.data else {}
        except Exception:
            raw = message.data.decode("utf-8", errors="replace") if message.data else ""
            body = {}
            logger.warning("JSON decode failed. message_id=%s raw=%s", msg_id, raw)

        logger.info(
            "Message received. message_id=%s attributes=%s data_len=%s body=%s",
            msg_id,
            message.attributes,
            len(message.data or b""),
            json.dumps(body, ensure_ascii=True),
        )

        cmd = build_command(python_path, body, extra_args)
        try:
            cmd_display = " ".join([f'\"{c}\"' if " " in str(c) else str(c) for c in cmd])
            logger.info("Subprocess starting. message_id=%s cmd=%s", msg_id, cmd_display)
            start = time.time()
            result = subprocess.run(cmd, check=True, cwd=working_dir, capture_output=True, text=True)
            elapsed = time.time() - start
            logger.info("Subprocess finished. message_id=%s returncode=0 elapsed_sec=%.2f", msg_id, elapsed)
            if result.stdout:
                logger.info("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.warning("stderr:\n%s", result.stderr.rstrip())
            logger.info("ACKing message. message_id=%s", msg_id)
            message.ack()
            logger.info("Done and ACKed. message_id=%s elapsed_sec=%.2f", msg_id, elapsed)
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start if "start" in locals() else None
            if elapsed is not None:
                logger.error("Subprocess failed. message_id=%s returncode=%s elapsed_sec=%.2f NACK.", msg_id, e.returncode, elapsed)
            else:
                logger.error("Subprocess failed. message_id=%s returncode=%s NACK.", msg_id, e.returncode)
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.rstrip())
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.rstrip())
            logger.info("NACKing message. message_id=%s", msg_id)
            message.nack()
        except Exception as e:
            logger.exception("Unexpected error. message_id=%s NACK.", msg_id)
            logger.info("NACKing message. message_id=%s", msg_id)
            message.nack()
        finally:
            logger.info("Callback end. message_id=%s", msg_id)

    streaming_pull_future = subscriber.subscribe(
        sub_path,
        callback=callback,
        flow_control=flow_control,
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
