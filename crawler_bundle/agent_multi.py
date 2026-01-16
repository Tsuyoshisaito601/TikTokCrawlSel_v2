import base64
import json
import os
import sys
import time
import subprocess
import threading
import logging
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import mysql.connector
from dotenv import load_dotenv
from google.cloud import pubsub_v1

ERROR_EXIT_CODE_TO_GENRE = {
    41: "proxy_block",
    42: "chrome_version",
    43: "other_process_exist",
    44: "unknown",
}
PROXY_BLOCK_RETRY_DELAY_SEC = 300

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

def _queue_dir_path(queue_dir: Optional[str], subscription: str, working_dir: str) -> str:
    base = queue_dir or os.path.join(working_dir, "queue")
    path = os.path.join(base, subscription)
    ensure_dir(path)
    return path

def _sanitize_message_id(message_id: str) -> str:
    return "".join(c for c in message_id if c.isalnum() or c in ("-", "_"))

def _write_json(path: str, payload: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=True)
    os.replace(tmp, path)

def _load_db_config(working_dir: str, logger: logging.Logger) -> Optional[Dict[str, str]]:
    env_path = Path(working_dir) / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    config = {
        "host": os.getenv("MYSQL_HOST", ""),
        "user": os.getenv("MYSQL_USER", ""),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", ""),
    }
    if not all(config.values()):
        masked = {key: bool(value) for key, value in config.items()}
        logger.warning("DB config missing. skip error log. config=%s", masked)
        return None
    return config

def _insert_error_log(
    db_config: Optional[Dict[str, str]],
    subscription: str,
    error_genre: str,
    logger: logging.Logger,
) -> bool:
    if not db_config:
        return False
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO crawler_error_logs (subscription_name, error_genre, created_at)
            VALUES (%s, %s, NOW())
            """,
            (subscription, error_genre),
        )
        conn.commit()
        cursor.close()
        logger.info("Error log saved. subscription=%s error_genre=%s", subscription, error_genre)
        return True
    except mysql.connector.Error:
        logger.exception("Error log insert failed. subscription=%s error_genre=%s", subscription, error_genre)
        return False
    finally:
        if conn:
            conn.close()

def _error_genre_from_returncode(returncode: int) -> Optional[str]:
    return ERROR_EXIT_CODE_TO_GENRE.get(returncode)

def _retry_policy(error_genre: Optional[str], default_max_retries: int) -> Tuple[int, int]:
    if default_max_retries <= 0:
        return 0, 0
    if error_genre == "proxy_block":
        return default_max_retries, PROXY_BLOCK_RETRY_DELAY_SEC
    return 1, 0

def _save_queue_message(queue_dir: str, message_id: str, data: bytes, attributes: Dict[str, str]) -> Dict[str, Any]:
    safe_id = _sanitize_message_id(message_id)
    ts_ms = int(time.time() * 1000)
    suffix = uuid.uuid4().hex[:8]
    path = os.path.join(queue_dir, f"{ts_ms}_{safe_id}_{suffix}.json")
    payload = {
        "message_id": message_id,
        "received_at": time.time(),
        "attributes": attributes,
        "data_b64": base64.b64encode(data or b"").decode("ascii"),
        "attempts": 0,
    }
    _write_json(path, payload)
    payload["queue_path"] = path
    return payload

def _load_queue_message(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            payload = json.load(fp)
        payload["queue_path"] = path
        return payload
    except Exception:
        return None

def _parse_body(data: bytes, logger: logging.Logger, message_id: str) -> Dict[str, Any]:
    try:
        return json.loads(data.decode("utf-8")) if data else {}
    except Exception:
        raw = data.decode("utf-8", errors="replace") if data else ""
        logger.warning("JSON decode failed. message_id=%s raw=%s", message_id, raw)
        return {}

def _parse_retry_count(attributes: Optional[Dict[str, str]], logger: logging.Logger) -> int:
    if not attributes:
        return 0
    raw = attributes.get("retry_count")
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid retry_count attribute: %s", raw)
        return 0

def worker(project_id: str, subcfg: Dict[str, Any], credentials_path: Optional[str] = None):
    subscription = subcfg["subscription_name"]
    working_dir = subcfg["working_dir"]
    python_path = subcfg["python_path"]
    extra_args = subcfg.get("extra_args", [])
    log_dir = subcfg.get("log_dir") or os.path.join(working_dir, "logs")
    logger = setup_logger(subscription, log_dir)
    db_config = _load_db_config(working_dir, logger)
    queue_dir = _queue_dir_path(subcfg.get("queue_dir"), subscription, working_dir)
    logger.info("Queue dir initialized. path=%s", queue_dir)
    retry_topic = subcfg.get("retry_topic")
    max_retries = int(subcfg.get("max_retries", 0) or 0)

    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    flow_control = pubsub_v1.types.FlowControl(
        max_messages=1,
        max_lease_duration=24 * 3600,
    )
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(project_id, subscription)
    logger.info("Worker initialized. subscription=%s working_dir=%s python_path=%s", subscription, working_dir, python_path)

    publisher = None
    retry_topic_path = None
    if retry_topic and max_retries > 0:
        publisher = pubsub_v1.PublisherClient()
        retry_topic_path = publisher.topic_path(project_id, retry_topic)
        logger.info("Retry publisher initialized. topic=%s max_retries=%s", retry_topic_path, max_retries)

    def process_pending_queue():
        try:
            entries = [p for p in os.listdir(queue_dir) if p.endswith(".json")]
        except FileNotFoundError:
            return
        if not entries:
            return
        entries.sort()
        logger.info("Processing pending queue. dir=%s count=%s", queue_dir, len(entries))
        for name in entries:
            path = os.path.join(queue_dir, name)
            payload = _load_queue_message(path)
            if not payload:
                bad_path = path + ".bad"
                try:
                    os.replace(path, bad_path)
                    logger.warning("Queue file invalid. moved=%s", bad_path)
                except Exception:
                    logger.exception("Queue file invalid and could not be moved. path=%s", path)
                continue
            msg_id = payload.get("message_id", name)
            attributes = payload.get("attributes") or {}
            data_b64 = payload.get("data_b64") or ""
            try:
                data = base64.b64decode(data_b64)
            except Exception:
                logger.exception("Queue file base64 decode failed. path=%s", path)
                continue
            retry_count = _parse_retry_count(attributes, logger)
            process_payload(msg_id, data, attributes, retry_count, payload, "queue")

    def publish_retry(
        msg_id: str,
        data: bytes,
        attributes: Dict[str, str],
        retry_count: int,
        reason: str,
        error_genre: Optional[str],
    ) -> bool:
        if not publisher or not retry_topic_path:
            logger.warning("Retry skipped (publisher not configured). message_id=%s reason=%s", msg_id, reason)
            return False
        effective_max_retries, delay_seconds = _retry_policy(error_genre, max_retries)
        if retry_count >= effective_max_retries:
            logger.warning("Retry skipped (max reached). message_id=%s retry_count=%s reason=%s", msg_id, retry_count, reason)
            return False
        next_count = retry_count + 1
        retry_attributes = dict(attributes or {})
        retry_attributes["retry_count"] = str(next_count)
        retry_attributes.setdefault("origin_message_id", msg_id)
        retry_attributes.setdefault("origin_subscription", subscription)
        if error_genre:
            retry_attributes.setdefault("error_genre", error_genre)
        if delay_seconds > 0:
            logger.warning(
                "Retry delayed. message_id=%s delay_sec=%s reason=%s error_genre=%s",
                msg_id,
                delay_seconds,
                reason,
                error_genre,
            )
            time.sleep(delay_seconds)
        try:
            future = publisher.publish(retry_topic_path, data or b"", **retry_attributes)
            publish_id = future.result()
            logger.info(
                "Retry published. message_id=%s retry_count=%s publish_id=%s reason=%s error_genre=%s",
                msg_id,
                next_count,
                publish_id,
                reason,
                error_genre,
            )
            return True
        except Exception:
            logger.exception("Retry publish failed. message_id=%s retry_count=%s reason=%s", msg_id, next_count, reason)
            return False

    def process_payload(
        msg_id: str,
        data: bytes,
        attributes: Dict[str, str],
        retry_count: int,
        queue_payload: Optional[Dict[str, Any]],
        source: str,
    ) -> None:
        queue_path = queue_payload.get("queue_path") if queue_payload else None
        if queue_payload and queue_path:
            attempts = int(queue_payload.get("attempts", 0)) + 1
            queue_payload["attempts"] = attempts
            queue_payload["last_attempt_at"] = time.time()
            _write_json(queue_path, queue_payload)
        body = _parse_body(data, logger, msg_id)
        cmd = build_command(python_path, body, extra_args)
        try:
            cmd_display = " ".join([f'\"{c}\"' if " " in str(c) else str(c) for c in cmd])
            logger.info("Subprocess starting. message_id=%s source=%s cmd=%s", msg_id, source, cmd_display)
            start = time.time()
            result = subprocess.run(cmd, check=True, cwd=working_dir, capture_output=True, text=True)
            elapsed = time.time() - start
            logger.info("Subprocess finished. message_id=%s source=%s returncode=0 elapsed_sec=%.2f", msg_id, source, elapsed)
            if result.stdout:
                logger.info("stdout:\n%s", result.stdout.rstrip())
            if result.stderr:
                logger.warning("stderr:\n%s", result.stderr.rstrip())
            if queue_path:
                os.remove(queue_path)
                logger.info("Queue file removed. message_id=%s path=%s", msg_id, queue_path)
            logger.info("Done. message_id=%s elapsed_sec=%.2f", msg_id, elapsed)
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start if "start" in locals() else None
            if elapsed is not None:
                logger.error("Subprocess failed. message_id=%s returncode=%s elapsed_sec=%.2f", msg_id, e.returncode, elapsed)
            else:
                logger.error("Subprocess failed. message_id=%s returncode=%s", msg_id, e.returncode)
            if e.stdout:
                logger.info("stdout:\n%s", e.stdout.rstrip())
            if e.stderr:
                logger.error("stderr:\n%s", e.stderr.rstrip())
            error_genre = _error_genre_from_returncode(e.returncode)
            if error_genre:
                _insert_error_log(db_config, subscription, error_genre, logger)
            if queue_payload and queue_path:
                queue_payload["last_error"] = f"subprocess_failed:{e.returncode}"
                queue_payload["last_error_at"] = time.time()
                _write_json(queue_path, queue_payload)
            published = publish_retry(msg_id, data, attributes, retry_count, "subprocess_failed", error_genre)
            if published and queue_path:
                os.remove(queue_path)
                logger.info("Queue file removed after retry publish. message_id=%s path=%s", msg_id, queue_path)
        except Exception:
            logger.exception("Unexpected error. message_id=%s source=%s", msg_id, source)
            if queue_payload and queue_path:
                queue_payload["last_error"] = "unexpected_error"
                queue_payload["last_error_at"] = time.time()
                _write_json(queue_path, queue_payload)
            published = publish_retry(msg_id, data, attributes, retry_count, "unexpected_error", None)
            if published and queue_path:
                os.remove(queue_path)
                logger.info("Queue file removed after retry publish. message_id=%s path=%s", msg_id, queue_path)

    def callback(message: pubsub_v1.subscriber.message.Message):
        msg_id = message.message_id
        attributes = dict(message.attributes or {})
        retry_count = _parse_retry_count(attributes, logger)
        logger.info("Callback start. message_id=%s retry_count=%s", msg_id, retry_count)
        data = message.data or b""
        logger.info(
            "Message received. message_id=%s attributes=%s data_len=%s",
            msg_id,
            attributes,
            len(data),
        )
        queue_payload = None
        try:
            queue_payload = _save_queue_message(queue_dir, msg_id, data, attributes)
            logger.info("Queue saved. message_id=%s path=%s", msg_id, queue_payload.get("queue_path"))
        except Exception:
            logger.exception("Queue save failed. message_id=%s NACK.", msg_id)
            message.nack()
            logger.info("Callback end. message_id=%s", msg_id)
            return

        message.ack()
        logger.info("ACKed early. message_id=%s", msg_id)

        process_payload(msg_id, data, attributes, retry_count, queue_payload, "subscription")
        logger.info("Callback end. message_id=%s", msg_id)

    process_pending_queue()

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
    defaults = {
        "retry_topic": cfg.get("retry_topic"),
        "max_retries": cfg.get("max_retries"),
        "queue_dir": cfg.get("queue_dir"),
        "log_dir": cfg.get("log_dir"),
    }

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
        merged = dict(defaults)
        merged.update(subcfg)
        t = threading.Thread(target=worker, args=(project_id, merged, credentials_path), daemon=True)
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
