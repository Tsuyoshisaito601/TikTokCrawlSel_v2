
# Bundle for PROJECT_ID=tiktok-analytics-prod-451609, TOPIC=crawler-jobs

Artifacts:
- pc_sessions.validated.csv
- gcloud_setup.sh
- cloud_scheduler_setup.sh
- agent.py
- agent_config.example.json

## 1) Pub/Sub subscriptions
```
bash gcloud_setup.sh
```

## 2) Cloud Scheduler jobs
```
bash cloud_scheduler_setup.sh
```
- Region: asia-northeast1
- TimeZone: Asia/Tokyo
- Schedule: every 48 hours
Edit the script to change cadence per job if needed.

## 3) Agent on PCs
- Copy `agent.py` and create `agent_config.json` from the example on each PC.
- Set `subscription_name` (e.g., pc-01-s1), `working_dir`, `python_path`, `credentials_path`.
- Run under Task Scheduler (run whether user logged on or not, highest privileges, retries).

## 4) Test
Publish a message to `crawler-jobs` with attributes `target_pc=<pc_id>, session=<session_id>`. The agent should launch the crawler with args embedded by the Scheduler.
