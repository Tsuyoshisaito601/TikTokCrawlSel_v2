#全体フロー図
┌─────────────────────────────────────────────────────────────────────────┐
│ crawl_favorite_users(light_or_heavy="both")                              │
│   └─→ 各ユーザーに対して crawl_user() を呼び出し                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ crawl_user(user, light_or_heavy="both")                                  │
│                                                                          │
│  1. navigate_to_user_page() ← ユーザーページへ遷移                        │
│  2. last_crawled 更新                                                    │
│                                                                          │
│  ┌─── if light_or_heavy == "both" ────────────────────────────────────┐ │
│  │                                                                     │ │
│  │  3. fetch_and_save_followers() ← フォロワー数取得・保存              │ │
│  │  4. get_and_save_user_name_datas() ← ニックネーム取得・保存          │ │
│  │                                                                     │ │
│  │  5. get_video_light_like_datas_from_user_page()                     │ │
│  │     └─→ スクロール → 各動画から以下を取得:                            │ │
│  │         ・video_url, video_id, user_username                        │ │
│  │         ・video_thumbnail_url, video_alt_info_text                  │ │
│  │         ・play_count_text ← [data-e2e='video-views'] から           │ │
│  │                                                                     │ │
│  │  6. parse_and_save_video_light_datas(light_like_datas)              │ │
│  │     └─→ 各動画に対して:                                              │ │
│  │         ・VideoLightRawData として DB保存                            │ │
│  │         ・Pub/Sub送信 (video_id, video_url, user_username, play_count)│ │
│  │                                                                     │ │
│  │  ┌─── if user.is_new_account == True ───────────────────────────┐  │ │
│  │  │  【新規アカウント】                                            │  │ │
│  │  │  7. 全動画に対して重いデータ取得                               │  │ │
│  │  │     └─→ video_id降順でソート                                  │  │ │
│  │  │     └─→ 100件ずつバッチ処理                                   │  │ │
│  │  │     └─→ navigate_to_video_page()                             │  │ │
│  │  │     └─→ get_video_heavy_data_from_video_page()               │  │ │
│  │  │     └─→ parse_and_save_video_heavy_data() ← DB保存+Pub/Sub    │  │ │
│  │  │     └─→ 処理済みURL除外 → 未処理なしなら終了                   │  │ │
│  │  │  8. is_new_account を False に更新                            │  │ │
│  │  └─────────────────────────────────────────────────────────────┘  │ │
│  │                                                                     │ │
│  │  ┌─── else (既存アカウント) ────────────────────────────────────┐  │ │
│  │  │  【取得実績ありアカウント】                                     │  │ │
│  │  │  7. get_videos_needing_update() ← needs_update=1の動画取得     │  │ │
│  │  │  8. 対象動画のみ重いデータ取得                                  │  │ │
│  │  │     └─→ parse_and_save_video_heavy_data() ← DB保存+Pub/Sub     │  │ │
│  │  └─────────────────────────────────────────────────────────────┘  │ │
│  │                                                                     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  9. last_crawled 更新                                                    │
└─────────────────────────────────────────────────────────────────────────┘

# 取得データ詳細

## get_video_light_like_datas_from_user_page (548行目)

| フィールド | 取得元 | 備考 |
|-----------|--------|------|
| `video_url` | `<a>` タグの href | |
| `video_id` | URLからパース | |
| `user_username` | URLからパース | |
| `video_thumbnail_url` | `<img>` の src | |
| `video_alt_info_text` | `<img>` の alt | |
| `play_count_text` | `[data-e2e='video-views']` | 再生数 |

---

## parse_and_save_video_light_datas (1306行目)

### DB保存

```python
VideoLightRawData(
    video_url, video_id, user_username,
    video_thumbnail_url, video_alt_info_text,
    play_count_text, play_count,
    crawling_algorithm, crawled_at
)
```

### Pub/Sub送信

```python
{
    "video_id": ...,
    "video_url": ...,
    "user_username": ...,
    "play_count": ...
}
```

---

# 変更履歴 (2024/12)

## 変更内容

| 項目 | 変更前 | 変更後 |
|------|--------|--------|
| `[data-e2e='video-views']` の解釈 | `like_count_text` | `play_count_text` |
| `get_video_play_count_datas_from_user_page` 呼び出し | あり（重複） | 削除 |
| `parse_and_save_play_count_datas` 呼び出し | あり | 削除 |
| `attach_play_counts` 呼び出し | あり | 削除 |
| `merged_light_datas` 変数 | あり | 削除（`light_like_datas`に統一） |
| `parse_and_save_video_light_datas` でのPub/Sub | なし | 追加 |

## 効率化ポイント

- スクロール処理が1回に削減（以前は2回）
- 同じ要素の重複取得を解消
- 約40-55秒の待機時間削減
- コードの簡素化（マージ処理不要に）