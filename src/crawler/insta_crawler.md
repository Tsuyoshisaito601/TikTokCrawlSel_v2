# insta_crawler.py ドキュメント

## 目的
Instagram（主にリール）からメタ情報（ライト）と詳細情報（ヘビー）を取得し、DB保存とPub/Sub通知を行うクローラーです。Seleniumでブラウザ操作し、クローラー用アカウントでログインして対象ユーザーを巡回します。

## 主な依存関係
- Selenium: `By`, `WebDriverWait`, `expected_conditions`, 例外（`TimeoutException`, `NoSuchElementException`）
- GCP Pub/Sub: `google.cloud.pubsub_v1.PublisherClient`
- DB層: `Database`, `CrawlerAccount`, `FavoriteUser`, `VideoLightRawData`
- リポジトリ: `InstaCrawlerAccountRepository`, `InstaFavoriteUserRepository`, `InstaVideoRepository`
- その他: `SeleniumManager`, `setup_logger`

## 環境変数
- `PROJECT_ID`: Pub/Subの送信先プロジェクトID
- `SADCAPTCHA_API_KEY`: `SeleniumManager` が利用（該当実装側の設定）

## ユーティリティ関数
### parse_insta_time(time_text, base_time)
- 「秒前/分前/時間前/日前/週間前」や「YYYY年M月D日」「YYYY-MM-DD」を `datetime` に変換
- 例外時は `None` を返し警告ログ
- 現在このファイル内では未使用

### parse_insta_video_url(url)
- InstagramのURLパスから `video_id` と `user_username` を抽出
- 例: `/username/p/video_id` を想定

### parse_insta_number(text)
- 「万」「億」「,」「小数点」を含む数値文字列を整数化
- 再生数・フォロワー数などの数値化に使用

## InstaCrawler クラス
### 主要セレクタ定数
- プロフィール、リール一覧、再生数、サムネ、ピン留め、詳細ページ情報などをCSSセレクタとして定数化
- `CLOSE_BUTTON_SELECTOR` は詳細ページの閉じるボタン（×）に対応

### 初期化とリソース管理
- `__init__`: リポジトリ、ログイン関連、ブラウザ設定（プロキシ、プロファイル再利用）などを受け取る
- `__enter__`:
  - アカウント選定（ID指定 or 空きアカウント）
  - `SeleniumManager` でドライバ生成
  - `skip_login` が `False` の場合はログイン実行
- `__exit__` / `_cleanup_resources`: ドライバとPub/Subクライアントを安全に終了

### Pub/Sub 送信
- `_init_publisher`: `PublisherClient` を生成し `insta-video-master-sync` トピックを利用
- `_publish_video_master_sync`: `target_table=insta_video_master` を付与しJSON送信
- `PROJECT_ID` 未設定時は送信スキップ

### ログイン処理
- `_login`:
  - `use_profile=True` の場合は既存プロファイルでログイン状態確認
  - 未ログイン時は通常のID/パスワード入力にフォールバック
  - プロフィール画像の出現でログイン成功判定

### ページ遷移
- `navigate_to_user_page`: ユーザーページに遷移し、存在しない場合は例外
- `navigate_to_reels_page`: リールタブクリックかURL直接遷移
  - ログメッセージに一部文字化けがある（内容自体はログ用途）

### スクロール
- `scroll_user_page`: 投稿が一定数に到達するまでスクロール（現状未使用）
- `scroll_reels_page`: リール一覧を読み切るためにスクロール

### 詳細データ（ヘビー）取得
- `get_video_heavy_data_from_video_page`:
  - 投稿日時、音源情報、タイトル、コメントを取得
  - コメントはJSON文字列化して `comments_json` に格納

### リール詳細への遷移・復帰
- `_click_reel_item_by_index`: 一覧の指定indexをクリックして詳細へ
- `_click_close_button_to_return`: 閉じるボタンで一覧へ戻る
- `_fallback_navigate_to_reels_page`: 復帰に失敗した場合のURL直接遷移

### ヘビー情報の収集戦略
- `collect_reel_heavy_data_map`:
  - 一覧ページでスクロールして順にクリックし詳細を取得
  - `user_username` が不明な場合はURL遷移方式へフォールバック
- `_collect_reel_heavy_data_map_by_url`:
  - 旧方式のURL直接遷移で詳細取得（フォールバック用途）

### ライト情報取得
- `get_video_like_dates_from_user_page`:
  - リール一覧から再生数、URL、ID、サムネURL、ピン留めを収集
  - `crawling_algorithm="instagram-reels-grid-v1"`

### ユーザー情報取得
- `get_and_save_user_name_datas`: 新UIのニックネーム優先で取得し保存
- `get_user_followers_count_from_user_page`: フォロワー数を取得し数値化

### 保存と通知
- `parse_and_save_video_light_datas`:
  - `VideoLightRawData` に変換して保存（ライト/ヘビー両方）
  - 再生数は `parse_insta_number` で数値化
  - Pub/Subにも通知

### クロール実行フロー
- `crawl_user`:
  - ユーザー存在確認 → フォロワー履歴保存 → ニックネーム保存
  - ライト情報取得 → ヘビー情報取得 → マージ → 保存/通知
- `crawl_favorite_users`:
  - DBから推しユーザーを取得して順次クロール

### 付随メソッド
- `_extract_like_count_from_label`: いいね数抽出パターン（現状未使用）
- `engagement_type` 引数は保持されるが本ファイル内では未使用

## CLI 実行
`main()` で引数を解析し、`test` モードはログインのみで停止します。

主要引数:
- `--crawler-account-id`
- `--max-videos-per-user` / `--max-users`
- `--device-type`（`pc`/`vps`）
- `--use-profile` / `--chrome-user-data-dir` / `--chrome-profile-directory`
- `--mode`（`light`/`test`）
- `--no-proxy`

## データ例
### heavy_data
```json
{
  "video_url": "...",
  "post_time_text": "...",
  "post_time_iso": "...",
  "audio_info_text": "...",
  "video_title": "...",
  "comments_json": "..."
}
```

### light_like_data
```json
{
  "video_url": "...",
  "video_id": "...",
  "user_username": "...",
  "video_thumbnail_url": "...",
  "video_alt_info_text": "",
  "like_count_text": null,
  "play_count_text": "...",
  "crawling_algorithm": "instagram-reels-grid-v1",
  "is_pinned": false
}
```
