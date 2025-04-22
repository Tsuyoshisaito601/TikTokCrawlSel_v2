# TikTok Light Crawler on Selenium

TikTok動画の再生数といいね数の推移データを収集するクローラー。約30,000動画のデータ収集を目標としています。

## 機能

- TikTok動画の基本情報の収集
  - 動画ID
  - URL
  - アカウント情報
  - タイトル
  - 投稿日時

- 動画の統計情報の収集
  - 再生数
  - いいね数

- マルチアカウント対応
  - 複数のクローラーアカウントを管理
  - アカウントの利用状況を追跡

## 環境構築
### 仮想環境の作成

```bash
# 仮想環境の作成
python -m venv venv

# 仮想環境の有効化
# Windows
.\venv\Scripts\activate
# Unix/MacOS
source venv/bin/activate

# 依存関係のインストール
pip install -r requirements.txt
```

### 環境変数の設定

`.env`ファイルを作成し、以下の環境変数を設定してください：

```
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=tiktok_crawler
```

## 使い方
1. データベースの初期化
```bash
python -m src.database.create_tables
```

2. [テスト時] サンプルデータを投入
src/database/seed_data.py のcrawler_accountsの username, password をあなたのものにしてから
```bash
python -m src.database.seed_data --max-users 10
```
max-usersはサンプルユーザー何人入れるか。コーナーケースのテストも欲しいので5人以上入れてね

3. [テスト時] クローラーを実行してみる
```bash
# 軽いデータのみクロール
python -m src.crawler.tiktok_crawler light

# 重いデータのみクロール（オプション指定例）
python -m src.crawler.tiktok_crawler heavy --crawler-account-id 1 --max-videos-per-user 10 --max-users 5 --recrawl

# 両方クロール
python -m src.crawler.tiktok_crawler both

# オプション一覧
--crawler-account-id INT     使用するクローラーアカウントのID
--max-videos-per-user INT 1アカウントあたりの最大取得動画数（デフォルト: 50）
--max-users INT          クロール対象の最大アカウント数（デフォルト: 10）
--recrawl                既にクロール済みの動画を再クロールする
```

4. [テスト時] 結果を確認
```bash
python -m src.database.show_data
```
./output/database_dump にcsvで出力されます

## DB構造
### crawler_accounts: クローラーアカウント管理
- id: int (PK, 自動採番)
- username: str (クローラーのTikTokアカウント名)
- password: str (クローラーのTikTokパスワード)
- proxy: str | null (プロキシ設定、未設定の場合はnull)
- is_alive: bool (アカウントの有効性)
- last_crawled_at: datetime | null (最終クロール日時、未クロールの場合はnull)

### favorite_users: クロール対象アカウント管理
- id: int (PK, 自動採番)
- favorite_user_username: str (クロール対象のTikTokアカウント名)
- crawler_account_id: int | null (FK -> crawler_accounts.id、未割り当ての場合はnull)
- favorite_user_is_alive: bool (アカウントの有効性)
- crawl_priority: int (クロール優先度)(未使用、システムがデカくなったら必要になってくると思う)
- last_crawled_at: datetime | null (最終クロール日時、未クロールの場合はnull)

### video_light_raw_data: 動画の軽いデータ
- id: int (PK, 自動採番)
- video_url: str (動画のURL)
- video_id: str (TikTokの動画ID)
- user_username: str (投稿者のアカウント名)
- video_thumbnail_url: str (サムネイル画像URL)
- video_alt_info_text: str (動画の代替テキスト)
- play_count_text: str (表示形式のままの再生数)
- play_count: int | null (パース後の再生数)
- like_count_text: str (表示形式のままのいいね数)
- like_count: int | null (パース後のいいね数)
- crawled_at: datetime (クロール日時)
- crawling_algorithm: str (クロールアルゴリズムの名前)

### video_heavy_raw_data: 動画の重いデータ
- id: int (PK, 自動採番)
- video_url: str (動画のURL)
- video_id: str (TikTokの動画ID)
- user_username: str (投稿者のアカウント名)
- user_nickname: str (投稿者のニックネーム)
- video_thumbnail_url: str (サムネイル画像URL)
- video_title: str (動画のタイトル)
- post_time_text: str (投稿日時の表示形式)
- post_time: datetime | null (パース後の投稿日時)
- audio_url: str | null (音声ファイルURL)
- audio_info_text: str | null (音声情報テキスト)
- audio_id: str | null (音声ID)
- audio_title: str | null (音声タイトル)
- audio_author_name: str | null (音声作者名)
- play_count_text: str | null (表示形式のままの再生数)
- play_count: int | null (パース後の再生数)
- like_count_text: str (表示形式のままのいいね数)
- like_count: int | null (パース後のいいね数)
- comment_count_text: str | null (表示形式のままのコメント数)
- comment_count: int | null (パース後のコメント数)
- collect_count_text: str | null (表示形式のままのコレクト数)
- collect_count: int | null (パース後のコレクト数)
- share_count_text: str | null (表示形式のままのシェア数)
- share_count: int | null (パース後のシェア数)
- crawled_at: datetime (クロール日時)
- crawling_algorithm: str (クロールアルゴリズムの名前)


## 注意事項

- 本クローラーは人間らしい動作をシミュレートし、サーバーに負荷をかけないよう配慮しています。
- クロール対象のアカウントや動画については、各サービスの利用規約やガイドラインに従ってください。
