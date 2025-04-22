import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from .database import Database
from ..logger import setup_logger
from dotenv import load_dotenv

logger = setup_logger(__name__)

def fetch_table_data(db: Database, table_name: str) -> List[Dict[str, Any]]:
    """指定されたテーブルのデータを全て取得する"""
    query = f"SELECT * FROM {table_name}"
    cursor = db.execute_query(query)
    columns = [desc[0] for desc in cursor.description]
    results = []
    
    for row in cursor.fetchall():
        result = {}
        for i, value in enumerate(row):
            # datetime型はstr型に変換
            if isinstance(value, datetime):
                value = value.strftime('%Y-%m-%d %H:%M:%S')
            result[columns[i]] = value
        results.append(result)
    
    return results

def save_to_csv(data: List[Dict[str, Any]], output_path: Path):
    """データをCSVファイルに保存する"""
    if not data:
        logger.warning(f"データが空のため、{output_path}は作成しません")
        return
    
    # 出力ディレクトリが存在しない場合は作成
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # CSVファイルに書き出し
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    logger.info(f"{output_path}にデータを保存しました（{len(data)}件）")

def main():
    """DBの内容をCSVファイルに出力する"""
    load_dotenv()
    with Database() as db:
        output_dir = Path("output/database_dump")
        
        logger.info("DBデータのエクスポートを開始します")
        
        # クローラーアカウント
        crawler_accounts = fetch_table_data(db, "crawler_accounts")
        save_to_csv(crawler_accounts, output_dir / "crawler_accounts.csv")
        
        # お気に入りアカウント
        favorite_users = fetch_table_data(db, "account_list")
        save_to_csv(favorite_users, output_dir / "favorite_users.csv")
        
        # 動画の重いデータ
        video_heavy = fetch_table_data(db, "video_heavy_raw_data")
        save_to_csv(video_heavy, output_dir / "video_heavy_raw_data.csv")
        
        # 動画の軽いデータ
        video_light = fetch_table_data(db, "video_light_raw_data")
        save_to_csv(video_light, output_dir / "video_light_raw_data.csv")
        
        logger.info("DBデータのエクスポートが完了しました")
        

if __name__ == "__main__":
    main()
