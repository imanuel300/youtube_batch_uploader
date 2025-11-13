import os
import pandas as pd
import requests
from tqdm import tqdm
import pickle
import urllib.parse
import logging
import time

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

CSV_FILE = "videos.csv"
DOWNLOAD_FOLDER = "downloads"
LOG_FILE = "upload_log.log"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
BASE_STORAGE_URL = "https://storage101.lon3.clouddrive.com/v1/MossoCloudFS_359702fa-5130-4cf4-9e74-778f0ddc61ed/ateretMordecay"
BASE_WEBSITE_URL = "https://www.ateretmordechai.org/%D7%90%D7%A8%D7%9B%D7%99%D7%95%D7%9F-%D7%A9%D7%99%D7%A2%D7%95%D7%A8%D7%99%D7%9D?view=media&id="

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def authenticate_youtube():
    logger.info("🔐 מאמת את YouTube API...")
    creds = None

    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
        logger.info("✅ טוקן נמצא ונטען")
        logger.info("💡 אם אתה רוצה להתחבר לפרויקט חדש, מחק את קובץ token.pickle")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("🔄 מרענן טוקן...")
            creds.refresh(Request())
        else:
            logger.info("🌐 מבקש הרשאות חדשות...")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            except Exception as e:
                if "access_denied" in str(e) or "403" in str(e):
                    logger.error("❌ שגיאת 403: access_denied")
                    logger.error("=" * 60)
                    logger.error("האפליקציה במצב Testing. כדי לפתור:")
                    logger.error("1. היכנס ל-Google Cloud Console")
                    logger.error("2. לך ל-APIs & Services > OAuth consent screen")
                    logger.error("3. הוסף את עצמך ל-Test users")
                    logger.error("4. מחק את token.pickle והפעל מחדש")
                    logger.error("=" * 60)
                raise
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
        logger.info("✅ אימות הושלם בהצלחה")

    return build("youtube", "v3", credentials=creds)


def download_file(url, out_path, max_retries=3):
    logger.info(f"⬇️ מתחיל הורדה: {url}")
    logger.info(f"📁 יעד: {out_path}")
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=30)
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))

            with open(out_path, 'wb') as f, tqdm(
                desc=os.path.basename(out_path),
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))

            logger.info(f"✅ הורדה הושלמה: {out_path}")
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ נכשל ניסיון הורדה {attempt + 1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"⏳ ממתין {wait_time} שניות לפני ניסיון חוזר...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ ההורדה נכשלה לאחר {max_retries} ניסיונות")
                raise


def resumable_upload(youtube, file_path, title, description, tags, max_retries=5):
    logger.info(f"📤 מתחיל העלאה ליוטיוב: {title}")
    
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags.split(",") if tags else []
        },
        "status": {
            "privacyStatus": "public"
        }
    }

    media = MediaFileUpload(file_path, chunksize=1024*1024*8, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media
    )

    file_size = os.path.getsize(file_path)
    uploaded = 0
    
    with tqdm(total=file_size, unit="B", unit_scale=True, desc="⬆️ Uploading", initial=0) as bar:
        response = None
        retry_count = 0
        
        while response is None and retry_count < max_retries:
            try:
                error = None
                while response is None:
                    try:
                        status, response = request.next_chunk()
                        if status:
                            uploaded = status.resumable_progress
                            bar.update(status.resumable_progress - bar.n)
                    except HttpError as e:
                        error = e
                        if e.resp.status in [500, 502, 503, 504]:
                            # Server error - retry
                            logger.warning(f"⚠️ שגיאת שרת: {e.resp.status}. מנסה להמשיך...")
                            time.sleep(2 ** retry_count)  # Exponential backoff
                            break
                        else:
                            raise
                
                if response is not None:
                    logger.info(f"✅ הועלה בהצלחה! Video ID: {response['id']}")
                    logger.info(f"🔗 https://www.youtube.com/watch?v={response['id']}")
                    break
                    
            except HttpError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"❌ ההעלאה נכשלה לאחר {max_retries} ניסיונות: {str(e)}")
                    raise
                else:
                    wait_time = min(2 ** retry_count, 60)  # Max 60 seconds
                    logger.warning(f"⚠️ שגיאה בהעלאה (ניסיון {retry_count}/{max_retries}): {str(e)}")
                    logger.info(f"⏳ ממתין {wait_time} שניות לפני ניסיון חוזר...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"❌ שגיאה לא צפויה: {str(e)}")
                    raise
                else:
                    wait_time = min(2 ** retry_count, 60)
                    logger.warning(f"⚠️ שגיאה (ניסיון {retry_count}/{max_retries}): {str(e)}")
                    logger.info(f"⏳ ממתין {wait_time} שניות לפני ניסיון חוזר...")
                    time.sleep(wait_time)

    return response


def main():
    logger.info("=" * 60)
    logger.info("🚀 מתחיל תהליך העלאה ליוטיוב")
    logger.info("=" * 60)
    
    try:
        youtube = authenticate_youtube()
        df = pd.read_csv(CSV_FILE)
        
        # Ensure 'uploaded' column exists and fill NaN values with empty string
        if "uploaded" not in df.columns:
            df["uploaded"] = ""
        df["uploaded"] = df["uploaded"].fillna("").astype(str)
        
        logger.info(f"📊 נמצאו {len(df)} שורות בקובץ CSV")
        
        uploaded_count = len(df[df["uploaded"].str.lower() == "yes"])
        remaining_count = len(df) - uploaded_count
        logger.info(f"✅ כבר הועלו: {uploaded_count} | 📤 נותרו: {remaining_count}")

        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

        for idx, row in df.iterrows():
            # Skip if already uploaded
            if str(row.get("uploaded", "")).lower() == "yes":
                title_text = str(row.get("title", "N/A")).strip()
                logger.info(f"⏭️ דילוג על שורה {idx + 1}: כבר הועלה - {title_text}")
                continue

            # Build title: rabi + cat + title
            rabi = str(row.get("rabi", "")).strip()
            cat = str(row.get("cat", "")).strip()
            title = str(row.get("title", "")).strip()
            video_title = f"{rabi} - {cat} - {title}" if rabi and cat else (f"{cat} - {title}" if cat else title)
            
            logger.info(f"\n{'=' * 60}")
            logger.info(f"📹 מעבד שורה {idx + 1}/{len(df)}: {video_title}")
            logger.info(f"{'=' * 60}")

            # Build full URL
            url_path = str(row.get("url", "")).strip()
            if not url_path.startswith("http"):
                # Add base URL if not already a full URL
                if url_path.startswith("/"):
                    full_url = BASE_STORAGE_URL + url_path
                else:
                    full_url = BASE_STORAGE_URL + "/" + url_path
            else:
                full_url = url_path
            
            logger.info(f"🔗 URL מלא: {full_url}")
            
            parsed = urllib.parse.urlparse(full_url)
            file_name = os.path.basename(parsed.path)  # filename only without ?params
            # Remove query parameters from filename
            if "?" in file_name:
                file_name = file_name.split("?")[0]
            local_file = os.path.join(DOWNLOAD_FOLDER, file_name)

            # Check if file already exists
            if os.path.exists(local_file):
                file_size = os.path.getsize(local_file)
                logger.info(f"✅ קובץ כבר קיים: {local_file} ({file_size / (1024*1024):.2f} MB)")
                logger.info("⏭️ דילוג על הורדה, ממשיך להעלאה...")
            else:
                # Download file
                try:
                    download_file(full_url, local_file)
                except Exception as e:
                    logger.error(f"❌ שגיאה בהורדה: {str(e)}")
                    logger.error(f"⏭️ דילוג על שורה {idx + 1}")
                    continue

            # Build description
            csv_id = str(row.get("id", "")).strip()
            added_date = str(row.get("added", "")).strip()
            
            # Remove " 0:00" from date if exists
            if added_date and " 0:00" in added_date:
                added_date = added_date.replace(" 0:00", "")
            
            website_link = BASE_WEBSITE_URL + csv_id if csv_id else ""
            
            description = "דפי מקורות וקובץ שמע בעמוד השיעור באתר הישיבה"
            if website_link:
                # Create clickable HTML link
                description += f"\n\n<a href=\"{website_link}\">{website_link}</a>"
            if added_date:
                description += f"\n\nתאריך: {added_date}"

            # Upload to YouTube
            try:
                response = resumable_upload(
                    youtube,
                    local_file,
                    video_title,
                    description,
                    ""  # No tags field in new CSV format
                )
                
                if response:
                    youtube_video_id = response.get("id") if isinstance(response, dict) else None
                    if youtube_video_id:
                        youtube_url = f"https://youtu.be/{youtube_video_id}"
                        df.at[idx, "youtube_url"] = youtube_url
                        logger.info(f"🔗 נשמר קישור: {youtube_url}")
                    df.at[idx, "uploaded"] = "yes"
                    df.to_csv(CSV_FILE, index=False)
                    logger.info("📌 סומן כ-uploaded ✅ ונשמר לקובץ CSV")
                    
                    # Delete file after successful upload
                    try:
                        if os.path.exists(local_file):
                            file_size = os.path.getsize(local_file)
                            os.remove(local_file)
                            logger.info(f"🗑️ קובץ נמחק: {local_file} ({file_size / (1024*1024):.2f} MB)")
                    except Exception as e:
                        logger.warning(f"⚠️ לא הצלחתי למחוק את הקובץ {local_file}: {str(e)}")
                else:
                    logger.error("❌ ההעלאה נכשלה")
                    
            except Exception as e:
                logger.error(f"❌ שגיאה בהעלאה: {str(e)}")
                logger.error(f"⏭️ ממשיך לשורה הבאה...")
                continue

        logger.info(f"\n{'=' * 60}")
        logger.info("🎉 כל ההעלאות הסתיימו!")
        logger.info(f"{'=' * 60}")
        
    except Exception as e:
        logger.error(f"❌ שגיאה קריטית: {str(e)}")
        raise


if __name__ == "__main__":
    main()
