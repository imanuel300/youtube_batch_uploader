import os
import pandas as pd
import requests
from tqdm import tqdm
import pickle
import urllib.parse
import logging
import time
import hmac
import hashlib
from datetime import datetime, timezone
import re  # ניקוי כותרות

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

CSV_FILE = "videos.csv"
DOWNLOAD_FOLDER = "downloads"
LOG_FILE = "upload_log.log"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Storage configuration
STORAGE_BASE = "https://storage101.lon3.clouddrive.com"
STORAGE_PATH_BASE = ""
STORAGE_KEY = ""
STORAGE_EXPIRES_SECONDS = 5700  # 5700 seconds = ~95 minutes

BASE_WEBSITE_URL = ""
# Endpoint on site to update DB provider field (GET)
UPDATE_PROVIDER_ENDPOINT = ""

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

MAX_TITLE_LENGTH = 100


def generate_storage_url(video_path):
    """
    Generate a signed temporary URL for video download.
    Based on PHP code: calculates HMAC SHA1 signature with expiration.
    
    Args:
        video_path: Relative path to video file (e.g., "/BIS/Achila/video.mp4")
    
    Returns:
        Full signed URL with signature and expiration
    """
    method = 'GET'
    
    # Get current UTC timestamp and add expiration time
    expires = int(datetime.now(timezone.utc).timestamp()) + STORAGE_EXPIRES_SECONDS
    
    # Build full path
    if video_path.startswith("/"):
        full_path = STORAGE_PATH_BASE + video_path
    else:
        full_path = STORAGE_PATH_BASE + "/" + video_path
    
    # Create HMAC data string (method, expires, path)
    hmac_data = f"{method}\n{expires}\n{full_path}"
    
    # Calculate HMAC SHA1 signature
    signature = hmac.new(
        STORAGE_KEY.encode('utf-8'),
        hmac_data.encode('utf-8'),
        hashlib.sha1
    ).hexdigest()
    
    # Build final URL
    temp_url = f"{STORAGE_BASE}{full_path}?temp_url_sig={signature}&temp_url_expires={expires}"
    
    return temp_url


def notify_site_update_provider(csv_id: str, youtube_url: str) -> bool:
    try:
        if not csv_id or not youtube_url:
            return False
        params = {
            "id": csv_id,
            "youtube_url": youtube_url,
        }
        resp = requests.get(UPDATE_PROVIDER_ENDPOINT, params=params, timeout=15)
        if resp.status_code == 200:
            logger.info("🛰️ עודכן provider באתר עבור id=%s", csv_id)
            return True
        else:
            logger.warning("⚠️ עדכון provider נכשל (HTTP %s): %s", resp.status_code, resp.text[:300])
            return False
    except Exception as e:
        logger.warning("⚠️ כשל בעדכון provider באתר: %s", str(e))
        return False


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
        # Ensure tracking columns exist
        if "uploaded" not in df.columns:
            df["uploaded"] = ""
        df["uploaded"] = df["uploaded"].fillna("").astype(str)
        if "provider_updated" not in df.columns:
            df["provider_updated"] = ""
        df["provider_updated"] = df["provider_updated"].fillna("").astype(str)
        
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
            # הגנה - אם title ריק נסה לפחות cat/rabi או שנהג title גנרי
            if not title and (rabi or cat):
                title = f"{cat or rabi}"
            video_title = f"{rabi} - {cat} - {title}" if rabi and cat else (f"{cat} - {title}" if cat else title)
            # ודא שהכותרת לא ריקה
            if not video_title or not video_title.strip():
                logger.warning(f"שורה {idx + 1}: לא נמצאה כותרת תקינה! דילוג")
                continue
            
            # ניקוי תווים אסורים בכותרת
            video_title = re.sub(r"[^\w\s\u0590-\u05fe\-\.,;:!?()\"'’״׳]", "", video_title)
            video_title = video_title.replace('\n', ' ').replace('\r', ' ')
            logger.debug(f"שורה {idx+1}, כותרת לשידור לאחר ניקוי: >>{video_title}<<")
            if not video_title or not video_title.strip():
                logger.warning(f"שורה {idx + 1}: לא נמצאה כותרת תקינה גם אחרי ניקוי! דילוג")
                continue
            
            # בדיקת אורך כותרת
            if len(video_title) > MAX_TITLE_LENGTH:
                logger.warning(f"שורה {idx + 1}: הכותרת ארוכה מדי ({len(video_title)} תווים). מקצץ ל-100 התווים האחרונים.")
                logger.warning(f"כותרת מקורית: {video_title}")
                video_title = video_title[-MAX_TITLE_LENGTH:]  # לוקח את 100 התווים האחרונים
                logger.warning(f"כותרת לאחר קיצוץ: {video_title}")
            
            logger.info(f"\n{'=' * 60}")
            logger.info(f"📹 מעבד שורה {idx + 1}/{len(df)}: {video_title}")
            logger.info(f"{'=' * 60}")

            # Build full URL with dynamic signature
            url_path = str(row.get("url", "")).strip()
            if not url_path.startswith("http"):
                # Generate signed URL
                full_url = generate_storage_url(url_path)
                logger.info(f"🔗 URL מלא (חתום): {full_url}")
            else:
                # Already a full URL, use as is
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
                    err_msg = str(e)
                    logger.error(f"❌ שגיאה בהורדה: {err_msg}")
                    if "Client Error: Not Found for url" in err_msg:
                        df.at[idx, "uploaded"] = "Not url"
                        df.to_csv(CSV_FILE, index=False)
                        logger.info(f"✅ נשמר בקובץ CSV עם 'Not url' בעמודת uploaded עבור שורה {idx+1}")
                    logger.error(f"⏭️ דילוג על שורה {idx + 1}")
                    continue

            # Build description
            csv_id = str(row.get("id", "")).strip()
            added_date = str(row.get("added", "")).strip()
            
            # Remove " 0:00" from date if exists
            if added_date and " 0:00" in added_date:
                added_date = added_date.replace(" 0:00", "")
                date_obj = datetime.strptime(added_date, "%m/%d/%Y")
                added_date = date_obj.strftime("%d/%m/%Y")
            
            website_link = BASE_WEBSITE_URL + csv_id if csv_id else ""
            
            description = "דפי מקורות וקובץ שמע בעמוד השיעור באתר הישיבה"
            if website_link:
                description += f"\n\nקישור לשיעור באתר הישיבה:\n{website_link}"
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
                        youtube_url = f"https://www.youtube.com/watch?v={youtube_video_id}"
                        df.at[idx, "youtube_url"] = youtube_url
                        logger.info(f"🔗 נשמר קישור: {youtube_url}")
                        # Notify site to update provider in DB and mark in CSV
                        notified = notify_site_update_provider(csv_id, youtube_url)
                        df.at[idx, "provider_updated"] = "yes" if notified else "error"
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
