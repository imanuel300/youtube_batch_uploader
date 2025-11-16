import logging
import os
import sys
import time
from typing import Optional

import pandas as pd
import requests
from requests import Response
from urllib.parse import quote

CSV_FILE = "videos.csv"
LOG_FILE = "cleanup_log.log"

# Rackspace Cloud Files (UK) credentials
AUTH_URL = "https://lon.identity.api.rackspacecloud.com/v2.0/tokens"
USERNAME = ""
API_KEY = ""
# Container (folder) holding the videos
CONTAINER_NAME = "ateretMordecay"

# Column name to mark deletion status
DELETED_COLUMN = "remote_deleted"

# Time to sleep between delete requests to avoid throttling
DELETE_DELAY_SECONDS = 0.5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class RackspaceClient:
    def __init__(self, username: str, api_key: str):
        self.username = username
        self.api_key = api_key
        self._token: Optional[str] = None
        self._storage_url: Optional[str] = None

    def authenticate(self) -> None:
        logger.info("🔐 מבצע אימות מול Rackspace Cloud Files...")

        payload = {
            "auth": {
                "RAX-KSKEY:apiKeyCredentials": {
                    "username": self.username,
                    "apiKey": self.api_key,
                }
            }
        }

        response = requests.post(AUTH_URL, json=payload, timeout=20)
        self._handle_response(response, "אימות נכשל")

        data = response.json()
        self._token = data["access"]["token"]["id"]

        # Locate the object storage endpoint for the LON (UK) region
        service_catalog = data["access"].get("serviceCatalog", [])
        object_store = next(
            (
                service
                for service in service_catalog
                if service.get("type") == "object-store"
            ),
            None,
        )

        if not object_store:
            raise RuntimeError("Object storage endpoint not found in service catalog")

        endpoints = object_store.get("endpoints", [])
        endpoint = next(
            (
                ep
                for ep in endpoints
                if ep.get("region") == "LON" and ep.get("publicURL")
            ),
            None,
        )

        if not endpoint:
            raise RuntimeError("LON endpoint for object storage not found")

        self._storage_url = endpoint["publicURL"].rstrip("/")
        logger.info("✅ אימות הצליח, כתובת אחסון: %s", self._storage_url)

    @property
    def token(self) -> str:
        if not self._token:
            raise RuntimeError("Client is not authenticated")
        return self._token

    @property
    def storage_url(self) -> str:
        if not self._storage_url:
            raise RuntimeError("Client is not authenticated")
        return self._storage_url

    def delete_object(self, object_path: str) -> str:
        if not object_path:
            raise ValueError("Object path is empty")

        if not self._token:
            self.authenticate()

        # Normalize path and ensure we don't duplicate slashes
        object_path = object_path.strip()
        if object_path.startswith("/"):
            object_path = object_path[1:]

        full_object_path = f"{CONTAINER_NAME}/{object_path}"

        encoded_path = quote(full_object_path, safe="/:")
        url = f"{self.storage_url}/{encoded_path}"

        logger.info("🗑️ מוחק בקשה: %s", url)
        response = requests.delete(url, headers={"X-Auth-Token": self.token}, timeout=30)

        if response.status_code in (204, 404):
            if response.status_code == 204:
                logger.info("✅ הקובץ נמחק מהשרת")
                return "yes"
            else:
                logger.warning("⚠️ הקובץ לא נמצא בשרת (כבר נמחק?)")
                return "not_found"

        self._handle_response(response, "מחיקת קובץ נכשלה")
        return "error"

    @staticmethod
    def _handle_response(response: Response, error_message: str) -> None:
        if response.ok:
            return
        try:
            details = response.json()
        except ValueError:
            details = response.text
        raise RuntimeError(f"{error_message}: {response.status_code} | {details}")


def ensure_deleted_column(df: pd.DataFrame) -> pd.DataFrame:
    if DELETED_COLUMN not in df.columns:
        df[DELETED_COLUMN] = ""
    else:
        df[DELETED_COLUMN] = df[DELETED_COLUMN].fillna("")
    return df


def main():
    if not os.path.exists(CSV_FILE):
        logger.error("❌ קובץ CSV לא נמצא: %s", CSV_FILE)
        sys.exit(1)

    df = pd.read_csv(CSV_FILE)
    df = ensure_deleted_column(df)

    client = RackspaceClient(USERNAME, API_KEY)
    client.authenticate()

    total_rows = len(df)
    logger.info("📄 קורא %s שורות מה-CSV", total_rows)

    processed = 0
    deleted = 0

    for idx, row in df.iterrows():
        uploaded_status = str(row.get("uploaded", "")).strip().lower()
        youtube_url = str(row.get("youtube_url", "")).strip()
        deleted_status = str(row.get(DELETED_COLUMN, "")).strip().lower()

        if uploaded_status != "yes" or not youtube_url:
            continue

        if deleted_status == "yes":
            logger.info("⏭️ שורה %s כבר נמחקה בעבר, מדלג", idx + 1)
            continue

        object_path = str(row.get("url", "")).strip()
        if not object_path:
            logger.warning("⚠️ אין נתיב לקובץ בשורה %s, מדלג", idx + 1)
            continue

        try:
            status = client.delete_object(object_path)
            if status == "yes":
                df.at[idx, DELETED_COLUMN] = "yes"
                deleted += 1
            else:
                df.at[idx, DELETED_COLUMN] = status
        except Exception as exc:
            logger.error("❌ שגיאה במחיקה עבור שורה %s: %s", idx + 1, exc)
            df.at[idx, DELETED_COLUMN] = f"error: {exc}"  # Keep error for reference

        processed += 1
        time.sleep(DELETE_DELAY_SECONDS)

    df.to_csv(CSV_FILE, index=False)

    logger.info("✅ ניקוי הסתיים.\n📊 טופלו: %s\n🗑️ נמחקו: %s", processed, deleted)


if __name__ == "__main__":
    main()
