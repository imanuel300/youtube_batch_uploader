# YouTube Batch Uploader 🎬

## ✅ תיאור

סקריפט Python לעדכון YouTube אצווה - מוריד קבצי וידאו מכתובות URL ומעלה אותם ליוטיוב אוטומטית.

### תכונות:
- ✅ מוריד קבצי וידאו מכתובות URL
- ✅ מעלה ליוטיוב עם YouTube Data API v3
- ✅ מסמן קבצים שהועלו בהצלחה בקובץ CSV
- ✅ דילוג על קבצים שכבר הורדו
- ✅ חידוש העלאה אוטומטי במקרה של נפילת אינטרנט
- ✅ לוגים מפורטים לקובץ
- ✅ התקדמות מלאה עם progress bars

## 📦 דרישות מקדימות

### התקנת חבילות Python
```bash
pip install google-api-python-client google-auth-oauthlib google-auth-httplib2 pandas requests tqdm
```

### קבצי הרשאות YouTube
1. פתח [Google Cloud Console](https://console.cloud.google.com/)
2. צור פרויקט חדש או בחר פרויקט קיים
3. הפעל את **YouTube Data API v3**
4. צור **OAuth 2.0 Client ID** מסוג Desktop application
5. הורד את `credentials.json` ושמור אותו בתיקיית הפרויקט

⚠️ **חשוב**: קובץ `credentials.json` לא יועלה ל-Git (מופיע ב-.gitignore)

## 📁 מבנה הקובץ CSV

קובץ `videos.csv` צריך להכיל את העמודות הבאות:

| עמודה | תיאור | דוגמה |
|-------|-------|-------|
| `file_url` | כתובת URL של קובץ הווידאו | `https://example.com/video1.mp4` |
| `title` | כותרת הסרטון | `Lesson 1` |
| `description` | תיאור הסרטון | `Intro lesson` |
| `tags` | תגיות (מופרדות בפסיקים) | `python,tutorial` |
| `uploaded` | סטטוס העלאה (`yes`/`no`) | `no` |

### דוגמה לקובץ CSV:
```csv
file_url,title,description,tags,uploaded
https://example.com/video1.mp4,Lesson 1,Intro lesson,"python,tutorial",no
https://example.com/video2.mp4,Lesson 2,Deep dive lesson,"python,advanced",no
```

## 🚀 איך להריץ

```bash
python youtube_uploader.py
```

### תהליך העבודה:
1. הסקריפט יבקש הרשאות YouTube בפעם הראשונה (פותח דפדפן)
2. מוריד קבצים לתיקייה `downloads/`
3. מעלה כל קובץ ליוטיוב
4. מעדכן את הקובץ CSV עם סטטוס `uploaded = yes`
5. שומר לוגים בקובץ `upload_log.log`

## 📝 קבצים וקבצים

- `youtube_uploader.py` - הסקריפט הראשי
- `videos.csv` - קובץ CSV עם רשימת הסרטונים
- `downloads/` - תיקיית קבצים מורדים
- `upload_log.log` - קובץ לוגים מפורט
- `credentials.json` - קובץ הרשאות Google (לא ב-Git)
- `token.pickle` - טוקן אימות (לא ב-Git)

## 🔒 אבטחה

קבצי הרשאות וטוקנים אינם נשמרים ב-Git בזכות קובץ `.gitignore`.

## ⚙️ תכונות מתקדמות

### חידוש העלאה אוטומטי
אם יש נפילת אינטרנט במהלך העלאה, הסקריפט ינסה לחדש את ההעלאה אוטומטית.

### דילוג על קבצים קיימים
אם קובץ כבר קיים בתיקיית `downloads/`, הסקריפט ידלג על ההורדה ויעלה ישירות את הקובץ הקיים.

### לוגים
כל הפעולות נשמרות בקובץ `upload_log.log` עם חותמת זמן מפורטת.
