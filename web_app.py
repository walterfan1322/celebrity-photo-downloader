# -*- coding: utf-8 -*-
"""
明星照片下載器 Web 版 v1.0
透過瀏覽器操作，區網內任何裝置皆可使用
"""

import os, sys, re, json, time, hashlib, sqlite3, threading, logging, uuid, shutil
import html as html_mod
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
from queue import Queue, Empty

import requests as http_requests
from PIL import Image
from flask import Flask, request, Response, jsonify, send_from_directory

# 確保同目錄的模組可被 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from video_maker import generate_video as _gen_video, TEMPLATES as VIDEO_TEMPLATES
    HAS_VIDEO = True
except ImportError:
    HAS_VIDEO = False
    VIDEO_TEMPLATES = {}

try:
    import imagehash
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False

try:
    import yt_dlp
    HAS_YTDLP = True
except ImportError:
    HAS_YTDLP = False

# ── 偵測 FFmpeg（yt-dlp 需要） ──
FFMPEG_LOCATION = None
if shutil.which("ffmpeg"):
    pass  # already in PATH, yt-dlp will find it
else:
    try:
        import imageio_ffmpeg
        _ffpath = imageio_ffmpeg.get_ffmpeg_exe()
        if os.path.isfile(_ffpath):
            # imageio_ffmpeg 的執行檔名不是 ffmpeg.exe，
            # 需複製一份讓 yt-dlp 能找到
            _ffdir = os.path.join(os.path.dirname(_ffpath), "_ytdlp")
            _ffcopy = os.path.join(_ffdir, "ffmpeg.exe")
            if not os.path.isfile(_ffcopy):
                os.makedirs(_ffdir, exist_ok=True)
                shutil.copy2(_ffpath, _ffcopy)
            FFMPEG_LOCATION = _ffdir
    except (ImportError, OSError):
        pass

# ── 設定（可透過環境變數覆寫） ─────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", os.path.join(APP_DIR, "Photos"))
VIDEO_ROOT = os.environ.get("VIDEO_ROOT", os.path.join(APP_DIR, "Videos"))
YT_ROOT = os.environ.get("YT_ROOT", os.path.join(APP_DIR, "YouTube"))
YT_DOWNLOADS = os.path.join(YT_ROOT, "downloads")
YT_EXTRACTS = os.path.join(YT_ROOT, "extracts")
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "history.db")
PHASH_THRESHOLD = 8
MAX_FILE_SIZE = 20 * 1024 * 1024
PORT = int(os.environ.get("PORT", "5000"))
CHROME_DEBUG_PORT = int(os.environ.get("CHROME_DEBUG_PORT", "9222"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SIZE_FILTERS = {
    "any": "",
    "large": "+filterui:imagesize-large",
    "xlarge": "+filterui:imagesize-wallpaper",
}

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
os.makedirs(VIDEO_ROOT, exist_ok=True)


def _migrate_yt_flat_to_structured():
    """一次性遷移：將 YouTube/ 根目錄的平放檔案搬到 downloads/_unsorted/"""
    if not os.path.isdir(YT_ROOT):
        os.makedirs(YT_DOWNLOADS, exist_ok=True)
        os.makedirs(YT_EXTRACTS, exist_ok=True)
        return
    # 如果已經有 downloads/ 子目錄就跳過
    if os.path.isdir(YT_DOWNLOADS):
        return
    exts = {".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".opus"}
    flat_files = [f for f in os.listdir(YT_ROOT)
                  if os.path.isfile(os.path.join(YT_ROOT, f))
                  and os.path.splitext(f)[1].lower() in exts]
    os.makedirs(YT_DOWNLOADS, exist_ok=True)
    os.makedirs(YT_EXTRACTS, exist_ok=True)
    if not flat_files:
        return
    unsorted = os.path.join(YT_DOWNLOADS, "_unsorted")
    os.makedirs(unsorted, exist_ok=True)
    for f in flat_files:
        src = os.path.join(YT_ROOT, f)
        dst = os.path.join(unsorted, f)
        os.rename(src, dst)
    logging.info("Migrated %d YT files to downloads/_unsorted/", len(flat_files))


_migrate_yt_flat_to_structured()


# ══════════════════════════════════════════════════════════
#  資料庫
# ══════════════════════════════════════════════════════════
class DatabaseManager:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init()

    def _init(self):
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    celebrity TEXT NOT NULL, url TEXT NOT NULL,
                    filename TEXT NOT NULL, md5 TEXT, phash TEXT,
                    size INTEGER, width INTEGER, height INTEGER,
                    source TEXT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_url ON downloads(url);
                CREATE INDEX IF NOT EXISTS idx_md5 ON downloads(md5);
                CREATE INDEX IF NOT EXISTS idx_celeb ON downloads(celebrity);
                CREATE TABLE IF NOT EXISTS used_keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    celebrity TEXT NOT NULL, keyword TEXT NOT NULL,
                    source TEXT, new_downloaded INTEGER DEFAULT 0,
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_kw_celeb ON used_keywords(celebrity);
                CREATE TABLE IF NOT EXISTS photo_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    celebrity TEXT NOT NULL, filename TEXT NOT NULL,
                    usage_type TEXT NOT NULL,
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_pu_celeb ON photo_usage(celebrity);
                CREATE INDEX IF NOT EXISTS idx_pu_cf ON photo_usage(celebrity, filename);
                CREATE TABLE IF NOT EXISTS celebrity_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical TEXT NOT NULL,
                    alias TEXT NOT NULL UNIQUE,
                    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_alias ON celebrity_aliases(alias);
            """)

    def url_exists(self, url):
        with self.lock:
            return self.conn.execute(
                "SELECT 1 FROM downloads WHERE url=? LIMIT 1", (url,)
            ).fetchone() is not None

    def md5_exists(self, md5):
        with self.lock:
            return self.conn.execute(
                "SELECT 1 FROM downloads WHERE md5=? LIMIT 1", (md5,)
            ).fetchone() is not None

    def get_phashes(self, celebrity):
        with self.lock:
            rows = self.conn.execute(
                "SELECT phash FROM downloads WHERE celebrity=? AND phash IS NOT NULL",
                (celebrity,),
            ).fetchall()
            return [r[0] for r in rows]

    def add(self, celebrity, url, filename, md5, phash, size, w, h, source):
        with self.lock:
            self.conn.execute(
                "INSERT INTO downloads"
                "(celebrity,url,filename,md5,phash,size,width,height,source)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (celebrity, url, filename, md5, phash, size, w, h, source),
            )
            self.conn.commit()

    def count(self, celebrity=None):
        with self.lock:
            if celebrity:
                return self.conn.execute(
                    "SELECT COUNT(*) FROM downloads WHERE celebrity=?", (celebrity,)
                ).fetchone()[0]
            return self.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]

    def celebrities(self):
        with self.lock:
            return self.conn.execute(
                "SELECT celebrity, COUNT(*) as n, MAX(ts) "
                "FROM downloads GROUP BY celebrity ORDER BY MAX(ts) DESC"
            ).fetchall()

    def get_used_keywords(self, celebrity):
        with self.lock:
            rows = self.conn.execute(
                "SELECT keyword FROM used_keywords WHERE celebrity=?", (celebrity,)
            ).fetchall()
            return [r[0] for r in rows]

    def add_used_keyword(self, celebrity, keyword, source, new_downloaded):
        with self.lock:
            self.conn.execute(
                "INSERT INTO used_keywords(celebrity,keyword,source,new_downloaded)"
                " VALUES(?,?,?,?)",
                (celebrity, keyword, source, new_downloaded),
            )
            self.conn.commit()

    # ── 照片使用紀錄 ──────────────────────────────────
    def mark_photos_used(self, celebrity, filenames, usage_type):
        """記錄照片被使用 (download/video/removed)"""
        with self.lock:
            self.conn.executemany(
                "INSERT INTO photo_usage(celebrity,filename,usage_type)"
                " VALUES(?,?,?)",
                [(celebrity, f, usage_type) for f in filenames],
            )
            self.conn.commit()

    def get_photo_usage_counts(self, celebrity):
        """回傳 {filename: 使用次數}"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT filename, COUNT(*) FROM photo_usage"
                " WHERE celebrity=? GROUP BY filename",
                (celebrity,),
            ).fetchall()
            return {r[0]: r[1] for r in rows}

    # ── 明星別名 ──────────────────────────────────────
    def resolve_alias(self, name):
        """
        查別名表，回傳正式名稱。
        找不到就回傳原名（小寫化）。
        """
        with self.lock:
            row = self.conn.execute(
                "SELECT canonical FROM celebrity_aliases WHERE alias=? COLLATE NOCASE LIMIT 1",
                (name,),
            ).fetchone()
            if row:
                return row[0]
            # 也可能本身就是 canonical
            row2 = self.conn.execute(
                "SELECT canonical FROM celebrity_aliases WHERE canonical=? COLLATE NOCASE LIMIT 1",
                (name,),
            ).fetchone()
            if row2:
                return row2[0]
            return name

    def add_alias(self, canonical, alias):
        """新增別名"""
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT OR REPLACE INTO celebrity_aliases(canonical, alias) VALUES(?,?)",
                    (canonical, alias),
                )
                self.conn.commit()
                return True
            except Exception:
                return False

    def remove_alias(self, alias):
        with self.lock:
            self.conn.execute("DELETE FROM celebrity_aliases WHERE alias=?", (alias,))
            self.conn.commit()

    def get_aliases(self, canonical=None):
        """取得別名列表。若指定 canonical，只回傳該名的別名。"""
        with self.lock:
            if canonical:
                rows = self.conn.execute(
                    "SELECT canonical, alias FROM celebrity_aliases WHERE canonical=? COLLATE NOCASE",
                    (canonical,),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT canonical, alias FROM celebrity_aliases ORDER BY canonical"
                ).fetchall()
            return [{"canonical": r[0], "alias": r[1]} for r in rows]

    def update_celebrity_name(self, old_name, new_name):
        """更新所有資料表中的Photo 名稱（合併用）"""
        with self.lock:
            self.conn.execute("UPDATE downloads SET celebrity=? WHERE celebrity=?", (new_name, old_name))
            self.conn.execute("UPDATE used_keywords SET celebrity=? WHERE celebrity=?", (new_name, old_name))
            self.conn.execute("UPDATE photo_usage SET celebrity=? WHERE celebrity=?", (new_name, old_name))
            self.conn.commit()


# ══════════════════════════════════════════════════════════
#  Bing 圖片搜尋
# ══════════════════════════════════════════════════════════
class BingImageScraper:
    def __init__(self):
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        urls, seen = [], set()
        offset, empty = 0, 0
        while len(urls) < max_num and empty < 3:
            try:
                page = self._fetch(keyword, offset, size_filter)
            except Exception:
                empty += 1; offset += 35; time.sleep(1); continue
            new = 0
            for u in page:
                if u not in seen:
                    seen.add(u); urls.append(u); new += 1
                    if callback:
                        callback(f"搜尋中... 找到 {len(urls)} 個連結")
                    if len(urls) >= max_num:
                        break
            empty = 0 if new else empty + 1
            offset += 35; time.sleep(0.4)
        return urls[:max_num]

    def _fetch(self, keyword, offset, size_filter=""):
        qft = "+filterui:photo-photo" + (size_filter or "")
        resp = self.session.get(
            "https://www.bing.com/images/search",
            params={"q": keyword, "first": offset, "count": 35,
                    "qft": qft, "form": "IRFLTR"},
            timeout=15,
        )
        resp.raise_for_status()
        result = []
        for m_raw in re.findall(r'class="iusc"[^>]*m="([^"]+)"', resp.text):
            try:
                d = json.loads(html_mod.unescape(m_raw))
                if d.get("murl", "").startswith("http"):
                    result.append(d["murl"])
            except Exception:
                continue
        if not result:
            for m in re.finditer(r'"murl"\s*:\s*"(https?://[^"]+)"', resp.text):
                try:
                    result.append(json.loads(f'"{m.group(1)}"'))
                except Exception:
                    result.append(m.group(1))
        return result


# ══════════════════════════════════════════════════════════
#  Google 圖片搜尋
# ══════════════════════════════════════════════════════════
class GoogleImageScraper:
    def __init__(self):
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        urls, seen = [], set()
        start, empty = 0, 0
        while len(urls) < max_num and empty < 3:
            try:
                page = self._fetch(keyword, start, size_filter)
            except Exception:
                empty += 1; start += 20; time.sleep(1); continue
            new = 0
            for u in page:
                if u not in seen:
                    seen.add(u); urls.append(u); new += 1
                    if callback:
                        callback(f"Google 搜尋中... 找到 {len(urls)} 個連結")
                    if len(urls) >= max_num:
                        break
            empty = 0 if new else empty + 1
            start += 20; time.sleep(0.5)
        return urls[:max_num]

    def _fetch(self, keyword, start, size_filter=""):
        tbs = ""
        if "large" in size_filter:
            tbs = "isz:l"
        elif "wallpaper" in size_filter:
            tbs = "isz:lt,islt:4mp"
        params = {"q": keyword, "tbm": "isch", "start": start,
                  "ijn": str(start // 20)}
        if tbs:
            params["tbs"] = tbs
        resp = self.session.get(
            "https://www.google.com/search", params=params, timeout=15)
        resp.raise_for_status()
        result = []
        # 從頁面中解析圖片 URL 陣列 ["url", w, h]
        for m in re.finditer(
            r'\["(https?://[^"]{10,})",[0-9]+,[0-9]+\]', resp.text
        ):
            u = m.group(1).replace("\\u003d", "=").replace("\\u0026", "&")
            if not any(x in u for x in (
                "google.com", "gstatic.com", "googleapis.com",
                "googleusercontent.com", "ytimg.com",
            )):
                result.append(u)
        # 備用: "ou" 欄位
        if not result:
            for m in re.finditer(r'"ou":"(https?://[^"]+)"', resp.text):
                result.append(m.group(1))
        return result


# ══════════════════════════════════════════════════════════
#  DuckDuckGo 圖片搜尋
# ══════════════════════════════════════════════════════════
class DuckDuckGoImageScraper:
    def __init__(self):
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        # 取得 vqd token
        try:
            resp = self.session.get(
                "https://duckduckgo.com/",
                params={"q": keyword, "iax": "images", "ia": "images"},
                timeout=15,
            )
            vqd_m = re.search(r'vqd=["\']([^"\']+)', resp.text)
            if not vqd_m:
                return []
            vqd = vqd_m.group(1)
        except Exception:
            return []

        sz = ""
        if "large" in size_filter:
            sz = "Large"
        elif "wallpaper" in size_filter:
            sz = "Wallpaper"

        urls, seen = [], set()
        f_param = f",size,{sz},,," if sz else ",,,,,"
        next_url = (
            f"https://duckduckgo.com/i.js?l=us-en&o=json"
            f"&q={keyword}&vqd={vqd}&f={f_param}&p=1"
        )
        empty = 0
        while len(urls) < max_num and empty < 3 and next_url:
            try:
                resp = self.session.get(next_url, timeout=15)
                data = resp.json()
            except Exception:
                empty += 1; time.sleep(1); continue
            results = data.get("results", [])
            if not results:
                break
            new = 0
            for item in results:
                img = item.get("image", "")
                if img and img not in seen:
                    seen.add(img); urls.append(img); new += 1
                    if callback:
                        callback(f"DuckDuckGo 搜尋中... 找到 {len(urls)} 個連結")
                    if len(urls) >= max_num:
                        break
            empty = 0 if new else empty + 1
            nxt = data.get("next")
            next_url = ("https://duckduckgo.com" + nxt) if nxt else None
            time.sleep(0.4)
        return urls[:max_num]


# ══════════════════════════════════════════════════════════
#  Pinterest 圖片搜尋（透過內部 API）
# ══════════════════════════════════════════════════════════
class PinterestImageScraper:
    def __init__(self):
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)
        self._csrf = None

    def _init_session(self):
        """取得 Pinterest cookies 和 CSRF token"""
        if self._csrf:
            return
        try:
            self.session.get("https://www.pinterest.com/", timeout=15)
            self._csrf = self.session.cookies.get("csrftoken", "")
            self.session.headers.update({
                "X-CSRFToken": self._csrf,
                "X-Requested-With": "XMLHttpRequest",
                "X-Pinterest-AppState": "active",
                "X-Pinterest-PWS-Handler": "www/search/[scope].js",
                "Referer": "https://www.pinterest.com/",
                "Accept": "*/*",
            })
        except Exception:
            pass

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        self._init_session()
        urls, seen = [], set()
        bookmarks = []
        empty = 0

        while len(urls) < max_num and empty < 3:
            try:
                data = json.dumps({
                    "options": {
                        "query": keyword,
                        "redux_normalize_feed": True,
                        "rs": "typed",
                        "scope": "pins",
                        "page_size": 25,
                        "bookmarks": bookmarks,
                        "applied_unified_filters": None,
                        "appliedProductFilters": "---",
                        "source_url": f"/search/pins/?q={keyword}",
                    },
                    "context": {},
                })
                resp = self.session.get(
                    "https://www.pinterest.com/resource/BaseSearchResource/get/",
                    params={
                        "source_url": f"/search/pins/?q={keyword}&rs=typed",
                        "data": data,
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    empty += 1; time.sleep(1); continue
                d = resp.json()
                resource = d.get("resource_response", {})
                results = resource.get("data", {}).get("results", [])
                if not results:
                    break
                new = 0
                for pin in results:
                    imgs = pin.get("images", {})
                    # 優先取 originals，其次 736x
                    img_url = ""
                    for key in ("orig", "originals", "736x", "564x"):
                        u = imgs.get(key, {}).get("url", "")
                        if u:
                            img_url = u; break
                    if img_url and img_url not in seen:
                        seen.add(img_url); urls.append(img_url); new += 1
                        if callback:
                            callback(f"Pinterest 搜尋中... 找到 {len(urls)} 個連結")
                        if len(urls) >= max_num:
                            break
                empty = 0 if new else empty + 1
                # 取得下一頁 bookmark
                bm = resource.get("bookmark")
                if bm:
                    bookmarks = [bm]
                else:
                    break
                time.sleep(0.5)
            except Exception:
                empty += 1; time.sleep(1)

        return urls[:max_num]


# ── 爬蟲對照表 ────────────────────────────────────────────
SCRAPERS = {
    "bing": BingImageScraper,
    "google": GoogleImageScraper,
    "duckduckgo": DuckDuckGoImageScraper,
    "pinterest": PinterestImageScraper,
}

# ── 關鍵字變體 ────────────────────────────────────────────
KEYWORD_SUFFIXES = [
    "photo", "HD photo", "portrait", "photoshoot",
    "wallpaper", "high resolution", "professional photo",
    "red carpet", "magazine", "editorial",
    "close up", "fashion", "event photo",
    "award show", "interview", "behind the scenes",
    "candid", "paparazzi", "studio photo",
    "2024", "2025", "2026", "latest",
]


def generate_keywords(celebrity, append_photo=True):
    """產生該明星的所有搜尋關鍵字變體"""
    keywords = []
    if append_photo:
        keywords.append(f"{celebrity} photo")
    else:
        keywords.append(celebrity)
    for suffix in KEYWORD_SUFFIXES:
        kw = f"{celebrity} {suffix}"
        if kw not in keywords:
            keywords.append(kw)
    return keywords


# ══════════════════════════════════════════════════════════
#  圖片下載器
# ══════════════════════════════════════════════════════════
def sanitize_name(name):
    """資料夾名稱：移除非法字元 + 統一小寫"""
    cleaned = "".join(c for c in name if c not in r'<>:"/\|?*').strip(". ") or "unknown"
    return cleaned.lower()


def resolve_celebrity(name):
    """解析別名 → 正式名稱（用於搜尋 & 資料夾）"""
    canonical = db.resolve_alias(name.strip())
    return canonical


class ImageDownloader:
    def __init__(self, db, celebrity, source="bing"):
        self.db = db
        self.celebrity = celebrity
        self.source = source
        self.cel_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
        os.makedirs(self.cel_dir, exist_ok=True)
        self.session = http_requests.Session()
        self.session.headers.update(HEADERS)
        self._stop = threading.Event()
        self.stats = dict(downloaded=0, skip_url=0, skip_md5=0, skip_phash=0, failed=0)
        self._phash_cache = []
        if HAS_IMAGEHASH:
            for h in db.get_phashes(celebrity):
                try:
                    self._phash_cache.append(imagehash.hex_to_hash(h))
                except Exception:
                    pass
        self._next = db.count(celebrity) + 1

    def stop(self):
        self._stop.set()

    def download_all(self, urls, dedup_url=True, dedup_md5=True,
                     dedup_phash=True, progress_cb=None, log_cb=None):
        total = len(urls)
        for i, url in enumerate(urls):
            if self._stop.is_set():
                if log_cb: log_cb("stop", "使用者中止", {})
                break
            r = self._one(url, dedup_url, dedup_md5, dedup_phash)
            if progress_cb: progress_cb(i + 1, total, self.stats)
            if log_cb: log_cb(r["status"], r.get("msg", ""), r)
        return self.stats

    def _one(self, url, du, dm, dp):
        if du and self.db.url_exists(url):
            self.stats["skip_url"] += 1
            return {"status": "skip_url", "msg": "URL 已存在，跳過"}
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            self.stats["failed"] += 1
            return {"status": "error", "msg": f"下載失敗: {e}"}
        if len(data) > MAX_FILE_SIZE or len(data) < 1000:
            self.stats["failed"] += 1
            return {"status": "error", "msg": "檔案大小異常"}
        try:
            img = Image.open(BytesIO(data)); img.verify()
            img = Image.open(BytesIO(data))
            w, h = img.size
        except Exception:
            self.stats["failed"] += 1
            return {"status": "error", "msg": "非有效圖片"}
        md5 = hashlib.md5(data).hexdigest()
        if dm and self.db.md5_exists(md5):
            self.stats["skip_md5"] += 1
            return {"status": "skip_md5", "msg": "MD5 重複，跳過"}
        phash_str = None
        if HAS_IMAGEHASH and dp:
            try:
                pv = imagehash.phash(img); phash_str = str(pv)
                for ex in self._phash_cache:
                    if abs(pv - ex) < PHASH_THRESHOLD:
                        self.stats["skip_phash"] += 1
                        return {"status": "skip_phash", "msg": "相似圖片已存在"}
            except Exception:
                pass
        ext = self._ext(url, data)
        fn = f"{self._next:05d}{ext}"
        fp = os.path.join(self.cel_dir, fn)
        while os.path.exists(fp):
            self._next += 1
            fn = f"{self._next:05d}{ext}"; fp = os.path.join(self.cel_dir, fn)
        with open(fp, "wb") as f:
            f.write(data)
        self.db.add(self.celebrity, url, fn, md5, phash_str, len(data), w, h, self.source)
        if phash_str and HAS_IMAGEHASH:
            self._phash_cache.append(imagehash.hex_to_hash(phash_str))
        self._next += 1
        self.stats["downloaded"] += 1
        return {"status": "ok", "msg": f"{fn} ({w}x{h}, {len(data)//1024}KB)"}

    @staticmethod
    def _ext(url, data):
        p = urlparse(url).path.lower()
        for e in (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"):
            if p.endswith(e): return ".jpg" if e == ".jpeg" else e
        if data[:3] == b"\xff\xd8\xff": return ".jpg"
        if data[:8] == b"\x89PNG\r\n\x1a\n": return ".png"
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP": return ".webp"
        return ".jpg"


# ══════════════════════════════════════════════════════════
#  Flask Web 應用
# ══════════════════════════════════════════════════════════
app = Flask(__name__)
db = DatabaseManager(DB_PATH)

# 任務管理
tasks = {}  # task_id -> {queue, downloader, thread, status}
tasks_lock = threading.Lock()


@app.route("/")
def index():
    return HTML_PAGE


@app.route("/api/celebrities")
def api_celebrities():
    rows = db.celebrities()
    return jsonify([
        {"name": r[0], "count": r[1], "last": r[2][:16] if r[2] else ""}
        for r in rows
    ])


@app.route("/api/stats")
def api_stats():
    rows = db.celebrities()
    return jsonify({
        "total_celebs": len(rows),
        "total_photos": sum(r[1] for r in rows),
        "has_imagehash": HAS_IMAGEHASH,
    })


@app.route("/api/images/<celebrity>")
def api_images(celebrity):
    d = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(d):
        return jsonify([])
    imgs = []
    for f in sorted(os.listdir(d)):
        if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")):
            fp = os.path.join(d, f)
            imgs.append({
                "filename": f,
                "url": f"/photos/{sanitize_name(celebrity)}/{f}",
                "size_kb": round(os.path.getsize(fp) / 1024),
            })
    return jsonify(imgs)


@app.route("/photos/<path:filepath>")
def serve_photo(filepath):
    parts = filepath.replace("\\", "/").split("/", 1)
    if len(parts) != 2:
        return "Not found", 404
    celeb_dir = os.path.join(DOWNLOAD_ROOT, parts[0])
    if not os.path.isdir(celeb_dir):
        return "Not found", 404
    return send_from_directory(celeb_dir, parts[1])


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if not celebrity:
        return jsonify({"error": "請輸入Photo 名稱"}), 400
    celebrity = resolve_celebrity(celebrity)

    task_id = str(uuid.uuid4())[:8]
    q = Queue()

    with tasks_lock:
        tasks[task_id] = {"queue": q, "downloader": None, "status": "running"}

    t = threading.Thread(
        target=_download_worker,
        args=(task_id, q, celebrity, data),
        daemon=True,
    )
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/batch", methods=["POST"])
def api_batch():
    data = request.json
    names = [n.strip() for n in (data.get("names") or "").split("\n") if n.strip()]
    if not names:
        return jsonify({"error": "請輸入至少一個名稱"}), 400

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with tasks_lock:
        tasks[task_id] = {"queue": q, "downloader": None, "status": "running"}

    t = threading.Thread(
        target=_batch_worker,
        args=(task_id, q, names, data),
        daemon=True,
    )
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/stop/<task_id>", methods=["POST"])
def api_stop(task_id):
    with tasks_lock:
        task = tasks.get(task_id)
    if task and task.get("downloader"):
        task["downloader"].stop()
    return jsonify({"ok": True})


@app.route("/api/progress/<task_id>")
def api_progress(task_id):
    def generate():
        with tasks_lock:
            task = tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type':'error','msg':'任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type':'heartbeat'})}\n\n"
        # 清理
        with tasks_lock:
            tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _download_worker(task_id, q, celebrity, opts, is_batch=False):
    try:
        sf = SIZE_FILTERS.get(opts.get("size", "any"), "")
        if opts.get("face_only"):
            sf += "+filterui:face-face"

        max_num = int(opts.get("count", 100))
        sources = opts.get("sources", ["bing"])
        append_photo = opts.get("append_photo", True)
        auto_keyword = opts.get("auto_keyword", True)

        # 產生關鍵字列表，過濾掉已用過的
        if auto_keyword:
            all_keywords = generate_keywords(celebrity, append_photo)
            used = set(db.get_used_keywords(celebrity))
            keywords = [kw for kw in all_keywords if kw not in used]
            if not keywords:
                q.put({"type": "log", "msg": "所有關鍵字變體都已用過，重新從頭開始", "tag": "info"})
                keywords = all_keywords
            q.put({"type": "log",
                    "msg": f"可用關鍵字: {len(keywords)} 組（已用過: {len(used)} 組）",
                    "tag": "info"})
        else:
            kw = celebrity
            if append_photo:
                kw += " photo"
            keywords = [kw]

        # 用多組關鍵字搜尋，直到湊夠目標數量
        all_urls = []
        seen_urls = set()
        keywords_used_this_run = []

        for keyword in keywords:
            if len(all_urls) >= max_num:
                break

            keywords_used_this_run.append(keyword)
            q.put({"type": "log", "msg": f"━ 搜尋關鍵字: {keyword}", "tag": "info"})
            remaining = max_num - len(all_urls)

            for src in sources:
                scraper_cls = SCRAPERS.get(src)
                if not scraper_cls:
                    continue
                src_label = {"bing": "Bing", "google": "Google",
                             "duckduckgo": "DuckDuckGo", "pinterest": "Pinterest"}.get(src, src)
                q.put({"type": "log", "msg": f"從 {src_label} 搜尋: {keyword}", "tag": "info"})
                try:
                    scraper = scraper_cls()
                    urls = scraper.search(
                        keyword, remaining, sf,
                        callback=lambda msg: q.put({"type": "log", "msg": msg, "tag": "info"}),
                    )
                    new = 0
                    for u in urls:
                        if u not in seen_urls:
                            seen_urls.add(u); all_urls.append(u); new += 1
                    q.put({"type": "log", "msg": f"{src_label}: 找到 {new} 個新連結", "tag": "info"})
                except Exception as e:
                    q.put({"type": "log", "msg": f"{src_label} 搜尋失敗: {e}", "tag": "error"})

        urls = all_urls
        if not urls:
            q.put({"type": "log", "msg": "未找到任何圖片", "tag": "error"})
            q.put({"type": "done", "stats": {}})
            return

        q.put({"type": "log", "msg": f"共找到 {len(urls)} 個連結，開始下載...", "tag": "info"})

        dl = ImageDownloader(db, celebrity, ",".join(sources))
        with tasks_lock:
            task = tasks.get(task_id)
            if task:
                task["downloader"] = dl

        def pcb(cur, tot, stats):
            q.put({"type": "progress", "current": cur, "total": tot, "stats": stats})

        def lcb(status, msg, detail):
            pfx = {"ok": "✓", "skip_url": "⊘", "skip_md5": "⊘",
                    "skip_phash": "⊘", "error": "✗", "stop": "■"}.get(status, "→")
            tag = {"ok": "ok", "error": "error", "stop": "stop"}.get(status, "skip")
            q.put({"type": "log", "msg": f"{pfx} {msg}", "tag": tag})

        stats = dl.download_all(
            urls,
            dedup_url=opts.get("dedup_url", True),
            dedup_md5=opts.get("dedup_md5", True),
            dedup_phash=opts.get("dedup_phash", True),
            progress_cb=pcb, log_cb=lcb,
        )

        # 記錄這次用過的關鍵字
        for kw in keywords_used_this_run:
            db.add_used_keyword(celebrity, kw, ",".join(sources), stats.get("downloaded", 0))

        if is_batch:
            q.put({"type": "batch_one_done", "celebrity": celebrity, "stats": stats})
        else:
            q.put({"type": "done", "stats": stats})

    except Exception as e:
        q.put({"type": "error", "msg": str(e)})


def _batch_worker(task_id, q, names, opts):
    try:
        for i, name in enumerate(names):
            with tasks_lock:
                task = tasks.get(task_id)
                if task and task.get("downloader"):
                    if task["downloader"]._stop.is_set():
                        break
            q.put({"type": "log",
                    "msg": f"━━━ 批次 {i+1}/{len(names)}: {name} ━━━", "tag": "info"})
            _download_worker(task_id, q, name, opts, is_batch=True)
            if i < len(names) - 1:
                time.sleep(1)
        q.put({"type": "done", "stats": {"batch": True, "total": len(names)}})
    except Exception as e:
        q.put({"type": "error", "msg": str(e)})


# ══════════════════════════════════════════════════════════
#  影片生成 API
# ══════════════════════════════════════════════════════════
video_tasks = {}
video_tasks_lock = threading.Lock()


@app.route("/api/preview-photos", methods=["POST"])
def api_preview_photos():
    """預覽智慧挑選的照片，供使用者確認/替換（優先選未用過的）"""
    if not HAS_VIDEO:
        return jsonify({"error": "影片模組未安裝"}), 500
    from video_maker import _collect_photos
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if not celebrity:
        return jsonify({"error": "請選擇 Photo"}), 400
    celebrity = resolve_celebrity(celebrity)

    photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "找不到該 Photo 的照片目錄"}), 400

    max_photos = int(data.get("max_photos", 12))
    shuffle = data.get("shuffle", True)

    # 取比需要多的候選（2.5 倍），再用使用紀錄排序
    pool_size = max(max_photos * 3, 30)
    pool = _collect_photos(photo_dir, pool_size, shuffle=False)

    # 取使用紀錄
    usage = db.get_photo_usage_counts(celebrity)

    # 按使用次數排序（少的優先），同次數的隨機
    import random as _rand
    scored = []
    for fp in pool:
        fname = os.path.basename(fp)
        cnt = usage.get(fname, 0)
        scored.append((cnt, _rand.random() if shuffle else 0, fp))
    scored.sort(key=lambda x: (x[0], x[1]))
    selected = [s[2] for s in scored[:max_photos]]
    if shuffle:
        _rand.shuffle(selected)

    result = []
    for fp in selected:
        fname = os.path.basename(fp)
        used_cnt = usage.get(fname, 0)
        result.append({
            "filename": fname,
            "path": fp,
            "thumb_url": f"/photos/{sanitize_name(celebrity)}/{fname}",
            "used_count": used_cnt,
        })
    return jsonify({"photos": result})


@app.route("/api/replace-photo", methods=["POST"])
def api_replace_photo():
    """替換一張照片：標記被移除的 + 從剩餘照片中優先選未用過的"""
    if not HAS_VIDEO:
        return jsonify({"error": "影片模組未安裝"}), 500
    from video_maker import _collect_photos
    import random as _rand
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if celebrity:
        celebrity = resolve_celebrity(celebrity)
    exclude = set(data.get("exclude", []))
    removed = data.get("removed_filename", "")

    photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "找不到目錄"}), 400

    # 標記被移除的照片
    if removed:
        db.mark_photos_used(celebrity, [removed], "removed")

    # 取所有可用照片
    all_photos = _collect_photos(photo_dir, max_photos=9999, shuffle=False)
    available = [p for p in all_photos if os.path.basename(p) not in exclude]
    if not available:
        return jsonify({"photo": None, "msg": "沒有更多照片可替換"})

    # 優先選使用次數少的
    usage = db.get_photo_usage_counts(celebrity)
    available.sort(key=lambda p: (usage.get(os.path.basename(p), 0), _rand.random()))
    pick = available[0]
    fname = os.path.basename(pick)
    return jsonify({
        "photo": {
            "filename": fname,
            "path": pick,
            "thumb_url": f"/photos/{sanitize_name(celebrity)}/{fname}",
            "used_count": usage.get(fname, 0),
        }
    })


@app.route("/api/mark-photo-used", methods=["POST"])
def api_mark_photo_used():
    """標記照片已被下載"""
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if celebrity:
        celebrity = resolve_celebrity(celebrity)
    filenames = data.get("filenames", [])
    usage_type = data.get("usage_type", "download")
    if celebrity and filenames:
        db.mark_photos_used(celebrity, filenames, usage_type)
    return jsonify({"ok": True})


@app.route("/api/download-photos-zip", methods=["POST"])
def api_download_photos_zip():
    """打包選中的照片為 zip 下載"""
    import zipfile, io
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if celebrity:
        celebrity = resolve_celebrity(celebrity)
    filenames = data.get("filenames", [])
    if not celebrity or not filenames:
        return jsonify({"error": "參數不足"}), 400

    photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "找不到目錄"}), 400

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in filenames:
            fp = os.path.join(photo_dir, fname)
            if os.path.isfile(fp):
                zf.write(fp, fname)
    buf.seek(0)

    # 標記為已下載
    db.mark_photos_used(celebrity, filenames, "download")

    safe_name = sanitize_name(celebrity)
    return Response(
        buf.getvalue(),
        mimetype="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}_photos.zip"',
        },
    )


@app.route("/api/aliases")
def api_aliases():
    """取得所有別名"""
    return jsonify(db.get_aliases())


@app.route("/api/alias", methods=["POST"])
def api_add_alias():
    """新增別名"""
    data = request.json
    canonical = (data.get("canonical") or "").strip()
    alias = (data.get("alias") or "").strip()
    if not canonical or not alias:
        return jsonify({"error": "請輸入正式名稱和別名"}), 400
    if canonical.lower() == alias.lower():
        return jsonify({"error": "正式名稱和別名不能相同"}), 400
    ok = db.add_alias(canonical, alias)
    return jsonify({"ok": ok})


@app.route("/api/alias", methods=["DELETE"])
def api_remove_alias():
    alias = (request.json.get("alias") or "").strip()
    if alias:
        db.remove_alias(alias)
    return jsonify({"ok": True})


def _dedup_folder(photo_dir):
    """
    對資料夾內的照片做三層去重：MD5 → pHash → 刪除重複。
    回傳 (removed_md5, removed_phash, kept)
    """
    exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
    files = []
    for f in sorted(os.listdir(photo_dir)):
        fp = os.path.join(photo_dir, f)
        if os.path.isfile(fp) and os.path.splitext(f)[1].lower() in exts:
            files.append(fp)

    if not files:
        return 0, 0, 0

    # ── 第一層：MD5 去重 ──
    md5_map = {}  # md5 -> first filepath
    to_remove_md5 = []
    for fp in files:
        try:
            h = hashlib.md5(open(fp, "rb").read()).hexdigest()
        except Exception:
            continue
        if h in md5_map:
            to_remove_md5.append(fp)
        else:
            md5_map[h] = fp

    for fp in to_remove_md5:
        try:
            os.remove(fp)
        except Exception:
            pass

    # 重新掃描剩餘檔案
    remaining = [fp for fp in files if os.path.isfile(fp)]

    # ── 第二層：pHash 去重 ──
    removed_phash = 0
    if HAS_IMAGEHASH and remaining:
        phash_list = []  # [(hash, filepath), ...]
        for fp in remaining:
            try:
                with Image.open(fp) as img:
                    ph = imagehash.phash(img.convert("RGB"))
                phash_list.append((ph, fp))
            except Exception:
                pass

        to_remove_ph = set()
        for i in range(len(phash_list)):
            if phash_list[i][1] in to_remove_ph:
                continue
            for j in range(i + 1, len(phash_list)):
                if phash_list[j][1] in to_remove_ph:
                    continue
                if phash_list[i][0] - phash_list[j][0] < PHASH_THRESHOLD:
                    # 保留檔案較大的（通常品質較好）
                    size_i = os.path.getsize(phash_list[i][1])
                    size_j = os.path.getsize(phash_list[j][1])
                    victim = phash_list[j][1] if size_i >= size_j else phash_list[i][1]
                    to_remove_ph.add(victim)

        for fp in to_remove_ph:
            try:
                os.remove(fp)
                removed_phash += 1
            except Exception:
                pass

    kept = len([f for f in os.listdir(photo_dir)
                if os.path.isfile(os.path.join(photo_dir, f))
                and os.path.splitext(f)[1].lower() in exts])

    return len(to_remove_md5), removed_phash, kept


@app.route("/api/merge-celebrity", methods=["POST"])
def api_merge_celebrity():
    """
    合併兩個明星：把 source 的照片/影片/DB 全部合併到 target。
    source 資料夾會被刪除。合併後自動去重。
    """
    data = request.json
    source = (data.get("source") or "").strip()
    target = (data.get("target") or "").strip()
    if not source or not target:
        return jsonify({"error": "請指定 source 和 target"}), 400
    if source == target:
        return jsonify({"error": "來源和目標不能相同"}), 400

    src_photo = os.path.join(DOWNLOAD_ROOT, sanitize_name(source))
    dst_photo = os.path.join(DOWNLOAD_ROOT, sanitize_name(target))
    src_video = os.path.join(VIDEO_ROOT, sanitize_name(source))
    dst_video = os.path.join(VIDEO_ROOT, sanitize_name(target))

    moved_photos = 0
    moved_videos = 0

    # 合併照片資料夾
    if os.path.isdir(src_photo):
        os.makedirs(dst_photo, exist_ok=True)
        for f in os.listdir(src_photo):
            src_fp = os.path.join(src_photo, f)
            dst_fp = os.path.join(dst_photo, f)
            if os.path.isfile(src_fp):
                if os.path.exists(dst_fp):
                    base, ext = os.path.splitext(f)
                    dst_fp = os.path.join(dst_photo, f"{base}_m{ext}")
                try:
                    shutil.move(src_fp, dst_fp)
                    moved_photos += 1
                except Exception:
                    pass
        try:
            os.rmdir(src_photo)
        except Exception:
            pass

    # 合併影片資料夾
    if os.path.isdir(src_video):
        os.makedirs(dst_video, exist_ok=True)
        for f in os.listdir(src_video):
            src_fp = os.path.join(src_video, f)
            dst_fp = os.path.join(dst_video, f)
            if os.path.isfile(src_fp):
                if os.path.exists(dst_fp):
                    base, ext = os.path.splitext(f)
                    dst_fp = os.path.join(dst_video, f"{base}_m{ext}")
                try:
                    shutil.move(src_fp, dst_fp)
                    moved_videos += 1
                except Exception:
                    pass
        try:
            os.rmdir(src_video)
        except Exception:
            pass

    # 合併 DB 紀錄
    db.update_celebrity_name(source, target)
    # 自動建立別名
    db.add_alias(target, source)

    # ── 合併後自動去重 ──
    rm_md5 = 0
    rm_phash = 0
    kept = 0
    if os.path.isdir(dst_photo):
        rm_md5, rm_phash, kept = _dedup_folder(dst_photo)

    parts = [f"已將 {source} 合併到 {target}"]
    parts.append(f"搬移照片 {moved_photos}，影片 {moved_videos}")
    if rm_md5 or rm_phash:
        parts.append(f"去重刪除: MD5 {rm_md5} 張 + pHash {rm_phash} 張")
    parts.append(f"最終保留 {kept} 張照片")

    return jsonify({
        "ok": True,
        "moved_photos": moved_photos,
        "moved_videos": moved_videos,
        "removed_md5": rm_md5,
        "removed_phash": rm_phash,
        "kept": kept,
        "msg": "，".join(parts),
    })


@app.route("/api/dedup-celebrity", methods=["POST"])
def api_dedup_celebrity():
    """對指定明星的照片資料夾做去重（不需要合併也可以用）"""
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    if not celebrity:
        return jsonify({"error": "請指定 Photo"}), 400
    celebrity = resolve_celebrity(celebrity)
    photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "找不到照片目錄"}), 400

    rm_md5, rm_phash, kept = _dedup_folder(photo_dir)
    total_removed = rm_md5 + rm_phash

    if total_removed == 0:
        msg = f"{celebrity}: 沒有發現重複照片（共 {kept} 張）"
    else:
        msg = f"{celebrity}: 刪除 MD5 重複 {rm_md5} 張 + pHash 相似 {rm_phash} 張，保留 {kept} 張"

    return jsonify({
        "ok": True,
        "removed_md5": rm_md5,
        "removed_phash": rm_phash,
        "kept": kept,
        "msg": msg,
    })


@app.route("/api/video-templates")
def api_video_templates():
    return jsonify({"has_video": HAS_VIDEO, "templates": VIDEO_TEMPLATES})


@app.route("/api/generate-video", methods=["POST"])
def api_generate_video():
    if not HAS_VIDEO:
        return jsonify({"error": "影片模組未安裝"}), 500
    data = request.json
    celebrity = (data.get("celebrity") or "").strip()
    template_id = data.get("template", "")
    if not celebrity:
        return jsonify({"error": "請選擇 Photo"}), 400
    celebrity = resolve_celebrity(celebrity)
    if template_id not in VIDEO_TEMPLATES:
        return jsonify({"error": "無效的模板"}), 400

    photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "找不到該 Photo 的照片目錄"}), 400

    video_dir = os.path.join(VIDEO_ROOT, sanitize_name(celebrity))
    os.makedirs(video_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(video_dir, f"{template_id}_{ts}.mp4")

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with video_tasks_lock:
        video_tasks[task_id] = {"queue": q, "status": "running"}

    opts = {
        "max_photos": int(data.get("max_photos", 12)),
        "shuffle": data.get("shuffle", True),
        "dur_per_photo": float(data.get("dur_per_photo", 0)),
    }
    # 如果前端有傳入使用者確認過的照片清單，就用那個
    selected = data.get("selected_photos")  # list of filenames
    if selected:
        opts["selected_photos"] = [
            os.path.join(photo_dir, f) for f in selected
        ]
    # 如果有指定秒數就用，沒有就用模板預設
    if opts["dur_per_photo"] > 0:
        pass
    else:
        tpl_info = VIDEO_TEMPLATES.get(template_id, {})
        opts["dur_per_photo"] = tpl_info.get("dur_per_photo", 2.0)

    t = threading.Thread(
        target=_video_worker,
        args=(task_id, q, celebrity, template_id, photo_dir, output_path, opts),
        daemon=True,
    )
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/video-progress/<task_id>")
def api_video_progress(task_id):
    def generate():
        with video_tasks_lock:
            task = video_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type':'error','message':'任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type':'heartbeat'})}\n\n"
        with video_tasks_lock:
            video_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/videos/<celebrity>")
def api_videos(celebrity):
    video_dir = os.path.join(VIDEO_ROOT, sanitize_name(celebrity))
    if not os.path.isdir(video_dir):
        return jsonify([])
    videos = []
    for f in sorted(os.listdir(video_dir), reverse=True):
        if f.lower().endswith(".mp4"):
            fp = os.path.join(video_dir, f)
            videos.append({
                "filename": f,
                "url": f"/videos/{sanitize_name(celebrity)}/{f}",
                "size_mb": round(os.path.getsize(fp) / 1024 / 1024, 1),
                "created": datetime.fromtimestamp(
                    os.path.getctime(fp)).strftime("%Y-%m-%d %H:%M"),
            })
    return jsonify(videos)


@app.route("/videos/<path:filepath>")
def serve_video(filepath):
    parts = filepath.replace("\\", "/").split("/", 1)
    if len(parts) != 2:
        return "Not found", 404
    video_dir = os.path.join(VIDEO_ROOT, parts[0])
    if not os.path.isdir(video_dir):
        return "Not found", 404
    return send_from_directory(video_dir, parts[1])


def _video_worker(task_id, q, celebrity, template_id, photo_dir, output_path, options):
    try:
        q.put({"type": "progress", "percent": 0, "message": "開始生成影片..."})

        def progress_cb(percent, message):
            q.put({"type": "progress", "percent": percent, "message": message})

        result = _gen_video(
            celebrity=celebrity,
            template_id=template_id,
            photo_dir=photo_dir,
            output_path=output_path,
            options=options,
            progress_cb=progress_cb,
        )

        if result["success"]:
            # 標記這些照片已被用於影片
            sel = options.get("selected_photos", [])
            if sel:
                fnames = [os.path.basename(p) for p in sel]
                try:
                    db.mark_photos_used(celebrity, fnames, "video")
                except Exception:
                    pass
            celeb_safe = sanitize_name(celebrity)
            filename = os.path.basename(output_path)
            q.put({
                "type": "done",
                "video_url": f"/videos/{celeb_safe}/{filename}",
                "duration": result.get("duration"),
                "file_size_mb": round(result.get("file_size", 0) / 1024 / 1024, 1),
            })
        else:
            q.put({"type": "error", "message": result.get("error", "未知錯誤")})
    except Exception as e:
        q.put({"type": "error", "message": str(e)})


# ══════════════════════════════════════════════════════════
#  YouTube 下載
# ══════════════════════════════════════════════════════════
yt_tasks = {}
yt_tasks_lock = threading.Lock()


_tiktok_chrome_lock = threading.Lock()

def _tiktok_search(keyword, max_results=10):
    """用 DrissionPage 連接持久 Chrome 抓取 TikTok 搜尋結果"""
    import re as _re
    try:
        from DrissionPage import ChromiumPage, ChromiumOptions
    except ImportError:
        logging.warning("TikTok search: DrissionPage not installed")
        return []
    results = []
    if not _tiktok_chrome_lock.acquire(timeout=30):
        logging.warning("TikTok search: another search in progress")
        return []
    try:
        import time as _time
        from urllib.parse import quote
        co = ChromiumOptions()
        co.set_local_port(CHROME_DEBUG_PORT)
        logging.info(f"TikTok search: connecting to Chrome port {CHROME_DEBUG_PORT} for '{keyword}'")
        page = ChromiumPage(co)
        # 導航到 TikTok 搜尋
        search_url = f"https://www.tiktok.com/search/video?q={quote(keyword)}"
        page.get(search_url)
        _time.sleep(7)
        # 取得影片描述 / 觀看數元素
        desc_els = page.eles('css:[data-e2e="search-card-desc"]')
        view_els = page.eles('css:[data-e2e="video-views"]')
        # 取得所有影片連結
        html = page.html
        video_links = _re.findall(
            r'href="(https://www\.tiktok\.com/@([\w.]+)/video/(\d+))"', html
        )
        # 去重
        seen = set()
        unique = []
        for full_url, author, vid_id in video_links:
            if vid_id not in seen:
                seen.add(vid_id)
                unique.append({"url": full_url, "author": author, "id": vid_id})
        # 取得縮圖（排除頭像）
        thumb_els = page.eles('css:img[src*="tiktokcdn"]')
        thumbs = [img.attr('src') for img in thumb_els
                  if '/avt-' not in (img.attr('src') or '')]
        for i, v in enumerate(unique[:max_results]):
            desc_text = ""
            if i < len(desc_els):
                raw = desc_els[i].text or ""
                desc_text = raw.split('\n')[0].strip()
            view_text = ""
            if i < len(view_els):
                view_text = (view_els[i].text or "").strip()
            view_count = 0
            if view_text:
                try:
                    vt = view_text.upper().replace(",", "")
                    if "M" in vt:
                        view_count = int(float(vt.replace("M", "")) * 1_000_000)
                    elif "K" in vt:
                        view_count = int(float(vt.replace("K", "")) * 1_000)
                    else:
                        view_count = int(vt)
                except Exception:
                    pass
            thumb = thumbs[i] if i < len(thumbs) else ""
            results.append({
                "id": v["id"],
                "title": desc_text,
                "url": v["url"],
                "duration": None,
                "channel": v["author"],
                "view_count": view_count,
                "thumbnail": thumb,
                "platform": "tiktok",
            })
        logging.info(f"TikTok search: found {len(results)} results for '{keyword}'")
        # 注意：不要 quit Chrome，保持持久連線
    except Exception as e:
        import traceback
        logging.warning(f"TikTok search error: {type(e).__name__}: {e}")
        logging.warning(traceback.format_exc())
    finally:
        _tiktok_chrome_lock.release()
    return results


@app.route("/api/yt/search", methods=["POST"])
def api_yt_search():
    if not HAS_YTDLP:
        return jsonify({"error": "yt-dlp 未安裝"}), 500
    data = request.json
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"error": "請輸入搜尋關鍵字"}), 400
    max_results = int(data.get("max_results", 10))
    video_type = (data.get("video_type") or "all").strip()
    platform = (data.get("platform") or "youtube").strip().lower()

    # 影片類型關鍵字對照（YouTube 專用）
    VIDEO_TYPE_KEYWORDS = {
        "stage": "stage performance live",
        "fancam": "fancam 직캠",
        "music_show": "Music Core OR Music Bank OR Inkigayo OR M Countdown",
        "mv": "MV official",
    }

    # 判斷是否為 URL
    is_url = query.startswith("http://") or query.startswith("https://")
    # 自動偵測平台
    if is_url:
        if "tiktok.com" in query:
            platform = "tiktok"
        elif "youtube.com" in query or "youtu.be" in query:
            platform = "youtube"

    try:
        tiktok_hint = ""
        ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
        if is_url:
            ydl_opts["noplaylist"] = True

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            if is_url:
                info = ydl.extract_info(query, download=False)
                entries = [info] if info.get("_type") != "playlist" else (info.get("entries") or [])
            elif platform == "tiktok":
                # TikTok 搜尋策略
                if query.startswith("@"):
                    # 使用者頁面 — 透過 yt-dlp
                    tiktok_url = f"https://www.tiktok.com/{query}"
                    info = ydl.extract_info(tiktok_url, download=False)
                    entries = (info.get("entries") or [])[:max_results]
                else:
                    # 關鍵字搜尋 — 使用 DrissionPage 抓 TikTok 搜尋頁
                    entries = []
                    tiktok_results = _tiktok_search(query, max_results)
                    if tiktok_results:
                        # 直接回傳 TikTok 搜尋結果，跳過 yt-dlp 解析
                        resp = {"results": tiktok_results}
                        return jsonify(resp)
                    else:
                        tiktok_hint = "TikTok 搜尋暫時無法取得結果，請貼上影片網址或輸入 @用戶名"
            else:
                # YouTube 搜尋
                search_query = query
                if video_type in VIDEO_TYPE_KEYWORDS:
                    search_query = f"{query} {VIDEO_TYPE_KEYWORDS[video_type]}"
                info = ydl.extract_info(f"ytsearch{max_results}:{search_query}", download=False)
                entries = info.get("entries", [])

        results = []
        for e in entries[:max_results]:
            if not e:
                continue
            vid_id = e.get("id", "")
            vid_url = e.get("url") or e.get("webpage_url") or ""
            if not vid_url and vid_id:
                if platform == "tiktok":
                    vid_url = f"https://www.tiktok.com/video/{vid_id}"
                else:
                    vid_url = f"https://www.youtube.com/watch?v={vid_id}"
            results.append({
                "id": vid_id,
                "title": e.get("title", ""),
                "url": vid_url,
                "duration": e.get("duration"),
                "channel": e.get("channel") or e.get("uploader") or e.get("creator", ""),
                "view_count": e.get("view_count"),
                "thumbnail": e.get("thumbnail") or (f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg" if platform == "youtube" else ""),
                "platform": platform,
            })
        resp = {"results": results}
        # 加入 TikTok 搜尋提示
        if tiktok_hint:
            resp["hint"] = tiktok_hint
        elif platform == "tiktok" and not is_url and not results:
            resp["hint"] = "TikTok 搜尋建議：貼上影片網址、或輸入 @用戶名 瀏覽其影片"
        return jsonify(resp)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/yt/download", methods=["POST"])
def api_yt_download():
    if not HAS_YTDLP:
        return jsonify({"error": "yt-dlp 未安裝"}), 500
    data = request.json
    url = (data.get("url") or "").strip()
    title = (data.get("title") or "video").strip()
    quality = data.get("quality", "bestvideo[height<=720]+bestaudio/best")
    keyword = (data.get("keyword") or "").strip()
    audio_only = "bestaudio" in quality and "bestvideo" not in quality

    if not url:
        return jsonify({"error": "請提供影片網址"}), 400

    # 依搜尋關鍵字歸檔到對應人名子資料夾
    if keyword:
        person_folder = sanitize_name(resolve_celebrity(keyword))
    else:
        person_folder = "_unsorted"
    dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
    os.makedirs(dl_dir, exist_ok=True)

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with yt_tasks_lock:
        yt_tasks[task_id] = {"queue": q, "status": "running"}

    def worker():
        try:
            q.put({"type": "progress", "percent": 0, "message": f"開始下載: {title}"})

            def progress_hook(d):
                if d["status"] == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate")
                    downloaded = d.get("downloaded_bytes", 0)
                    pct = int(downloaded / total * 100) if total else 0
                    speed = d.get("speed")
                    speed_str = f"{speed / 1024 / 1024:.1f} MB/s" if speed else ""
                    eta = d.get("eta")
                    eta_str = f"剩餘 {eta}秒" if eta else ""
                    q.put({
                        "type": "progress",
                        "percent": min(pct, 95),
                        "message": f"下載中 {pct}% {speed_str} {eta_str}".strip(),
                    })
                elif d["status"] == "finished":
                    q.put({"type": "progress", "percent": 96, "message": "合併/轉檔中..."})

            out_tpl = os.path.join(dl_dir, "%(title).80s [%(id)s].%(ext)s")
            ydl_opts = {
                "format": quality,
                "outtmpl": out_tpl,
                "progress_hooks": [progress_hook],
                "merge_output_format": "mp4" if not audio_only else None,
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
            }
            if FFMPEG_LOCATION:
                ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION
            if audio_only:
                ydl_opts["postprocessors"] = [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }]
                ydl_opts.pop("merge_output_format", None)

            info = None
            # 嘗試下載，如果分流格式失敗則降級到單一格式
            for attempt, fmt in enumerate([quality, "best"]):
                if attempt > 0 and (audio_only or fmt == quality):
                    break
                try:
                    ydl_opts_try = dict(ydl_opts, format=fmt)
                    with yt_dlp.YoutubeDL(ydl_opts_try) as ydl:
                        info = ydl.extract_info(url, download=True)
                        info = ydl.sanitize_info(info)
                    # 檢查是否真的下載了完整檔案（非 .part）
                    vid_id_check = info.get("id", "")
                    found_complete = False
                    for fname in os.listdir(dl_dir):
                        fp_chk = os.path.join(dl_dir, fname)
                        if os.path.isfile(fp_chk) and vid_id_check in fname and not fname.endswith(".part"):
                            if os.path.getsize(fp_chk) > 10240:  # > 10KB
                                found_complete = True
                                break
                    if found_complete:
                        if attempt > 0:
                            q.put({"type": "progress", "percent": 95,
                                   "message": "已降級為單一格式下載"})
                        break
                    elif attempt == 0:
                        q.put({"type": "progress", "percent": 5,
                               "message": "分流下載失敗，嘗試單一格式..."})
                        # 清除失敗的 .part 檔案
                        for fname in list(os.listdir(dl_dir)):
                            if fname.endswith(".part") and vid_id_check in fname:
                                try:
                                    os.remove(os.path.join(dl_dir, fname))
                                except OSError:
                                    pass
                except Exception as e_dl:
                    if attempt == 0:
                        q.put({"type": "progress", "percent": 5,
                               "message": f"格式 {fmt} 失敗，嘗試降級..."})
                    else:
                        raise e_dl

            if info is None:
                q.put({"type": "error", "message": "所有格式都下載失敗"})
                return

            # 找到實際輸出檔案（排除 .part）
            ext = "mp3" if audio_only else "mp4"
            actual_file = None
            vid_id = info.get("id", "")
            for f in sorted(os.listdir(dl_dir), key=lambda x: os.path.getmtime(os.path.join(dl_dir, x)), reverse=True):
                fp_c = os.path.join(dl_dir, f)
                if os.path.isfile(fp_c) and vid_id in f and not f.endswith(".part"):
                    if os.path.getsize(fp_c) > 10240:
                        actual_file = f
                        break
            if not actual_file:
                files = [x for x in sorted(os.listdir(dl_dir),
                         key=lambda x: os.path.getmtime(os.path.join(dl_dir, x)), reverse=True)
                         if os.path.isfile(os.path.join(dl_dir, x))
                         and not x.endswith(".part")
                         and os.path.getsize(os.path.join(dl_dir, x)) > 10240]
                if files:
                    actual_file = files[0]

            if actual_file:
                fp = os.path.join(dl_dir, actual_file)
                rel_path = f"downloads/{person_folder}/{actual_file}"
                file_size = os.path.getsize(fp)
                q.put({
                    "type": "done",
                    "filename": actual_file,
                    "rel_path": rel_path,
                    "file_url": f"/yt-files/{rel_path}",
                    "file_size_mb": round(file_size / 1024 / 1024, 1),
                    "title": info.get("title", ""),
                    "duration": info.get("duration"),
                    "person": person_folder,
                })
            else:
                # 清除殘留的 .part 檔案
                for fname in list(os.listdir(dl_dir)):
                    if fname.endswith(".part") and vid_id in fname:
                        try:
                            os.remove(os.path.join(dl_dir, fname))
                        except OSError:
                            pass
                q.put({"type": "error", "message": "下載完成但找不到完整檔案"})

        except Exception as e:
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/yt/progress/<task_id>")
def api_yt_progress(task_id):
    def generate():
        with yt_tasks_lock:
            task = yt_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type': 'error', 'message': '任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        with yt_tasks_lock:
            yt_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/yt/downloads")
def api_yt_downloads():
    """列出已下載的 YT 影片（遞迴掃描 downloads/ 和 extracts/）"""
    exts = {".mp4", ".mkv", ".webm", ".mp3", ".m4a", ".opus"}
    files = []
    for category in ("downloads", "extracts"):
        cat_dir = os.path.join(YT_ROOT, category)
        if not os.path.isdir(cat_dir):
            continue
        for person in sorted(os.listdir(cat_dir)):
            person_dir = os.path.join(cat_dir, person)
            if not os.path.isdir(person_dir):
                continue
            for f in os.listdir(person_dir):
                fp = os.path.join(person_dir, f)
                if not os.path.isfile(fp):
                    continue
                if os.path.splitext(f)[1].lower() not in exts:
                    continue
                rel_path = f"{category}/{person}/{f}"
                size = os.path.getsize(fp)
                files.append({
                    "filename": f,
                    "rel_path": rel_path,
                    "url": f"/yt-files/{rel_path}",
                    "size_mb": round(size / 1024 / 1024, 1),
                    "ext": os.path.splitext(f)[1].lower(),
                    "created": datetime.fromtimestamp(os.path.getmtime(fp)).strftime("%Y-%m-%d %H:%M"),
                    "category": category,
                    "person": person,
                })
    files.sort(key=lambda x: x["created"], reverse=True)
    return jsonify(files)


@app.route("/yt-files/<path:filename>")
def serve_yt_file(filename):
    safe = os.path.normpath(os.path.join(YT_ROOT, filename))
    if not safe.startswith(os.path.normpath(YT_ROOT)):
        return "Forbidden", 403
    return send_from_directory(YT_ROOT, filename)


# ══════════════════════════════════════════════════════════
#  人臉辨識影片擷取
# ══════════════════════════════════════════════════════════
MODELS_DIR = os.path.join(APP_DIR, "models")
YUNET_MODEL = os.path.join(MODELS_DIR, "yunet.onnx")
SFACE_MODEL = os.path.join(MODELS_DIR, "sface.onnx")
HAS_FACE_MODELS = os.path.isfile(YUNET_MODEL) and os.path.isfile(SFACE_MODEL)

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

EXTRACT_CONFIG = {
    "sample_interval": 1.0,       # 每幾秒取一幀
    "similarity_threshold": 0.35, # SFace cosine similarity 門檻 (SFace 建議 0.363)
    "merge_gap": 3.0,             # 時間段合併間隔（秒）
    "padding": 0.5,               # 每段前後緩衝（秒）
    "max_ref_photos": 80,         # 最多用幾張參考照
}

extract_tasks = {}
extract_tasks_lock = threading.Lock()


def _build_face_models(width=320, height=320):
    """建立人臉偵測器和辨識器"""
    detector = cv2.FaceDetectorYN.create(YUNET_MODEL, "", (width, height), 0.7, 0.3, 5000)
    recognizer = cv2.FaceRecognizerSF.create(SFACE_MODEL, "")
    return detector, recognizer


def _build_reference_embeddings(person_name, progress_cb=None):
    """從已下載照片建立人臉特徵向量"""
    photo_dir = os.path.join(DOWNLOAD_ROOT, person_name)
    if not os.path.isdir(photo_dir):
        return None

    # 檢查快取
    cache_path = os.path.join(photo_dir, f".face_embeddings.npy")
    if os.path.isfile(cache_path):
        return np.load(cache_path)

    import glob as _glob
    import random as _random
    exts = ["*.jpg", "*.jpeg", "*.png", "*.webp"]
    files = []
    for ext in exts:
        files.extend(_glob.glob(os.path.join(photo_dir, ext)))
    if not files:
        return None

    max_photos = EXTRACT_CONFIG["max_ref_photos"]
    if len(files) > max_photos:
        _random.shuffle(files)
        files = files[:max_photos]

    detector, recognizer = _build_face_models()
    embeddings = []

    for i, fpath in enumerate(files):
        try:
            img = cv2.imread(fpath)
            if img is None:
                continue
            h, w = img.shape[:2]
            detector.setInputSize((w, h))
            _, faces = detector.detect(img)
            if faces is None or len(faces) == 0:
                continue
            # 取最大的臉（假設主角最大）
            areas = [f[2] * f[3] for f in faces]
            best = faces[int(np.argmax(areas))]
            aligned = recognizer.alignCrop(img, best)
            emb = recognizer.feature(aligned)
            embeddings.append(emb.flatten())
        except Exception:
            continue
        if progress_cb and i % 10 == 0:
            progress_cb(i / len(files))

    if len(embeddings) < 3:
        return None

    mat = np.stack(embeddings)
    # 正規化
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1
    mat = mat / norms
    np.save(cache_path, mat)
    return mat


def _scan_video_for_person(video_path, ref_embeddings, progress_cb=None):
    """掃描影片，回傳出現該人的時間戳列表"""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    duration = total_frames / fps if fps > 0 else 0

    interval = EXTRACT_CONFIG["sample_interval"]
    threshold = EXTRACT_CONFIG["similarity_threshold"]

    detector, recognizer = _build_face_models()
    timestamps = []

    t = 0.0
    while t < duration:
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
        ret, frame = cap.read()
        if not ret:
            t += interval
            continue

        h, w = frame.shape[:2]
        detector.setInputSize((w, h))
        _, faces = detector.detect(frame)

        if faces is not None:
            for face in faces:
                try:
                    aligned = recognizer.alignCrop(frame, face)
                    emb = recognizer.feature(aligned).flatten()
                    emb_norm = np.linalg.norm(emb)
                    if emb_norm > 0:
                        emb = emb / emb_norm
                    # cosine similarity vs all references
                    sims = ref_embeddings @ emb
                    if np.max(sims) >= threshold:
                        timestamps.append(t)
                        break
                except Exception:
                    continue

        t += interval
        if progress_cb:
            progress_cb(t / duration if duration > 0 else 1)

    cap.release()
    return timestamps


def _merge_segments(timestamps):
    """將時間戳合併成連續片段"""
    if not timestamps:
        return []
    interval = EXTRACT_CONFIG["sample_interval"]
    gap = EXTRACT_CONFIG["merge_gap"]
    pad = EXTRACT_CONFIG["padding"]

    timestamps.sort()
    segments = []
    start = timestamps[0]
    end = timestamps[0]

    for t in timestamps[1:]:
        if t - end <= gap + interval:
            end = t
        else:
            segments.append((max(0, start - pad), end + interval + pad))
            start = t
            end = t
    segments.append((max(0, start - pad), end + interval + pad))
    return segments


def _extract_segments(video_path, segments, output_path):
    """用 FFmpeg 剪輯並合併片段"""
    ffmpeg_exe = _get_ffmpeg()
    import tempfile, subprocess
    temp_dir = tempfile.mkdtemp()
    try:
        seg_files = []
        for i, (start, end) in enumerate(segments):
            seg_path = os.path.join(temp_dir, f"seg_{i:04d}.mp4")
            cmd = [
                ffmpeg_exe, "-y",
                "-ss", f"{start:.2f}",
                "-i", video_path,
                "-t", f"{end - start:.2f}",
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                seg_path
            ]
            subprocess.run(cmd, capture_output=True, timeout=120)
            if os.path.isfile(seg_path) and os.path.getsize(seg_path) > 0:
                seg_files.append(seg_path)

        if not seg_files:
            return None

        if len(seg_files) == 1:
            shutil.copy2(seg_files[0], output_path)
        else:
            list_path = os.path.join(temp_dir, "list.txt")
            with open(list_path, "w") as f:
                for sp in seg_files:
                    f.write(f"file '{sp.replace(os.sep, '/')}'\n")
            cmd = [
                ffmpeg_exe, "-y",
                "-f", "concat", "-safe", "0",
                "-i", list_path,
                "-c", "copy",
                output_path
            ]
            subprocess.run(cmd, capture_output=True, timeout=120)

        return output_path if os.path.isfile(output_path) else None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@app.route("/api/yt/extract", methods=["POST"])
def api_yt_extract():
    """啟動人臉辨識影片擷取"""
    if not HAS_FACE_MODELS or not HAS_CV2:
        return jsonify({"error": "人臉模型或 OpenCV 未安裝"}), 500

    data = request.json
    filename = (data.get("filename") or "").strip()
    person = (data.get("person") or "").strip()
    if not filename or not person:
        return jsonify({"error": "需要 filename 和 person"}), 400

    # filename 現在是相對路徑如 downloads/ahyeon/video.mp4
    video_path = os.path.normpath(os.path.join(YT_ROOT, filename))
    if not video_path.startswith(os.path.normpath(YT_ROOT)):
        return jsonify({"error": "無效的檔案路徑"}), 400
    if not os.path.isfile(video_path):
        return jsonify({"error": f"找不到影片: {filename}"}), 404

    original_person = person
    person = resolve_celebrity(person)
    photo_dir = os.path.join(DOWNLOAD_ROOT, person)
    if not os.path.isdir(photo_dir):
        # 回傳可用的照片資料夾讓前端選擇
        available = sorted([
            d for d in os.listdir(DOWNLOAD_ROOT)
            if os.path.isdir(os.path.join(DOWNLOAD_ROOT, d))
               and not d.startswith(".")
        ])
        return jsonify({
            "need_mapping": True,
            "keyword": original_person,
            "resolved": person,
            "available_folders": available,
            "message": f"找不到「{original_person}」的照片資料夾，請選擇要用哪個人物的照片進行辨識"
        }), 200

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with extract_tasks_lock:
        extract_tasks[task_id] = {"queue": q}

    def worker():
        try:
            # Phase 1: 建立參考特徵
            q.put({"type": "progress", "phase": "embeddings", "percent": 0,
                   "message": f"建立 {person} 的臉部特徵..."})

            def emb_cb(pct):
                q.put({"type": "progress", "phase": "embeddings",
                       "percent": int(pct * 15),
                       "message": f"建立臉部特徵 {int(pct*100)}%"})

            ref = _build_reference_embeddings(person, emb_cb)
            if ref is None or len(ref) < 3:
                q.put({"type": "error", "message": f"{person} 的照片中找不到足夠的臉部特徵（至少需要 3 張）"})
                return

            q.put({"type": "progress", "phase": "embeddings", "percent": 15,
                   "message": f"完成！使用 {len(ref)} 張臉部特徵"})

            # Phase 2: 掃描影片
            q.put({"type": "progress", "phase": "scanning", "percent": 15,
                   "message": "掃描影片中的人臉..."})

            def scan_cb(pct):
                p = 15 + int(pct * 65)
                q.put({"type": "progress", "phase": "scanning",
                       "percent": min(p, 80),
                       "message": f"掃描影片 {int(pct*100)}%"})

            timestamps = _scan_video_for_person(video_path, ref, scan_cb)

            if not timestamps:
                q.put({"type": "error", "message": f"影片中未偵測到 {person}"})
                return

            # Phase 3: 合併片段
            segments = _merge_segments(timestamps)
            total_dur = sum(e - s for s, e in segments)
            q.put({"type": "progress", "phase": "cutting", "percent": 82,
                   "message": f"找到 {len(segments)} 個片段（共 {total_dur:.1f} 秒），剪輯中..."})

            # Phase 4: FFmpeg 剪輯
            base = os.path.splitext(os.path.basename(filename))[0]
            out_name = f"{base}_{person}.mp4"
            extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
            os.makedirs(extract_dir, exist_ok=True)
            out_path = os.path.join(extract_dir, out_name)
            result = _extract_segments(video_path, segments, out_path)

            if result and os.path.isfile(result):
                rel_path = f"extracts/{sanitize_name(person)}/{out_name}"
                fsize = os.path.getsize(result)
                q.put({
                    "type": "done",
                    "filename": out_name,
                    "rel_path": rel_path,
                    "file_url": f"/yt-files/{rel_path}",
                    "file_size_mb": round(fsize / 1024 / 1024, 1),
                    "segments": len(segments),
                    "duration": round(total_dur, 1),
                })
            else:
                q.put({"type": "error", "message": "FFmpeg 剪輯失敗"})

        except Exception as e:
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/yt/extract-progress/<task_id>")
def api_yt_extract_progress(task_id):
    """SSE 進度"""
    def generate():
        with extract_tasks_lock:
            task = extract_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type': 'error', 'message': '任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        with extract_tasks_lock:
            extract_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ══════════════════════════════════════════════════════════
#  精華影片生成
# ══════════════════════════════════════════════════════════
HIGHLIGHT_STRATEGIES = {
    "closeup":  {"face_weight": 1.0, "motion_weight": 0.0, "label": "特寫優先"},
    "dynamic":  {"face_weight": 0.0, "motion_weight": 1.0, "label": "動態優先"},
    "balanced": {"face_weight": 0.6, "motion_weight": 0.4, "label": "均衡"},
    "random":   {"face_weight": 0.0, "motion_weight": 0.0, "label": "隨機"},
}

highlight_tasks = {}
highlight_tasks_lock = threading.Lock()


def _get_ffmpeg():
    """取得 FFmpeg 執行檔路徑"""
    if FFMPEG_LOCATION:
        return os.path.join(FFMPEG_LOCATION, "ffmpeg.exe")
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        return "ffmpeg"


def _score_video_highlights(video_path, strategy, progress_cb=None):
    """對影片每秒評分，找出精彩時刻"""
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    duration = total_frames / fps if fps > 0 else 0
    if duration < 2:
        cap.release()
        return []

    weights = HIGHLIGHT_STRATEGIES.get(strategy, HIGHLIGHT_STRATEGIES["balanced"])
    face_w = weights["face_weight"]
    motion_w = weights["motion_weight"]
    is_random = (strategy == "random")

    detector = None
    if face_w > 0 and HAS_CV2:
        detector, _ = _build_face_models()

    scores = []
    prev_gray = None
    t = 0.0

    while t < duration:
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
        ret, frame = cap.read()
        if not ret:
            t += 1.0
            continue

        h, w = frame.shape[:2]
        frame_area = h * w

        # 臉部評分：臉部面積佔畫面比例
        face_score = 0.0
        if detector and face_w > 0:
            detector.setInputSize((w, h))
            _, faces = detector.detect(frame)
            if faces is not None and len(faces) > 0:
                areas = [f[2] * f[3] for f in faces]
                face_score = min(max(areas) / frame_area * 10, 1.0)

        # 動態評分：幀間差異
        motion_score = 0.0
        if motion_w > 0:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            if prev_gray is not None:
                diff = cv2.absdiff(gray, prev_gray)
                motion_score = min(diff.mean() / 30.0, 1.0)
            prev_gray = gray

        if is_random:
            import random as _rng
            combined = _rng.random()
        else:
            combined = face_w * face_score + motion_w * motion_score

        scores.append({"time": t, "score": combined})
        t += 1.0
        if progress_cb:
            progress_cb(t / duration)

    cap.release()
    return scores


def _select_highlight_clips(all_scores, clip_duration, total_duration, max_per_video):
    """從所有影片中挑選最佳片段"""
    candidates = []
    for video_path, scores in all_scores:
        for s in scores:
            candidates.append({
                "video": video_path,
                "time": s["time"],
                "score": s["score"],
            })

    if not candidates:
        return []

    candidates.sort(key=lambda x: x["score"], reverse=True)

    selected = []
    video_counts = {}
    total = 0.0

    for c in candidates:
        if total >= total_duration:
            break

        v = c["video"]
        if max_per_video > 0 and video_counts.get(v, 0) >= max_per_video:
            continue

        # 避免同一部影片的片段重疊
        overlap = False
        for s in selected:
            if s["video"] == v and abs(s["time"] - c["time"]) < clip_duration + 1:
                overlap = True
                break
        if overlap:
            continue

        selected.append(c)
        video_counts[v] = video_counts.get(v, 0) + 1
        total += clip_duration

    # 按影片和時間排序，讓播放更流暢
    selected.sort(key=lambda x: (x["video"], x["time"]))
    return selected


def _compile_highlight(clips, clip_duration, output_path, transition,
                       transition_dur, resolution, progress_cb=None,
                       audio_mode="original"):
    """用 FFmpeg 把片段合成精華影片"""
    ffmpeg = _get_ffmpeg()
    import tempfile, subprocess

    res_map = {"720p": (1280, 720), "1080p": (1920, 1080),
                "720p_v": (720, 1280), "1080p_v": (1080, 1920)}
    tw, th = res_map.get(resolution, (1280, 720))

    temp_dir = tempfile.mkdtemp()
    try:
        # Phase 1: 擷取每個片段並統一解析度
        seg_files = []
        for i, clip in enumerate(clips):
            seg_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            start = max(0, clip["time"] - 0.5)
            cmd = [
                ffmpeg, "-y",
                "-ss", f"{start:.2f}",
                "-i", clip["video"],
                "-t", f"{clip_duration + 0.5:.2f}",
                "-vf", f"scale={tw}:{th}:force_original_aspect_ratio=decrease,"
                       f"pad={tw}:{th}:(ow-iw)/2:(oh-ih)/2:black",
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            ]
            if audio_mode == "mute":
                cmd += ["-an"]
            else:
                cmd += ["-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2"]
            cmd += [
                "-r", "30",
                "-shortest",
                seg_path,
            ]
            subprocess.run(cmd, capture_output=True, timeout=120)
            if os.path.isfile(seg_path) and os.path.getsize(seg_path) > 0:
                seg_files.append(seg_path)
            if progress_cb:
                progress_cb(i / len(clips))

        if not seg_files:
            return None

        # Phase 2: 合成
        has_audio = (audio_mode != "mute")
        if transition == "crossfade" and len(seg_files) > 1 and transition_dur > 0:
            # xfade 鏈式過場
            result = _xfade_concat(ffmpeg, seg_files, clip_duration,
                                   transition_dur, output_path, temp_dir,
                                   has_audio=has_audio)
        else:
            # 直接 concat
            list_path = os.path.join(temp_dir, "list.txt")
            with open(list_path, "w") as f:
                for sp in seg_files:
                    f.write(f"file '{sp.replace(os.sep, '/')}'\n")
            cmd = [
                ffmpeg, "-y",
                "-f", "concat", "-safe", "0",
                "-i", list_path,
                "-c", "copy",
                output_path,
            ]
            subprocess.run(cmd, capture_output=True, timeout=120)
            result = output_path

        return result if os.path.isfile(output_path) else None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _xfade_concat(ffmpeg, seg_files, clip_dur, xfade_dur, output_path, temp_dir,
                   has_audio=True):
    """用 xfade filter 鏈式合成片段"""
    import subprocess
    n = len(seg_files)
    if n == 1:
        shutil.copy2(seg_files[0], output_path)
        return output_path

    inputs = []
    for f in seg_files:
        inputs.extend(["-i", f])

    # 建構 xfade filter chain
    filters = []
    offset = clip_dur - xfade_dur
    prev = "[0]"
    for i in range(1, n):
        curr = f"[{i}]"
        out = f"[v{i}]" if i < n - 1 else "[vout]"
        filters.append(
            f"{prev}{curr}xfade=transition=fade:duration={xfade_dur:.2f}"
            f":offset={offset:.2f}{out}"
        )
        offset += clip_dur - xfade_dur
        prev = out

    filter_complex = ";".join(filters)

    if has_audio:
        # 音訊 concat
        audio_inputs = "".join(f"[{i}:a]" for i in range(n))
        audio_filter = f"{audio_inputs}concat=n={n}:v=0:a=1[aout]"
        filter_complex += ";" + audio_filter

    cmd = [
        ffmpeg, "-y",
    ] + inputs + [
        "-filter_complex", filter_complex,
        "-map", "[vout]",
    ]
    if has_audio:
        cmd += ["-map", "[aout]", "-c:a", "aac", "-b:a", "128k"]
    cmd += [
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        output_path,
    ]
    subprocess.run(cmd, capture_output=True, timeout=300)
    return output_path


@app.route("/api/yt/highlight", methods=["POST"])
def api_yt_highlight():
    """啟動精華影片生成"""
    if not HAS_CV2:
        return jsonify({"error": "OpenCV 未安裝"}), 500

    data = request.json
    person = (data.get("person") or "").strip()
    if not person:
        return jsonify({"error": "請選擇人物"}), 400

    person = resolve_celebrity(person)
    person_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    if not os.path.isdir(person_dir):
        return jsonify({"error": f"找不到 {person} 的擷取影片"}), 400

    # 收集影片（支援指定特定影片）
    exts = {".mp4", ".mkv", ".webm"}
    selected_files = data.get("videos")  # 前端傳入的已選影片清單
    if selected_files and isinstance(selected_files, list):
        videos = []
        for f in selected_files:
            f = os.path.basename(f)  # 安全：只取檔名
            fp = os.path.join(person_dir, f)
            if os.path.isfile(fp) and os.path.splitext(f)[1].lower() in exts:
                videos.append(fp)
    else:
        videos = [os.path.join(person_dir, f) for f in os.listdir(person_dir)
                  if os.path.splitext(f)[1].lower() in exts
                  and not f.startswith("highlight_")]
    if not videos:
        return jsonify({"error": "沒有可用的擷取影片"}), 400

    # 讀取選項
    audio_mode = data.get("audio_mode", "original")  # original / mute
    strategy = data.get("strategy", "balanced")
    clip_duration = float(data.get("clip_duration", 3))
    total_duration = float(data.get("total_duration", 30))
    max_per_video = int(data.get("max_per_video", 5))
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    resolution = data.get("resolution", "720p")

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with highlight_tasks_lock:
        highlight_tasks[task_id] = {"queue": q}

    def worker():
        try:
            # Phase 1: 掃描所有影片評分
            all_scores = []
            for vi, vpath in enumerate(videos):
                vname = os.path.basename(vpath)
                q.put({"type": "progress", "percent": int(vi / len(videos) * 60),
                       "message": f"分析影片 {vi+1}/{len(videos)}: {vname[:40]}..."})

                def scan_cb(pct):
                    p = int((vi + pct) / len(videos) * 60)
                    q.put({"type": "progress", "percent": min(p, 59),
                           "message": f"分析 {vname[:30]}... {int(pct*100)}%"})

                scores = _score_video_highlights(vpath, strategy, scan_cb)
                if scores:
                    all_scores.append((vpath, scores))

            if not all_scores:
                q.put({"type": "error", "message": "所有影片都無法分析"})
                return

            # Phase 2: 挑選片段
            q.put({"type": "progress", "percent": 62, "message": "挑選精彩片段..."})
            clips = _select_highlight_clips(all_scores, clip_duration,
                                            total_duration, max_per_video)
            if not clips:
                q.put({"type": "error", "message": "找不到符合條件的片段"})
                return

            actual_dur = len(clips) * clip_duration
            q.put({"type": "progress", "percent": 65,
                   "message": f"選出 {len(clips)} 個片段（預計 {actual_dur:.0f} 秒），合成中..."})

            # Phase 3: FFmpeg 合成
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_name = f"highlight_{sanitize_name(person)}_{ts}.mp4"
            out_path = os.path.join(person_dir, out_name)

            def compile_cb(pct):
                p = 65 + int(pct * 30)
                q.put({"type": "progress", "percent": min(p, 95),
                       "message": f"合成影片 {int(pct*100)}%"})

            result = _compile_highlight(
                clips, clip_duration, out_path, transition,
                transition_dur, resolution, compile_cb,
                audio_mode=audio_mode,
            )

            if result and os.path.isfile(result):
                rel_path = f"extracts/{sanitize_name(person)}/{out_name}"
                fsize = os.path.getsize(result)
                q.put({
                    "type": "done",
                    "filename": out_name,
                    "rel_path": rel_path,
                    "file_url": f"/yt-files/{rel_path}",
                    "file_size_mb": round(fsize / 1024 / 1024, 1),
                    "clips_count": len(clips),
                    "duration": round(actual_dur, 1),
                })
            else:
                q.put({"type": "error", "message": "FFmpeg 合成失敗"})

        except Exception as e:
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


@app.route("/api/yt/highlight-progress/<task_id>")
def api_yt_highlight_progress(task_id):
    """精華影片 SSE 進度"""
    def generate():
        with highlight_tasks_lock:
            task = highlight_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type': 'error', 'message': '任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        with highlight_tasks_lock:
            highlight_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/yt/highlight-persons")
def api_yt_highlight_persons():
    """列出有擷取影片的人物"""
    persons = []
    if os.path.isdir(YT_EXTRACTS):
        exts = {".mp4", ".mkv", ".webm"}
        for d in sorted(os.listdir(YT_EXTRACTS)):
            dp = os.path.join(YT_EXTRACTS, d)
            if not os.path.isdir(dp):
                continue
            vids = [f for f in os.listdir(dp)
                    if os.path.splitext(f)[1].lower() in exts
                    and not f.startswith("highlight_")]
            if vids:
                persons.append({"name": d, "video_count": len(vids)})
    return jsonify(persons)


@app.route("/api/yt/highlight-videos/<person>")
def api_yt_highlight_videos(person):
    """列出某人物的擷取影片（供精華影片選擇）"""
    person = sanitize_name(resolve_celebrity(person))
    person_dir = os.path.join(YT_EXTRACTS, person)
    if not os.path.isdir(person_dir):
        return jsonify([])
    exts = {".mp4", ".mkv", ".webm"}
    videos = []
    for f in sorted(os.listdir(person_dir)):
        fp = os.path.join(person_dir, f)
        if not os.path.isfile(fp):
            continue
        if os.path.splitext(f)[1].lower() not in exts:
            continue
        if f.startswith("highlight_"):
            continue
        size = os.path.getsize(fp)
        # 取得影片時長
        dur = None
        try:
            import cv2 as _cv2
            cap = _cv2.VideoCapture(fp)
            if cap.isOpened():
                dur = round(cap.get(_cv2.CAP_PROP_FRAME_COUNT) / max(cap.get(_cv2.CAP_PROP_FPS), 1), 1)
            cap.release()
        except Exception:
            pass
        videos.append({
            "filename": f,
            "size_mb": round(size / 1024 / 1024, 1),
            "duration": dur,
            "url": f"/yt-files/extracts/{person}/{f}",
        })
    return jsonify(videos)


# ══════════════════════════════════════════════════════════
#  一鍵影片生成（自動化 pipeline）
# ══════════════════════════════════════════════════════════
auto_video_tasks = {}
auto_video_tasks_lock = threading.Lock()


@app.route("/api/auto-video", methods=["POST"])
def api_auto_video():
    """一鍵影片生成：TikTok搜尋 → 下載 → 人臉擷取 → 精華剪輯"""
    if not HAS_YTDLP:
        return jsonify({"error": "yt-dlp 未安裝"}), 500
    if not HAS_FACE_MODELS or not HAS_CV2:
        return jsonify({"error": "人臉模型或 OpenCV 未安裝"}), 500

    data = request.json
    person = (data.get("person") or "").strip()
    if not person:
        return jsonify({"error": "請輸入人物名稱"}), 400

    search_keyword = (data.get("search_keyword") or "").strip() or person
    max_videos = int(data.get("max_videos", 5))
    clip_duration = float(data.get("clip_duration", 3))
    total_duration = float(data.get("total_duration", 30))
    strategy = data.get("strategy", "balanced")
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    resolution = data.get("resolution", "720p_v")  # 預設直式
    audio_mode = data.get("audio_mode", "original")
    platform = data.get("platform", "tiktok")

    # 先確認照片資料夾存在
    resolved_person = resolve_celebrity(person)
    photo_dir = os.path.join(DOWNLOAD_ROOT, resolved_person)
    if not os.path.isdir(photo_dir):
        available = sorted([
            d for d in os.listdir(DOWNLOAD_ROOT)
            if os.path.isdir(os.path.join(DOWNLOAD_ROOT, d))
               and not d.startswith(".")
        ])
        return jsonify({
            "need_mapping": True,
            "keyword": person,
            "resolved": resolved_person,
            "available_folders": available,
            "message": f"找不到「{person}」的照片資料夾，請選擇要用哪個人物的照片進行辨識"
        }), 200

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with auto_video_tasks_lock:
        auto_video_tasks[task_id] = {"queue": q, "status": "running"}

    def worker():
        try:
            _auto_video_pipeline(
                q, resolved_person, search_keyword, max_videos,
                clip_duration, total_duration, strategy, transition,
                transition_dur, resolution, audio_mode, platform
            )
        except Exception as e:
            import traceback
            logging.error(f"auto-video error: {traceback.format_exc()}")
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


def _auto_video_pipeline(q, person, search_keyword, max_videos,
                          clip_duration, total_duration, strategy,
                          transition, transition_dur, resolution,
                          audio_mode, platform):
    """完整 pipeline：搜尋 → 下載 → 擷取 → 精華剪輯"""
    import subprocess

    # ── Phase 1: 搜尋 (0-5%) ──
    q.put({"type": "progress", "phase": "search", "percent": 0,
           "message": f"🔍 搜尋 TikTok: {search_keyword}..."})

    if platform == "tiktok":
        if search_keyword.startswith("@"):
            # yt-dlp 抓使用者頁面
            try:
                tiktok_url = f"https://www.tiktok.com/{search_keyword}"
                ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(tiktok_url, download=False)
                    entries = (info.get("entries") or [])[:max_videos]
                search_results = [{
                    "url": e.get("url") or e.get("webpage_url") or f"https://www.tiktok.com/video/{e.get('id','')}",
                    "title": e.get("title", ""),
                    "channel": e.get("channel") or e.get("uploader", ""),
                    "id": e.get("id", ""),
                } for e in entries if e]
            except Exception as e:
                q.put({"type": "error", "message": f"TikTok 搜尋失敗: {e}"})
                return
        else:
            search_results = _tiktok_search(search_keyword, max_videos)
    else:
        # YouTube 搜尋
        try:
            ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch{max_videos}:{search_keyword}", download=False)
                entries = info.get("entries", [])
            search_results = [{
                "url": e.get("url") or e.get("webpage_url") or f"https://www.youtube.com/watch?v={e.get('id','')}",
                "title": e.get("title", ""),
                "channel": e.get("channel") or e.get("uploader", ""),
                "id": e.get("id", ""),
            } for e in entries if e]
        except Exception as e:
            q.put({"type": "error", "message": f"YouTube 搜尋失敗: {e}"})
            return

    if not search_results:
        q.put({"type": "error", "message": f"搜尋「{search_keyword}」沒有結果"})
        return

    q.put({"type": "progress", "phase": "search", "percent": 5,
           "message": f"✅ 找到 {len(search_results)} 個影片"})

    # ── Phase 2: 下載 (5-35%) ──
    person_folder = sanitize_name(person)
    dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
    os.makedirs(dl_dir, exist_ok=True)

    downloaded_files = []
    for vi, v in enumerate(search_results):
        url = v.get("url", "")
        title = v.get("title", "") or f"video_{vi+1}"
        pct_base = 5 + int(vi / len(search_results) * 30)
        pct_end = 5 + int((vi + 1) / len(search_results) * 30)

        q.put({"type": "progress", "phase": "download", "percent": pct_base,
               "message": f"📥 下載 {vi+1}/{len(search_results)}: {title[:50]}..."})

        try:
            out_tpl = os.path.join(dl_dir, "%(title).80s [%(id)s].%(ext)s")
            ydl_opts = {
                "format": "bestvideo[height<=720]+bestaudio/best",
                "outtmpl": out_tpl,
                "merge_output_format": "mp4",
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
            }
            if FFMPEG_LOCATION:
                ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION

            # 嘗試下載，失敗降級
            info = None
            for attempt, fmt in enumerate(["bestvideo[height<=720]+bestaudio/best", "best"]):
                if attempt > 0 and fmt == ydl_opts["format"]:
                    break
                try:
                    ydl_opts_try = dict(ydl_opts, format=fmt)
                    with yt_dlp.YoutubeDL(ydl_opts_try) as ydl:
                        info = ydl.extract_info(url, download=True)
                        info = ydl.sanitize_info(info)
                    break
                except Exception:
                    if attempt == 0:
                        continue
                    raise

            if info:
                vid_id = info.get("id", "")
                actual_file = None
                for f in sorted(os.listdir(dl_dir),
                                key=lambda x: os.path.getmtime(os.path.join(dl_dir, x)),
                                reverse=True):
                    fp_c = os.path.join(dl_dir, f)
                    if (os.path.isfile(fp_c) and vid_id in f
                            and not f.endswith(".part")
                            and os.path.getsize(fp_c) > 10240):
                        actual_file = f
                        break

                if actual_file:
                    rel_path = f"downloads/{person_folder}/{actual_file}"
                    downloaded_files.append({
                        "filename": actual_file,
                        "rel_path": rel_path,
                        "full_path": os.path.join(dl_dir, actual_file),
                    })

        except Exception as e:
            logging.warning(f"auto-video download failed: {url} - {e}")
            q.put({"type": "progress", "phase": "download", "percent": pct_end,
                   "message": f"⚠️ 下載失敗: {title[:40]}... ({e})"})
            continue

        q.put({"type": "progress", "phase": "download", "percent": pct_end,
               "message": f"✅ 已下載 {len(downloaded_files)}/{len(search_results)}"})

    if not downloaded_files:
        q.put({"type": "error", "message": "所有影片都下載失敗"})
        return

    q.put({"type": "progress", "phase": "download", "percent": 35,
           "message": f"📥 下載完成：{len(downloaded_files)} 個影片"})

    # ── Phase 3: 人臉擷取 (35-70%) ──
    q.put({"type": "progress", "phase": "extract", "percent": 35,
           "message": f"🧑 建立 {person} 的臉部特徵..."})

    def emb_cb(pct):
        p = 35 + int(pct * 5)
        q.put({"type": "progress", "phase": "extract", "percent": min(p, 40),
               "message": f"建立臉部特徵 {int(pct*100)}%"})

    ref = _build_reference_embeddings(person, emb_cb)
    if ref is None or len(ref) < 3:
        q.put({"type": "error",
               "message": f"{person} 的照片中找不到足夠的臉部特徵（至少需要 3 張）"})
        return

    q.put({"type": "progress", "phase": "extract", "percent": 40,
           "message": f"✅ 使用 {len(ref)} 張臉部特徵，開始掃描影片..."})

    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)
    extracted_files = []

    for vi, dlf in enumerate(downloaded_files):
        video_path = dlf["full_path"]
        vname = dlf["filename"]
        pct_base = 40 + int(vi / len(downloaded_files) * 25)

        q.put({"type": "progress", "phase": "extract", "percent": pct_base,
               "message": f"🔍 掃描 {vi+1}/{len(downloaded_files)}: {vname[:40]}..."})

        def scan_cb(pct, _vi=vi):
            p = 40 + int((_vi + pct) / len(downloaded_files) * 25)
            q.put({"type": "progress", "phase": "extract", "percent": min(p, 65),
                   "message": f"掃描 {vname[:30]}... {int(pct*100)}%"})

        timestamps = _scan_video_for_person(video_path, ref, scan_cb)
        if not timestamps:
            q.put({"type": "progress", "phase": "extract", "percent": pct_base + 5,
                   "message": f"⏭️ {vname[:40]} 中未偵測到 {person}"})
            continue

        segments = _merge_segments(timestamps)
        total_dur = sum(e - s for s, e in segments)

        base = os.path.splitext(os.path.basename(vname))[0]
        out_name = f"{base}_{person}.mp4"
        out_path = os.path.join(extract_dir, out_name)
        result = _extract_segments(video_path, segments, out_path)

        if result and os.path.isfile(result):
            extracted_files.append(result)
            q.put({"type": "progress", "phase": "extract",
                   "percent": 40 + int((vi + 1) / len(downloaded_files) * 25),
                   "message": f"✅ 擷取 {len(segments)} 段（{total_dur:.1f}秒）from {vname[:30]}"})

    if not extracted_files:
        q.put({"type": "error", "message": f"所有影片中都未偵測到 {person}"})
        return

    q.put({"type": "progress", "phase": "extract", "percent": 70,
           "message": f"🎯 擷取完成：{len(extracted_files)} 個影片包含 {person}"})

    # ── Phase 4: 精華剪輯 (70-100%) ──
    q.put({"type": "progress", "phase": "highlight", "percent": 70,
           "message": "🎬 分析影片，挑選精彩片段..."})

    all_scores = []
    for vi, vpath in enumerate(extracted_files):
        vname = os.path.basename(vpath)
        def score_cb(pct, _vi=vi):
            p = 70 + int((_vi + pct) / len(extracted_files) * 15)
            q.put({"type": "progress", "phase": "highlight", "percent": min(p, 85),
                   "message": f"分析 {vname[:30]}... {int(pct*100)}%"})

        scores = _score_video_highlights(vpath, strategy, score_cb)
        if scores:
            all_scores.append((vpath, scores))

    if not all_scores:
        q.put({"type": "error", "message": "所有擷取影片都無法分析"})
        return

    max_per_video = max(2, int(total_duration / clip_duration / max(len(all_scores), 1)) + 1)
    clips = _select_highlight_clips(all_scores, clip_duration,
                                     total_duration, max_per_video)
    if not clips:
        q.put({"type": "error", "message": "找不到符合條件的片段"})
        return

    actual_dur = len(clips) * clip_duration
    q.put({"type": "progress", "phase": "highlight", "percent": 87,
           "message": f"✂️ 選出 {len(clips)} 個片段（{actual_dur:.0f}秒），合成中..."})

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"auto_{sanitize_name(person)}_{ts}.mp4"
    out_path = os.path.join(extract_dir, out_name)

    def compile_cb(pct):
        p = 87 + int(pct * 12)
        q.put({"type": "progress", "phase": "highlight", "percent": min(p, 99),
               "message": f"合成影片 {int(pct*100)}%"})

    result = _compile_highlight(
        clips, clip_duration, out_path, transition,
        transition_dur, resolution, compile_cb,
        audio_mode=audio_mode,
    )

    if result and os.path.isfile(result):
        rel_path = f"extracts/{sanitize_name(person)}/{out_name}"
        fsize = os.path.getsize(result)
        q.put({
            "type": "done",
            "filename": out_name,
            "rel_path": rel_path,
            "file_url": f"/yt-files/{rel_path}",
            "file_size_mb": round(fsize / 1024 / 1024, 1),
            "clips_count": len(clips),
            "duration": round(actual_dur, 1),
            "downloaded": len(downloaded_files),
            "extracted": len(extracted_files),
        })
    else:
        q.put({"type": "error", "message": "FFmpeg 合成失敗"})


@app.route("/api/auto-video/progress/<task_id>")
def api_auto_video_progress(task_id):
    """一鍵影片生成 SSE 進度"""
    def generate():
        with auto_video_tasks_lock:
            task = auto_video_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type': 'error', 'message': '任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=60)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        with auto_video_tasks_lock:
            auto_video_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/yt/rotate", methods=["POST"])
def api_yt_rotate():
    """旋轉影片 (90/180/270 度)"""
    import subprocess as _sp

    data = request.json
    rel_path = (data.get("rel_path") or "").strip()
    angle = data.get("angle")

    if not rel_path:
        return jsonify({"error": "需要 rel_path"}), 400
    if angle not in (90, 180, 270):
        return jsonify({"error": "angle 須為 90, 180 或 270"}), 400

    video_path = os.path.normpath(os.path.join(YT_ROOT, rel_path))
    if not video_path.startswith(os.path.normpath(YT_ROOT)):
        return jsonify({"error": "無效的檔案路徑"}), 400
    if not os.path.isfile(video_path):
        return jsonify({"error": f"找不到影片: {rel_path}"}), 404

    ffmpeg_exe = _get_ffmpeg()
    if angle == 90:
        vf = "transpose=1"
    elif angle == 270:
        vf = "transpose=2"
    else:
        vf = "transpose=1,transpose=1"

    dir_name = os.path.dirname(video_path)
    base, ext = os.path.splitext(os.path.basename(video_path))
    tmp_out = os.path.join(dir_name, f".rotating_{base}{ext}")

    try:
        cmd = [
            ffmpeg_exe, "-y",
            "-i", video_path,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "copy",
            "-map_metadata", "0",
            tmp_out,
        ]
        result = _sp.run(cmd, capture_output=True, timeout=600)
        if result.returncode != 0 or not os.path.isfile(tmp_out):
            stderr = result.stderr.decode(errors="replace")[:500]
            return jsonify({"error": f"FFmpeg 失敗: {stderr}"}), 500

        os.replace(tmp_out, video_path)
        size = os.path.getsize(video_path)
        return jsonify({
            "ok": True,
            "rel_path": rel_path,
            "size_mb": round(size / 1024 / 1024, 1),
        })
    except _sp.TimeoutExpired:
        if os.path.isfile(tmp_out):
            os.remove(tmp_out)
        return jsonify({"error": "FFmpeg 逾時（影片可能太大）"}), 500
    except Exception as e:
        if os.path.isfile(tmp_out):
            os.remove(tmp_out)
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════
#  HTML 前端
# ══════════════════════════════════════════════════════════
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Media Downloader</title>
<style>
:root{--pri:#4F46E5;--pri-h:#4338CA;--ok:#10B981;--warn:#F59E0B;--err:#EF4444;
--bg:#F3F4F6;--card:#FFF;--txt:#1F2937;--txt2:#6B7280;--border:#E5E7EB}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Microsoft JhengHei',sans-serif;
background:var(--bg);color:var(--txt);padding:16px}
.container{max-width:920px;margin:0 auto}
header{text-align:center;padding:20px 0 8px;font-size:1.6em;font-weight:700;color:var(--pri)}
header small{display:block;font-size:.45em;color:var(--txt2);font-weight:400;margin-top:4px}
/* Tabs */
.tabs{display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid var(--border)}
.tab-btn{padding:10px 24px;border:none;background:none;font-size:1em;font-weight:600;color:var(--txt2);
  cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;transition:all .2s}
.tab-btn:hover{color:var(--pri)}
.tab-btn.active{color:var(--pri);border-bottom-color:var(--pri)}
.tab-content{display:none}
.tab-content.active{display:block}
/* YT specific */
.yt-results{display:grid;gap:12px;margin-top:12px}
.yt-item{display:flex;gap:12px;padding:12px;background:var(--card);border:1px solid var(--border);border-radius:10px;cursor:pointer;transition:box-shadow .2s;position:relative}
.yt-item:hover{box-shadow:0 2px 8px rgba(0,0,0,.1)}
.yt-item.yt-checked{border-color:var(--pri);background:rgba(99,102,241,.05)}
.yt-item img{width:180px;height:101px;object-fit:cover;border-radius:6px;flex-shrink:0}
.yt-item-info{flex:1;min-width:0}
.yt-item-title{font-weight:600;font-size:.92em;margin-bottom:4px;overflow:hidden;text-overflow:ellipsis;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.yt-item-meta{font-size:.8em;color:var(--txt2)}
.yt-item-actions{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
.yt-item-cb{position:absolute;top:8px;right:8px;width:22px;height:22px;cursor:pointer;accent-color:var(--pri);z-index:2}
.yt-batch-bar{display:flex;align-items:center;gap:10px;padding:10px 16px;background:var(--pri);color:#fff;border-radius:10px;margin-bottom:10px;font-size:.88em;flex-wrap:wrap}
.yt-batch-bar button{background:rgba(255,255,255,.2);color:#fff;border:1px solid rgba(255,255,255,.4);padding:5px 14px;border-radius:6px;cursor:pointer;font-size:.9em;font-weight:500}
.yt-batch-bar button:hover{background:rgba(255,255,255,.35)}
.yt-dl-list{display:grid;gap:8px;margin-top:12px}
.yt-dl-item{display:flex;align-items:center;gap:10px;padding:10px;background:var(--card);border:1px solid var(--border);border-radius:8px;font-size:.88em;flex-wrap:wrap}
.yt-dl-item .fname{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.yt-dl-item a{color:var(--pri);text-decoration:none;font-weight:500}
.card{background:var(--card);border-radius:12px;padding:20px 24px;margin-bottom:14px;
box-shadow:0 1px 3px rgba(0,0,0,.08)}
.card h3{font-size:1em;color:var(--pri);margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.row{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
.row:last-child{margin-bottom:0}
label{font-size:.85em;color:var(--txt2);min-width:70px}
input[type=text],input[type=number],select{padding:7px 12px;border:1px solid var(--border);
border-radius:8px;font-size:.9em;outline:none;transition:border .2s}
input:focus,select:focus{border-color:var(--pri)}
input[type=text]{flex:1;min-width:180px}
input[type=number]{width:80px}
select{min-width:120px}
.cb{display:flex;align-items:center;gap:4px;font-size:.85em;cursor:pointer;user-select:none}
.cb input{accent-color:var(--pri)}
.btn{padding:8px 20px;border:none;border-radius:8px;font-size:.9em;font-weight:600;
cursor:pointer;transition:all .15s;display:inline-flex;align-items:center;gap:6px}
.btn-pri{background:var(--pri);color:#fff}.btn-pri:hover{background:var(--pri-h)}
.btn-danger{background:var(--err);color:#fff}.btn-danger:hover{background:#DC2626}
.btn-sec{background:#E5E7EB;color:var(--txt)}.btn-sec:hover{background:#D1D5DB}
.btn:disabled{opacity:.5;cursor:not-allowed}
.progress-wrap{background:#E5E7EB;border-radius:8px;height:22px;overflow:hidden;position:relative}
.progress-bar{height:100%;background:linear-gradient(90deg,var(--pri),#818CF8);
border-radius:8px;transition:width .3s;min-width:0}
.progress-text{position:absolute;right:8px;top:50%;transform:translateY(-50%);
font-size:.75em;font-weight:600;color:var(--txt)}
.stats{display:flex;gap:16px;flex-wrap:wrap;font-size:.82em;margin-top:8px;color:var(--txt2)}
.stats span{display:flex;align-items:center;gap:3px}
.stats .dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.log{max-height:260px;overflow-y:auto;font-family:Consolas,'Courier New',monospace;
font-size:.8em;line-height:1.7;padding:8px;background:#F9FAFB;border-radius:8px;margin-top:10px}
.log .ok{color:var(--ok)}.log .skip{color:var(--warn)}.log .error{color:var(--err)}
.log .info{color:var(--pri)}.log .stop{color:#999}
table{width:100%;border-collapse:collapse;font-size:.88em}
th{text-align:left;padding:8px 12px;background:#F9FAFB;color:var(--txt2);font-weight:600;
border-bottom:2px solid var(--border)}
td{padding:8px 12px;border-bottom:1px solid var(--border)}
tr:hover td{background:#F3F4F6}
.cel-name{color:var(--pri);cursor:pointer;font-weight:500}.cel-name:hover{text-decoration:underline}
.total-bar{margin-top:10px;font-size:.82em;color:var(--txt2)}
/* Modal */
.modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:100;
justify-content:center;align-items:center;padding:20px}
.modal.show{display:flex}
.modal-box{background:#fff;border-radius:16px;width:100%;max-width:800px;max-height:85vh;
display:flex;flex-direction:column;overflow:hidden}
.modal-head{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;
border-bottom:1px solid var(--border)}
.modal-head h3{font-size:1.1em}
.modal-close{background:none;border:none;font-size:1.5em;cursor:pointer;color:var(--txt2);
padding:0 4px}.modal-close:hover{color:var(--txt)}
.modal-body{padding:16px 20px;overflow-y:auto;flex:1}
.gallery{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:8px}
.gallery img{width:100%;aspect-ratio:1;object-fit:cover;border-radius:8px;cursor:pointer;
transition:transform .15s}.gallery img:hover{transform:scale(1.03)}
.gallery-info{font-size:.72em;text-align:center;color:var(--txt2);margin-top:2px}
textarea{width:100%;min-height:150px;padding:10px;border:1px solid var(--border);
border-radius:8px;font-size:.9em;resize:vertical;font-family:inherit}
/* Lightbox */
.lightbox{display:none;position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:200;
justify-content:center;align-items:center;cursor:zoom-out}
.lightbox.show{display:flex}
.lightbox img{max-width:95vw;max-height:95vh;object-fit:contain;border-radius:4px}
@media(max-width:600px){.row{flex-direction:column;align-items:stretch}label{min-width:auto}
.gallery{grid-template-columns:repeat(auto-fill,minmax(100px,1fr))}}
.vid-hist-item{display:flex;align-items:center;gap:10px;padding:8px 4px;border-bottom:1px solid var(--border);font-size:.85em}
.vid-hist-item:last-child{border:none}
.vid-hist-item a{color:var(--pri);text-decoration:none;font-weight:500}
.vid-hist-item a:hover{text-decoration:underline}
.vid-hist-item .meta{color:var(--txt2);font-size:.9em}
/* 照片預覽 */
.preview-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(100px,1fr));gap:8px;margin:12px 0}
.preview-cell{position:relative;border-radius:8px;overflow:hidden;aspect-ratio:1;background:#111;cursor:pointer}
.preview-cell img{width:100%;height:100%;object-fit:cover;transition:opacity .2s}
.preview-cell .x-btn,.preview-cell .dl-btn{position:absolute;width:24px;height:24px;border-radius:50%;
  background:rgba(0,0,0,.65);color:#fff;border:none;font-size:14px;line-height:24px;text-align:center;
  cursor:pointer;opacity:0;transition:opacity .2s;z-index:2;text-decoration:none;display:flex;align-items:center;justify-content:center}
.preview-cell .x-btn{top:2px;right:2px}
.preview-cell .dl-btn{top:2px;left:2px;font-size:13px}
.preview-cell:hover .x-btn,.preview-cell:hover .dl-btn{opacity:1}
.preview-cell.replacing img{opacity:.3}
.preview-cell.replacing::after{content:'';position:absolute;top:50%;left:50%;width:20px;height:20px;
  margin:-10px 0 0 -10px;border:2px solid #fff;border-top-color:transparent;border-radius:50%;animation:spin .6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.preview-actions{display:flex;gap:10px;align-items:center;margin-top:8px}
.preview-hint{font-size:.82em;color:var(--txt2)}
.preview-cell .used-badge{position:absolute;bottom:2px;left:2px;font-size:10px;padding:1px 5px;
  border-radius:8px;background:rgba(255,160,0,.85);color:#fff;z-index:2;pointer-events:none}
</style>
</head>
<body>
<div class="container">
<header>Media Downloader<small>Web 版 — 區網內任何裝置皆可使用</small></header>

<div class="tabs">
  <button class="tab-btn active" onclick="switchTab('photo')">📸 Photo Download</button>
  <button class="tab-btn" onclick="switchTab('yt')">🎬 影片下載</button>
  <button class="tab-btn" onclick="switchTab('autovid')">🚀 一鍵生成</button>
</div>

<!-- ═══════════ TAB 1: Photo Download ═══════════ -->
<div id="tab-photo" class="tab-content active">

<!-- 搜尋設定 -->
<div class="card">
<h3>搜尋設定</h3>
<div class="row">
  <label>Photo 名稱</label>
  <input type="text" id="inp-name" placeholder="例: Taylor Swift, IU, 周杰倫">
</div>
<div class="row">
  <label>圖片來源</label>
  <label class="cb"><input type="checkbox" id="src-bing" checked> Bing</label>
  <label class="cb"><input type="checkbox" id="src-google"> Google</label>
  <label class="cb"><input type="checkbox" id="src-ddg"> DuckDuckGo</label>
  <label class="cb"><input type="checkbox" id="src-pinterest"> Pinterest</label>
</div>
<div class="row">
  <label>下載數量</label>
  <input type="number" id="inp-count" value="100" min="10" max="1000" step="10">
  <label>圖片大小</label>
  <select id="sel-size">
    <option value="any">不限</option>
    <option value="large">大圖 (&gt;500px)</option>
    <option value="xlarge">超大 (&gt;1024px)</option>
  </select>
</div>
<div class="row">
  <label class="cb"><input type="checkbox" id="chk-photo" checked> 附加 "photo"</label>
  <label class="cb"><input type="checkbox" id="chk-face"> 僅人物照</label>
  <label class="cb"><input type="checkbox" id="chk-autokw" checked> 智慧關鍵字</label>
  <span style="flex:1"></span>
  <label class="cb"><input type="checkbox" id="chk-url" checked> URL去重</label>
  <label class="cb"><input type="checkbox" id="chk-md5" checked> MD5去重</label>
  <label class="cb"><input type="checkbox" id="chk-phash" checked> 感知雜湊</label>
</div>
<div class="row" style="margin-top:14px">
  <button class="btn btn-pri" id="btn-start" onclick="startDownload()">▶ 開始下載</button>
  <button class="btn btn-danger" id="btn-stop" onclick="stopDownload()" disabled>■ 停止</button>
  <button class="btn btn-sec" onclick="showBatch()">批次下載</button>
</div>
</div>

<!-- 進度 -->
<div class="card" id="progress-card" style="display:none">
<h3>下載進度</h3>
<div class="progress-wrap">
  <div class="progress-bar" id="prog-bar" style="width:0%"></div>
  <span class="progress-text" id="prog-text">0/0</span>
</div>
<div class="stats" id="stats-row">
  <span><span class="dot" style="background:var(--ok)"></span> 新增: <b id="st-dl">0</b></span>
  <span><span class="dot" style="background:var(--warn)"></span> URL跳過: <b id="st-url">0</b></span>
  <span><span class="dot" style="background:var(--warn)"></span> MD5跳過: <b id="st-md5">0</b></span>
  <span><span class="dot" style="background:var(--warn)"></span> 相似: <b id="st-ph">0</b></span>
  <span><span class="dot" style="background:var(--err)"></span> 失敗: <b id="st-fail">0</b></span>
</div>
<div class="log" id="log"></div>
</div>

<!-- 已下載 Photo -->
<div class="card">
<h3>已下載 Photo</h3>
<table>
<thead><tr><th>名稱</th><th>數量</th><th>最後下載</th></tr></thead>
<tbody id="celeb-list"></tbody>
</table>
<div class="total-bar" id="total-bar"></div>
</div>

<!-- 影片生成 -->
<div class="card">
<h3>🎬 影片生成</h3>
<div class="row">
  <label>選擇 Photo</label>
  <select id="vid-celeb" style="min-width:180px" onchange="onVidCelebChange()">
    <option value="">-- 請先下載照片 --</option>
  </select>
  <span id="vid-photo-count" style="font-size:.82em;color:var(--txt2)"></span>
</div>
<div class="row">
  <label>影片模板</label>
  <select id="vid-template" onchange="showTemplateDesc()"></select>
</div>
<div id="template-desc" style="font-size:.85em;color:var(--txt2);padding:4px 0 8px;display:none"></div>
<div class="row">
  <label>最多照片</label>
  <input type="range" id="vid-max-photos" min="4" max="30" value="12"
         oninput="document.getElementById('vid-max-label').textContent=this.value"
         style="flex:1;max-width:200px">
  <span id="vid-max-label" style="font-size:.9em;min-width:24px">12</span>
  <label class="cb" style="margin-left:16px">
    <input type="checkbox" id="vid-shuffle" checked> 隨機排序
  </label>
</div>
<div class="row">
  <label>每張秒數</label>
  <input type="range" id="vid-duration" min="0.3" max="6" value="0" step="0.1"
         oninput="updateDurLabel()"
         style="flex:1;max-width:200px">
  <span id="vid-dur-label" style="font-size:.9em;min-width:60px">自動</span>
</div>
<div class="row" style="margin-top:10px">
  <button class="btn btn-pri" id="btn-preview-photos" onclick="previewPhotos()">👁 預覽照片</button>
  <button class="btn btn-pri" id="btn-gen-video" onclick="confirmGenerate()" style="display:none">🎬 確認生成影片</button>
</div>
<div id="photo-preview-area" style="display:none;margin-top:14px">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <span class="preview-hint">✕ 移除不要的 · ⬇ 下載單張 · 用過的照片會自動降低優先</span>
    <button class="btn" onclick="previewPhotos()" style="font-size:.8em;padding:4px 10px">🔄 重新挑選</button>
  </div>
  <div class="preview-grid" id="photo-preview-grid"></div>
  <div class="preview-actions">
    <span id="preview-count" style="font-size:.85em;color:var(--txt2)"></span>
    <button class="btn" onclick="downloadAllPhotos()" style="font-size:.82em;padding:4px 12px">📦 全部下載</button>
  </div>
</div>
<div id="vid-progress-area" style="display:none;margin-top:14px">
  <div class="progress-wrap">
    <div class="progress-bar" id="vid-prog-bar" style="width:0%"></div>
    <span class="progress-text" id="vid-prog-text">0%</span>
  </div>
  <div id="vid-prog-msg" style="font-size:.82em;color:var(--txt2);margin-top:6px"></div>
</div>
<div id="vid-result" style="display:none;margin-top:14px">
  <video id="vid-player" controls style="width:100%;max-height:420px;border-radius:8px;background:#000"></video>
  <div style="margin-top:6px;font-size:.82em;color:var(--txt2)">
    <span id="vid-result-info"></span>
    <a id="vid-download-link" href="#" download style="margin-left:12px;color:var(--pri)">⬇ 下載影片</a>
  </div>
</div>
<div id="vid-history" style="margin-top:14px;display:none">
  <h4 style="font-size:.9em;color:var(--txt2);margin-bottom:8px">歷史影片</h4>
  <div id="vid-history-list"></div>
</div>
</div>

<!-- 別名 / 合併管理 -->
<div class="card">
<h3>🔗 別名 / 合併管理</h3>
<p style="font-size:.82em;color:var(--txt2);margin-bottom:10px">
  設定別名後，輸入任何別名都會自動對應到同一個 Photo。也可以合併重複的資料夾。
</p>
<div class="row">
  <select id="merge-src" style="min-width:140px"><option value="">-- 來源（被合併）--</option></select>
  <span style="font-size:1.2em">→</span>
  <select id="merge-dst" style="min-width:140px"><option value="">-- 目標（保留）--</option></select>
  <button class="btn btn-pri" onclick="mergeCelebrity()" style="font-size:.85em">合併+去重</button>
</div>
<div class="row" style="margin-top:6px">
  <select id="dedup-celeb" style="min-width:180px"><option value="">-- 選擇 Photo --</option></select>
  <button class="btn" onclick="dedupCelebrity()" style="font-size:.85em">🧹 單獨去重</button>
  <span id="dedup-result" style="font-size:.82em;color:var(--txt2)"></span>
</div>
<div style="margin-top:12px;border-top:1px solid var(--border);padding-top:12px">
  <div class="row">
    <input id="alias-canonical" placeholder="正式名稱 (如 ahyeon)" style="flex:1;max-width:160px">
    <input id="alias-name" placeholder="別名 (如 雅賢)" style="flex:1;max-width:160px">
    <button class="btn" onclick="addAlias()" style="font-size:.85em">+ 新增別名</button>
  </div>
  <div id="alias-list" style="margin-top:8px;font-size:.85em"></div>
</div>
</div>

</div>
</div><!-- /tab-photo -->

<!-- ═══════════ TAB 2: YouTube 下載 ═══════════ -->
<div id="tab-yt" class="tab-content">

<div class="card">
<h3>🔍 搜尋影片</h3>
<div class="row">
  <select id="yt-platform" style="min-width:110px" onchange="onPlatformChange()">
    <option value="youtube" selected>YouTube</option>
    <option value="tiktok">TikTok</option>
  </select>
  <input type="text" id="yt-query" placeholder="輸入關鍵字或貼上 YouTube / TikTok 網址" style="flex:1"
         onkeydown="if(event.key==='Enter')ytSearch()">
  <button class="btn btn-pri" onclick="ytSearch()" id="btn-yt-search">搜尋</button>
</div>
<div class="row" id="yt-type-row">
  <label>影片類型</label>
  <select id="yt-video-type">
    <option value="all">全部</option>
    <option value="stage" selected>舞台表演</option>
    <option value="fancam">直拍 (Fancam)</option>
    <option value="music_show">音樂節目</option>
    <option value="mv">MV</option>
  </select>
  <label style="margin-left:16px">搜尋數量</label>
  <select id="yt-max-results">
    <option value="5">5</option>
    <option value="10" selected>10</option>
    <option value="20">20</option>
  </select>
  <label style="margin-left:16px">預設品質</label>
  <select id="yt-quality">
    <option value="bestvideo[height<=1080]+bestaudio/best">1080p</option>
    <option value="bestvideo[height<=720]+bestaudio/best" selected>720p</option>
    <option value="bestvideo[height<=480]+bestaudio/best">480p</option>
    <option value="bestvideo+bestaudio/best">最高畫質</option>
    <option value="bestaudio/best">僅音訊 (MP3)</option>
  </select>
</div>
<div id="yt-search-status" style="font-size:.82em;color:var(--txt2);margin-top:4px"></div>
</div>

<div id="yt-progress-card" class="card" style="display:none">
<h3>⬇️ 下載中</h3>
<div id="yt-dl-title" style="font-weight:600;margin-bottom:8px"></div>
<div class="progress-wrap">
  <div class="progress-bar" id="yt-prog-bar" style="width:0%"></div>
  <span class="progress-text" id="yt-prog-text">0%</span>
</div>
<div id="yt-prog-msg" style="font-size:.82em;color:var(--txt2);margin-top:6px"></div>
</div>

<div id="yt-results-card" class="card" style="display:none">
<h3>搜尋結果</h3>
<div id="yt-batch-bar" class="yt-batch-bar" style="display:none">
  <input type="checkbox" id="yt-select-all" style="width:18px;height:18px;cursor:pointer;accent-color:#fff"
         onchange="ytToggleAll(this.checked)">
  <span id="yt-batch-count">已選 0 部</span>
  <button onclick="ytBatchAction('download')">⬇ 批量下載</button>
  <button onclick="ytBatchAction('extract')">🎯 批量下載+擷取</button>
</div>
<div id="yt-results" class="yt-results"></div>
</div>

<div id="yt-extract-card" class="card" style="display:none">
<h3>🎯 人物擷取</h3>
<div id="yt-extract-title" style="font-weight:600;margin-bottom:8px"></div>
<div class="row">
  <label>擷取人物</label>
  <select id="extract-person" style="min-width:160px"></select>
  <button class="btn btn-pri" onclick="ytExtract()" id="btn-extract" style="font-size:.85em">🔍 開始擷取</button>
</div>
<div class="progress-wrap" id="extract-prog-wrap" style="display:none;margin-top:8px">
  <div class="progress-bar" id="extract-prog-bar" style="width:0%"></div>
  <span class="progress-text" id="extract-prog-text">0%</span>
</div>
<div id="extract-msg" style="font-size:.82em;color:var(--txt2);margin-top:6px"></div>
</div>

<!-- 人物對應選擇對話框 -->
<div id="yt-mapping-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:9999;justify-content:center;align-items:center">
<div style="background:#fff;border-radius:12px;padding:24px;max-width:420px;width:90%;box-shadow:0 8px 32px rgba(0,0,0,.2)">
  <h3 style="margin:0 0 8px">⚠️ 找不到照片資料夾</h3>
  <p id="mapping-msg" style="font-size:.9em;color:var(--txt2);margin:0 0 16px"></p>
  <label style="font-size:.85em;font-weight:600">選擇要用哪個人物的照片辨識：</label>
  <select id="mapping-select" style="width:100%;padding:8px;margin:8px 0 16px;font-size:1em;border-radius:6px;border:1px solid #ccc"></select>
  <div style="display:flex;gap:8px;justify-content:flex-end">
    <button class="btn" onclick="ytMappingCancel()" style="background:#eee;color:#333">取消</button>
    <button class="btn btn-pri" onclick="ytMappingConfirm()">✅ 確認並擷取</button>
  </div>
</div>
</div>

<div id="yt-highlight-card" class="card">
<h3>🎬 精華影片</h3>
<div class="row" style="flex-wrap:wrap;gap:8px;align-items:end">
  <div>
    <label style="font-size:.8em;color:var(--txt2)">人物</label>
    <select id="hl-person" style="min-width:120px" onchange="hlLoadPersonInfo()"></select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">策略</label>
    <select id="hl-strategy">
      <option value="balanced">均衡</option>
      <option value="closeup">特寫優先</option>
      <option value="dynamic">動態優先</option>
      <option value="random">隨機</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">每段秒數</label>
    <select id="hl-clip-dur">
      <option value="2">2 秒</option>
      <option value="3" selected>3 秒</option>
      <option value="4">4 秒</option>
      <option value="5">5 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">總長度</label>
    <select id="hl-total-dur">
      <option value="15">15 秒</option>
      <option value="30" selected>30 秒</option>
      <option value="45">45 秒</option>
      <option value="60">60 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">每部最多取</label>
    <select id="hl-max-per">
      <option value="2">2 段</option>
      <option value="3">3 段</option>
      <option value="5" selected>5 段</option>
      <option value="0">不限</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">過場</label>
    <select id="hl-transition" onchange="document.getElementById('hl-xfade-dur').style.display=this.value==='crossfade'?'':'none'">
      <option value="crossfade" selected>淡入淡出</option>
      <option value="cut">直切</option>
    </select>
  </div>
  <div id="hl-xfade-dur">
    <label style="font-size:.8em;color:var(--txt2)">過場秒數</label>
    <select id="hl-xfade-val">
      <option value="0.3">0.3 秒</option>
      <option value="0.5" selected>0.5 秒</option>
      <option value="0.8">0.8 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">解析度</label>
    <select id="hl-resolution">
      <option value="720p" selected>720p</option>
      <option value="1080p">1080p</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">音訊</label>
    <select id="hl-audio-mode">
      <option value="original" selected>保留原音</option>
      <option value="mute">靜音</option>
    </select>
  </div>
</div>
<div id="hl-video-list" style="margin-top:10px;display:none">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
    <span style="font-size:.82em;font-weight:600;color:var(--txt2)">選擇影片來源</span>
    <label style="font-size:.78em;color:var(--pri);cursor:pointer">
      <input type="checkbox" id="hl-select-all" checked onchange="hlToggleAllVideos(this.checked)"> 全選
    </label>
  </div>
  <div id="hl-videos" style="display:grid;gap:4px;max-height:180px;overflow-y:auto;padding-right:4px"></div>
</div>
<div style="margin-top:10px">
  <span id="hl-person-info" style="font-size:.82em;color:var(--txt2)"></span>
</div>
<button class="btn btn-pri" onclick="ytHighlight()" id="btn-highlight" style="margin-top:10px">🎬 開始製作精華</button>
<div class="progress-wrap" id="hl-prog-wrap" style="display:none;margin-top:8px">
  <div class="progress-bar" id="hl-prog-bar" style="width:0%"></div>
  <span class="progress-text" id="hl-prog-text">0%</span>
</div>
<div id="hl-msg" style="font-size:.82em;color:var(--txt2);margin-top:6px"></div>
</div>

<div class="card">
<h3>📁 已下載影片</h3>
<button class="btn" onclick="ytLoadDownloads()" style="font-size:.82em;margin-bottom:8px">🔄 重新整理</button>
<div id="yt-downloads" class="yt-dl-list">
  <span style="color:var(--txt2);font-size:.85em">載入中...</span>
</div>
</div>

</div><!-- /tab-yt -->

<!-- ═══════════ TAB 3: 一鍵生成 ═══════════ -->
<div id="tab-autovid" class="tab-content">

<div class="card">
<h3>🚀 一鍵影片生成</h3>
<p style="font-size:.82em;color:var(--txt2);margin-bottom:12px">
  輸入人物名稱，自動完成：TikTok 搜尋 → 下載 → 人臉辨識擷取 → 精華剪輯
</p>
<div class="row">
  <label>人物名稱</label>
  <input type="text" id="av-person" placeholder="例: ahyeon, karina, wonyoung"
         onkeydown="if(event.key==='Enter')autoVideoStart()">
</div>
<div class="row">
  <label>搜尋關鍵字</label>
  <input type="text" id="av-keyword" placeholder="(選填) 預設同人物名稱，可輸入更精確的關鍵字">
</div>
<div class="row" style="flex-wrap:wrap;gap:8px;align-items:end">
  <div>
    <label style="font-size:.8em;color:var(--txt2)">平台</label>
    <select id="av-platform">
      <option value="tiktok" selected>TikTok</option>
      <option value="youtube">YouTube</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">下載數量</label>
    <select id="av-max-videos">
      <option value="3">3 部</option>
      <option value="5" selected>5 部</option>
      <option value="8">8 部</option>
      <option value="10">10 部</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">策略</label>
    <select id="av-strategy">
      <option value="balanced" selected>均衡</option>
      <option value="closeup">特寫優先</option>
      <option value="dynamic">動態優先</option>
      <option value="random">隨機</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">每段秒數</label>
    <select id="av-clip-dur">
      <option value="2">2 秒</option>
      <option value="3" selected>3 秒</option>
      <option value="4">4 秒</option>
      <option value="5">5 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">總長度</label>
    <select id="av-total-dur">
      <option value="15">15 秒</option>
      <option value="30" selected>30 秒</option>
      <option value="45">45 秒</option>
      <option value="60">60 秒</option>
      <option value="90">90 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">方向</label>
    <select id="av-orientation">
      <option value="vertical" selected>直式 (9:16 TikTok)</option>
      <option value="horizontal">橫式 (16:9)</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">解析度</label>
    <select id="av-resolution">
      <option value="720" selected>720p</option>
      <option value="1080">1080p</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">過場</label>
    <select id="av-transition">
      <option value="crossfade" selected>淡入淡出</option>
      <option value="cut">直切</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">音訊</label>
    <select id="av-audio">
      <option value="original" selected>保留原音</option>
      <option value="mute">靜音</option>
    </select>
  </div>
</div>
<div style="margin-top:14px">
  <button class="btn btn-pri" id="btn-auto-video" onclick="autoVideoStart()"
          style="font-size:1em;padding:10px 32px">🚀 一鍵生成影片</button>
</div>
</div>

<div id="av-progress-card" class="card" style="display:none">
<h3 id="av-phase-title">⏳ 處理中...</h3>
<div class="progress-wrap">
  <div class="progress-bar" id="av-prog-bar" style="width:0%"></div>
  <span class="progress-text" id="av-prog-text">0%</span>
</div>
<div id="av-prog-msg" style="font-size:.88em;color:var(--txt2);margin-top:8px"></div>
<div id="av-log" style="max-height:200px;overflow-y:auto;font-family:Consolas,monospace;font-size:.78em;line-height:1.8;padding:8px;background:#F9FAFB;border-radius:8px;margin-top:10px"></div>
</div>

<div id="av-result-card" class="card" style="display:none">
<h3>🎉 生成完成</h3>
<div id="av-result-info" style="margin-bottom:12px"></div>
<div id="av-result-video" style="text-align:center"></div>
</div>

</div><!-- /tab-autovid -->

<!-- 圖庫彈窗 -->
<div class="modal" id="gallery-modal">
<div class="modal-box">
<div class="modal-head"><h3 id="gallery-title">圖庫</h3><button class="modal-close" onclick="closeModal('gallery-modal')">&times;</button></div>
<div class="modal-body"><div class="gallery" id="gallery-grid"></div></div>
</div>
</div>

<!-- 批次彈窗 -->
<div class="modal" id="batch-modal">
<div class="modal-box" style="max-width:480px">
<div class="modal-head"><h3>批次下載</h3><button class="modal-close" onclick="closeModal('batch-modal')">&times;</button></div>
<div class="modal-body">
<p style="margin-bottom:10px;font-size:.88em;color:var(--txt2)">每行輸入一個 Photo 名稱：</p>
<textarea id="batch-names" placeholder="Taylor Swift&#10;IU&#10;周杰倫"></textarea>
<div style="margin-top:14px;text-align:right">
<button class="btn btn-pri" onclick="startBatch()">開始批次下載</button>
</div>
</div>
</div>
</div>

<!-- Lightbox -->
<div class="lightbox" id="lightbox" onclick="this.classList.remove('show')">
<img id="lightbox-img">
</div>

<script>
// ── Tab 切換 ──────────────────────────────────────────
function switchTab(tab){
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.querySelector('.tab-btn[onclick*="'+tab+'"]').classList.add('active');
  document.getElementById('tab-'+tab).classList.add('active');
  if(tab==='yt') ytLoadDownloads();
}

let currentTask = null;
let eventSource = null;

async function startDownload() {
  const name = document.getElementById('inp-name').value.trim();
  if (!name) return alert('請輸入Photo 名稱');
  const body = getOpts(name);
  setUI(true);
  clearLog();
  document.getElementById('progress-card').style.display = '';
  try {
    const r = await fetch('/api/download', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    const d = await r.json();
    if (d.error) { alert(d.error); setUI(false); return; }
    currentTask = d.task_id;
    listenSSE(d.task_id);
  } catch(e) { alert('連線失敗: ' + e); setUI(false); }
}

async function startBatch() {
  const names = document.getElementById('batch-names').value.trim();
  if (!names) return alert('請輸入至少一個名稱');
  closeModal('batch-modal');
  const body = getOpts('');
  body.names = names;
  setUI(true);
  clearLog();
  document.getElementById('progress-card').style.display = '';
  try {
    const r = await fetch('/api/batch', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    const d = await r.json();
    if (d.error) { alert(d.error); setUI(false); return; }
    currentTask = d.task_id;
    listenSSE(d.task_id);
  } catch(e) { alert('連線失敗: ' + e); setUI(false); }
}

function getOpts(celebrity) {
  const sources = [];
  if (document.getElementById('src-bing').checked) sources.push('bing');
  if (document.getElementById('src-google').checked) sources.push('google');
  if (document.getElementById('src-ddg').checked) sources.push('duckduckgo');
  if (document.getElementById('src-pinterest').checked) sources.push('pinterest');
  if (!sources.length) sources.push('bing');
  return {
    celebrity,
    sources,
    count: parseInt(document.getElementById('inp-count').value) || 100,
    size: document.getElementById('sel-size').value,
    append_photo: document.getElementById('chk-photo').checked,
    face_only: document.getElementById('chk-face').checked,
    dedup_url: document.getElementById('chk-url').checked,
    dedup_md5: document.getElementById('chk-md5').checked,
    dedup_phash: document.getElementById('chk-phash').checked,
    auto_keyword: document.getElementById('chk-autokw').checked,
  };
}

function listenSSE(taskId) {
  if (eventSource) eventSource.close();
  eventSource = new EventSource('/api/progress/' + taskId);
  eventSource.onmessage = (e) => {
    const d = JSON.parse(e.data);
    switch(d.type) {
      case 'log':
        addLog(d.msg, d.tag || 'info');
        break;
      case 'progress':
        updateProgress(d);
        break;
      case 'batch_one_done':
        addLog('━━ ' + (d.celebrity||'') + ' 完成（新增 ' + (d.stats&&d.stats.downloaded||0) + ' 張）━━', 'info');
        loadCelebs();
        break;
      case 'done':
        eventSource.close(); eventSource = null;
        addLog('━━━ 全部完成 ━━━', 'info');
        setUI(false);
        loadCelebs();
        break;
      case 'error':
        addLog('錯誤: ' + d.msg, 'error');
        eventSource.close(); eventSource = null;
        setUI(false);
        break;
      case 'heartbeat': break;
    }
  };
  eventSource.onerror = () => {
    eventSource.close(); eventSource = null;
    setUI(false);
  };
}

function stopDownload() {
  if (currentTask) fetch('/api/stop/' + currentTask, {method:'POST'});
}

function updateProgress(d) {
  const pct = d.total ? (d.current / d.total * 100) : 0;
  document.getElementById('prog-bar').style.width = pct + '%';
  document.getElementById('prog-text').textContent = d.current + '/' + d.total;
  if (d.stats) {
    document.getElementById('st-dl').textContent = d.stats.downloaded || 0;
    document.getElementById('st-url').textContent = d.stats.skip_url || 0;
    document.getElementById('st-md5').textContent = d.stats.skip_md5 || 0;
    document.getElementById('st-ph').textContent = d.stats.skip_phash || 0;
    document.getElementById('st-fail').textContent = d.stats.failed || 0;
  }
}

function addLog(msg, tag) {
  const log = document.getElementById('log');
  const ts = new Date().toTimeString().slice(0,8);
  const line = document.createElement('div');
  line.className = tag;
  line.textContent = '[' + ts + '] ' + msg;
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}

function clearLog() {
  document.getElementById('log').innerHTML = '';
  document.getElementById('prog-bar').style.width = '0%';
  document.getElementById('prog-text').textContent = '0/0';
  ['st-dl','st-url','st-md5','st-ph','st-fail'].forEach(id => document.getElementById(id).textContent = '0');
}

function setUI(downloading) {
  document.getElementById('btn-start').disabled = downloading;
  document.getElementById('btn-stop').disabled = !downloading;
  document.getElementById('inp-name').disabled = downloading;
}

async function loadCelebs() {
  try {
    const r = await fetch('/api/celebrities');
    const data = await r.json();
    const tbody = document.getElementById('celeb-list');
    tbody.innerHTML = '';
    let total = 0;
    data.forEach(c => {
      total += c.count;
      const tr = document.createElement('tr');
      tr.innerHTML = '<td class="cel-name" onclick="openGallery(\'' + c.name.replace(/'/g,"\\'") + '\')">'
        + c.name + '</td><td>' + c.count + '</td><td>' + c.last + '</td>';
      tbody.appendChild(tr);
    });
    document.getElementById('total-bar').textContent =
      '共 ' + data.length + ' 個 Photo，' + total + ' 張照片';
    // 更新影片區的Photo 下拉
    updateVideoCelebDropdown(data);
    // 更新合併下拉
    updateMergeDropdowns(data);
  } catch(e) {}
}

async function openGallery(name) {
  document.getElementById('gallery-title').textContent = name + ' — 圖庫';
  const grid = document.getElementById('gallery-grid');
  grid.innerHTML = '<p style="color:var(--txt2)">載入中...</p>';
  document.getElementById('gallery-modal').classList.add('show');
  try {
    const r = await fetch('/api/images/' + encodeURIComponent(name));
    const imgs = await r.json();
    grid.innerHTML = '';
    if (!imgs.length) { grid.innerHTML = '<p style="color:var(--txt2)">尚無照片</p>'; return; }
    imgs.forEach(img => {
      const div = document.createElement('div');
      div.innerHTML = '<img src="' + img.url + '" loading="lazy" onclick="showLight(this.src)">'
        + '<div class="gallery-info">' + img.filename + ' (' + img.size_kb + 'KB)</div>';
      grid.appendChild(div);
    });
  } catch(e) { grid.innerHTML = '<p style="color:var(--err)">載入失敗</p>'; }
}

function showLight(src) {
  document.getElementById('lightbox-img').src = src;
  document.getElementById('lightbox').classList.add('show');
}

function showBatch() { document.getElementById('batch-modal').classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }

// 初始化
loadCelebs();
fetch('/api/stats').then(r=>r.json()).then(d=>{
  if(!d.has_imagehash) document.getElementById('chk-phash').disabled=true;
});

// Enter 鍵觸發
document.getElementById('inp-name').addEventListener('keydown', e=>{
  if(e.key==='Enter') startDownload();
});

// ── 影片生成 ─────────────────────────────────────────
const VID_TEMPLATES = {
  velocity:    {name:"🔥 Velocity 節拍變速",  desc:"慢推→瞬間閃切＋動態模糊，TikTok fan edit 經典風格", defDur:1.5},
  parallax_3d: {name:"📐 3D 視差效果",        desc:"前景人物浮起＋背景反向移動，CapCut 爆紅 3D Zoom", defDur:2.5},
  film_vhs:    {name:"🎞️ 底片 / VHS 復古",    desc:"膠片顆粒＋光暈漏光＋掃描線＋復古色調", defDur:2.5},
  rgb_glitch:  {name:"💫 RGB 色散故障",        desc:"色版偏移閃爍＋故障條紋＋高對比，時尚雜誌質感", defDur:1.2},
  cinema:      {name:"🎬 電影預告片",          desc:"寬銀幕黑邊＋青橙調色＋打字字幕，電影主角感", defDur:3.0},
  heartbeat:   {name:"❤️ 心跳脈動",            desc:"照片跟節奏放大縮小＋暗角呼吸＋白閃切換", defDur:1.5},
};

(function initVideoSection(){
  const sel = document.getElementById('vid-template');
  sel.innerHTML = '';
  Object.entries(VID_TEMPLATES).forEach(([id,t])=>{
    const opt = document.createElement('option');
    opt.value = id; opt.textContent = t.name;
    sel.appendChild(opt);
  });
  showTemplateDesc();
})();

function showTemplateDesc(){
  const id = document.getElementById('vid-template').value;
  const t = VID_TEMPLATES[id];
  const el = document.getElementById('template-desc');
  if(t){
    el.textContent = t.desc; el.style.display = '';
    // 切模板時重設秒數到該模板預設值
    const slider = document.getElementById('vid-duration');
    slider.value = t.defDur;
    updateDurLabel();
  } else{el.style.display = 'none';}
}

function updateDurLabel(){
  const v = parseFloat(document.getElementById('vid-duration').value);
  const maxPhotos = parseInt(document.getElementById('vid-max-photos').value);
  const total = (v * maxPhotos).toFixed(1);
  document.getElementById('vid-dur-label').textContent = v.toFixed(1) + '秒 (≈' + total + '秒)';
}

function updateVideoCelebDropdown(celebs){
  const sel = document.getElementById('vid-celeb');
  const prev = sel.value;
  sel.innerHTML = '<option value="">-- 選擇 Photo --</option>';
  celebs.forEach(c=>{
    const opt = document.createElement('option');
    opt.value = c.name;
    opt.textContent = c.name + ' (' + c.count + '張)';
    sel.appendChild(opt);
  });
  if(prev) sel.value = prev;
}

function onVidCelebChange(){
  const name = document.getElementById('vid-celeb').value;
  if(name) loadVideoHistory(name);
}

let videoES = null;
let previewedPhotos = []; // [{filename, path, thumb_url}, ...]

async function previewPhotos(){
  const celebrity = document.getElementById('vid-celeb').value;
  if(!celebrity) return alert('請選擇 Photo');
  const maxPhotos = parseInt(document.getElementById('vid-max-photos').value);
  const shuffle = document.getElementById('vid-shuffle').checked;

  document.getElementById('btn-preview-photos').disabled = true;
  document.getElementById('btn-preview-photos').textContent = '⏳ 挑選中...';
  document.getElementById('photo-preview-area').style.display = 'none';
  document.getElementById('btn-gen-video').style.display = 'none';

  try{
    const r = await fetch('/api/preview-photos',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, max_photos:maxPhotos, shuffle})
    });
    const d = await r.json();
    if(d.error){alert(d.error); return;}
    previewedPhotos = d.photos || [];
    renderPreviewGrid();
    document.getElementById('photo-preview-area').style.display = '';
    document.getElementById('btn-gen-video').style.display = '';
  }catch(e){
    alert('連線失敗: '+e);
  }finally{
    document.getElementById('btn-preview-photos').disabled = false;
    document.getElementById('btn-preview-photos').textContent = '👁 預覽照片';
  }
}

function renderPreviewGrid(){
  const grid = document.getElementById('photo-preview-grid');
  grid.innerHTML = '';
  previewedPhotos.forEach((p,i)=>{
    const cell = document.createElement('div');
    cell.className = 'preview-cell';
    cell.dataset.idx = i;
    let html = '<img src="'+p.thumb_url+'" alt="'+p.filename+'" loading="lazy">'
      +'<a class="dl-btn" href="'+p.thumb_url+'" download="'+p.filename+'" title="下載" onclick="markSingleDownload(event,'+i+')">⬇</a>'
      +'<button class="x-btn" onclick="removePreviewPhoto('+i+')" title="移除此照片">✕</button>';
    if(p.used_count && p.used_count > 0){
      html += '<span class="used-badge">用過'+p.used_count+'次</span>';
    }
    cell.innerHTML = html;
    grid.appendChild(cell);
  });
  document.getElementById('preview-count').textContent = '已選 '+previewedPhotos.length+' 張照片';
}

function markSingleDownload(e, idx){
  e.stopPropagation();
  const celebrity = document.getElementById('vid-celeb').value;
  const p = previewedPhotos[idx];
  if(celebrity && p){
    fetch('/api/mark-photo-used',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, filenames:[p.filename], usage_type:'download'})
    }).catch(()=>{});
  }
}

async function downloadAllPhotos(){
  const celebrity = document.getElementById('vid-celeb').value;
  if(!celebrity || !previewedPhotos.length) return;
  const filenames = previewedPhotos.map(p=>p.filename);
  try{
    const r = await fetch('/api/download-photos-zip',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, filenames})
    });
    if(!r.ok){alert('下載失敗'); return;}
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = celebrity+'_photos.zip';
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }catch(e){alert('下載失敗: '+e);}
}

async function removePreviewPhoto(idx){
  const celebrity = document.getElementById('vid-celeb').value;
  const cell = document.querySelector('.preview-cell[data-idx="'+idx+'"]');
  if(!cell || cell.classList.contains('replacing')) return;
  cell.classList.add('replacing');

  const removedFilename = previewedPhotos[idx].filename;
  const exclude = previewedPhotos.map(p=>p.filename);

  try{
    const r = await fetch('/api/replace-photo',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, exclude, removed_filename: removedFilename})
    });
    const d = await r.json();
    if(d.photo){
      previewedPhotos[idx] = d.photo;
    } else {
      // No replacement available, just remove
      previewedPhotos.splice(idx, 1);
    }
    renderPreviewGrid();
  }catch(e){
    cell.classList.remove('replacing');
    alert('替換失敗: '+e);
  }
}

async function confirmGenerate(){
  if(!previewedPhotos.length) return alert('請先預覽照片');
  const celebrity = document.getElementById('vid-celeb').value;
  if(!celebrity) return alert('請選擇 Photo');
  const template = document.getElementById('vid-template').value;
  const maxPhotos = parseInt(document.getElementById('vid-max-photos').value);
  const shuffle = document.getElementById('vid-shuffle').checked;
  const durPerPhoto = parseFloat(document.getElementById('vid-duration').value);
  const selectedPhotos = previewedPhotos.map(p=>p.filename);

  document.getElementById('btn-gen-video').disabled = true;
  document.getElementById('vid-progress-area').style.display = '';
  document.getElementById('vid-result').style.display = 'none';
  document.getElementById('vid-prog-bar').style.width = '0%';
  document.getElementById('vid-prog-text').textContent = '0%';
  document.getElementById('vid-prog-msg').textContent = '準備中...';
  document.getElementById('vid-prog-msg').style.color = 'var(--txt2)';

  try{
    const r = await fetch('/api/generate-video',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, template, max_photos:maxPhotos, shuffle, dur_per_photo:durPerPhoto, selected_photos:selectedPhotos})
    });
    const d = await r.json();
    if(d.error){alert(d.error); document.getElementById('btn-gen-video').disabled=false; return;}
    listenVideoSSE(d.task_id);
  }catch(e){
    alert('連線失敗: '+e);
    document.getElementById('btn-gen-video').disabled=false;
  }
}

function listenVideoSSE(taskId){
  if(videoES) videoES.close();
  videoES = new EventSource('/api/video-progress/'+taskId);
  videoES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        document.getElementById('vid-prog-bar').style.width = pct+'%';
        document.getElementById('vid-prog-text').textContent = pct+'%';
        document.getElementById('vid-prog-msg').textContent = d.message||'';
        break;
      case 'done':
        videoES.close(); videoES=null;
        document.getElementById('btn-gen-video').disabled=false;
        document.getElementById('vid-progress-area').style.display='none';
        showVideoResult(d);
        loadVideoHistory(document.getElementById('vid-celeb').value);
        break;
      case 'error':
        videoES.close(); videoES=null;
        document.getElementById('btn-gen-video').disabled=false;
        document.getElementById('vid-prog-msg').textContent = '錯誤: '+d.message;
        document.getElementById('vid-prog-msg').style.color = 'var(--err)';
        break;
      case 'heartbeat': break;
    }
  };
  videoES.onerror = ()=>{
    videoES.close(); videoES=null;
    document.getElementById('btn-gen-video').disabled=false;
  };
}

function showVideoResult(d){
  document.getElementById('vid-result').style.display='';
  const player = document.getElementById('vid-player');
  player.src = d.video_url;
  document.getElementById('vid-result-info').textContent =
    '時長: '+(d.duration||0)+'秒 | 大小: '+(d.file_size_mb||0)+'MB';
  document.getElementById('vid-download-link').href = d.video_url;
}

async function loadVideoHistory(celebrity){
  if(!celebrity) return;
  try{
    const r = await fetch('/api/videos/'+encodeURIComponent(celebrity));
    const videos = await r.json();
    const container = document.getElementById('vid-history');
    const list = document.getElementById('vid-history-list');
    if(!videos.length){container.style.display='none'; return;}
    container.style.display='';
    list.innerHTML='';
    videos.forEach(v=>{
      const div = document.createElement('div');
      div.className='vid-hist-item';
      div.innerHTML='<a href="'+v.url+'" target="_blank">▶ '+v.filename+'</a>'
        +'<span class="meta">'+v.size_mb+'MB | '+v.created+'</span>';
      list.appendChild(div);
    });
  }catch(e){}
}

// ── 別名 / 合併管理 ───────────────────────────────────
function updateMergeDropdowns(celebs){
  ['merge-src','merge-dst','dedup-celeb'].forEach(id=>{
    const sel = document.getElementById(id);
    if(!sel) return;
    const prev = sel.value;
    const labels = {'merge-src':'-- 來源（被合併）--','merge-dst':'-- 目標（保留）--','dedup-celeb':'-- 選擇 Photo --'};
    sel.innerHTML = '<option value="">'+(labels[id]||'--')+'</option>';
    celebs.forEach(c=>{
      const opt = document.createElement('option');
      opt.value = c.name;
      opt.textContent = c.name + ' (' + c.count + '張)';
      sel.appendChild(opt);
    });
    if(prev) sel.value = prev;
  });
}

async function mergeCelebrity(){
  const src = document.getElementById('merge-src').value;
  const dst = document.getElementById('merge-dst').value;
  if(!src||!dst) return alert('請選擇來源和目標');
  if(src===dst) return alert('來源和目標不能相同');
  if(!confirm('確定要把「'+src+'」合併到「'+dst+'」嗎？\n來源資料夾會被刪除，此操作不可逆。')) return;
  try{
    const r = await fetch('/api/merge-celebrity',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({source:src, target:dst})
    });
    const d = await r.json();
    if(d.error){alert(d.error); return;}
    alert(d.msg);
    loadCelebs();
    loadAliases();
  }catch(e){alert('合併失敗: '+e);}
}

async function dedupCelebrity(){
  const celebrity = document.getElementById('dedup-celeb').value;
  if(!celebrity) return alert('請選擇 Photo');
  const el = document.getElementById('dedup-result');
  el.textContent = '去重中...';
  try{
    const r = await fetch('/api/dedup-celebrity',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity})
    });
    const d = await r.json();
    if(d.error){el.textContent = d.error; return;}
    el.textContent = d.msg;
    loadCelebs();
  }catch(e){el.textContent = '失敗: '+e;}
}

async function addAlias(){
  const canonical = document.getElementById('alias-canonical').value.trim();
  const alias = document.getElementById('alias-name').value.trim();
  if(!canonical||!alias) return alert('請輸入正式名稱和別名');
  try{
    const r = await fetch('/api/alias',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({canonical, alias})
    });
    const d = await r.json();
    if(d.error){alert(d.error); return;}
    document.getElementById('alias-canonical').value = '';
    document.getElementById('alias-name').value = '';
    loadAliases();
  }catch(e){alert('新增失敗: '+e);}
}

async function removeAlias(alias){
  if(!confirm('移除別名「'+alias+'」？')) return;
  try{
    await fetch('/api/alias',{
      method:'DELETE', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({alias})
    });
    loadAliases();
  }catch(e){}
}

async function loadAliases(){
  try{
    const r = await fetch('/api/aliases');
    const list = await r.json();
    const el = document.getElementById('alias-list');
    if(!list.length){el.innerHTML='<span style="color:var(--txt2)">尚無別名</span>'; return;}
    el.innerHTML = list.map(a=>
      '<span style="display:inline-flex;align-items:center;gap:4px;margin:3px 6px 3px 0;padding:3px 8px;background:var(--card);border:1px solid var(--border);border-radius:12px">'
      +'<b>'+a.alias+'</b> → '+a.canonical
      +' <button onclick="removeAlias(\''+a.alias.replace(/'/g,"\\'")+'\')" style="border:none;background:none;color:var(--err);cursor:pointer;font-size:.9em;padding:0 2px">✕</button>'
      +'</span>'
    ).join('');
  }catch(e){}
}

// 初始載入別名
loadAliases();

// ═══════════ YouTube 下載 ═══════════════════════════════
let ytES = null;

function _fmtDuration(s){
  if(!s) return '';
  const m = Math.floor(s/60), ss = s%60;
  if(m>=60){const h=Math.floor(m/60); return h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0');}
  return m+':'+String(ss).padStart(2,'0');
}
function _fmtViews(n){
  if(!n) return '';
  if(n>=1e6) return (n/1e6).toFixed(1)+'M';
  if(n>=1e3) return (n/1e3).toFixed(1)+'K';
  return n+'';
}

function onPlatformChange(){
  const p = document.getElementById('yt-platform').value;
  const typeRow = document.getElementById('yt-type-row');
  const inp = document.getElementById('yt-query');
  if(p === 'tiktok'){
    typeRow.style.display = 'none';
    inp.placeholder = '輸入關鍵字搜尋、貼上 TikTok 網址、或 @用戶名';
  } else {
    typeRow.style.display = '';
    inp.placeholder = '輸入關鍵字或貼上 YouTube / TikTok 網址';
  }
}

async function ytSearch(){
  const query = document.getElementById('yt-query').value.trim();
  if(!query) return alert('請輸入搜尋關鍵字或網址');
  const maxResults = parseInt(document.getElementById('yt-max-results').value);
  const videoType = document.getElementById('yt-video-type').value;
  let platform = document.getElementById('yt-platform').value;
  // 自動偵測 URL 平台
  if(query.startsWith('http')){
    if(query.includes('tiktok.com')) platform = 'tiktok';
    else if(query.includes('youtube.com')||query.includes('youtu.be')) platform = 'youtube';
    document.getElementById('yt-platform').value = platform;
    onPlatformChange();
  }
  const btn = document.getElementById('btn-yt-search');
  const status = document.getElementById('yt-search-status');
  btn.disabled = true;
  status.textContent = '搜尋中...';
  document.getElementById('yt-results-card').style.display = 'none';

  try{
    const r = await fetch('/api/yt/search',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({query, max_results:maxResults, video_type:videoType, platform})
    });
    const d = await r.json();
    if(d.error){status.textContent = '錯誤: '+d.error; return;}
    const results = d.results||[];
    let msg = '找到 '+results.length+' 個結果';
    if(d.hint) msg += ' — '+d.hint;
    status.textContent = msg;
    renderYtResults(results);
  }catch(e){
    status.textContent = '搜尋失敗: '+e;
  }finally{
    btn.disabled = false;
  }
}

// Store search results for download reference
let _ytResults = [];

function renderYtResults(results){
  _ytResults = results;
  const container = document.getElementById('yt-results');
  const card = document.getElementById('yt-results-card');
  const batchBar = document.getElementById('yt-batch-bar');
  container.innerHTML = '';
  batchBar.style.display = 'none';
  document.getElementById('yt-select-all').checked = false;
  if(!results.length){card.style.display='none'; return;}
  card.style.display = '';

  results.forEach((v, idx)=>{
    const div = document.createElement('div');
    div.className = 'yt-item';
    const durStr = _fmtDuration(v.duration);
    const viewStr = _fmtViews(v.view_count);
    const safeTitle = (v.title||'').replace(/</g,'&lt;');
    div.innerHTML = '<input type="checkbox" class="yt-item-cb" data-cb-idx="'+idx+'" onchange="ytUpdateBatch()">'
      +'<img src="'+(v.thumbnail||'')+'" alt="" onerror="this.style.display=\'none\'">'
      +'<div class="yt-item-info">'
      +'<div class="yt-item-title">'+safeTitle+'</div>'
      +'<div class="yt-item-meta">'+(v.channel||'')+' · '+(durStr?durStr+' · ':'')+viewStr+' 次觀看</div>'
      +'<div class="yt-item-actions">'
      +'<button class="btn btn-pri" style="font-size:.8em;padding:4px 12px" data-yt-idx="'+idx+'">⬇ 下載</button>'
      +'<button class="btn" style="font-size:.8em;padding:4px 12px;background:var(--ok);color:#fff" data-yt-extract-idx="'+idx+'">🎯 下載+擷取</button>'
      +'<a href="'+(v.url||'')+'" target="_blank" style="font-size:.8em;color:var(--pri);text-decoration:none;padding:4px 8px">▶ 開啟</a>'
      +'</div></div>';
    container.appendChild(div);
  });
  // Event delegation for download & extract buttons
  container.onclick = (e)=>{
    if(e.target.classList.contains('yt-item-cb')) return; // let checkbox handle itself
    const dlBtn = e.target.closest('[data-yt-idx]');
    const exBtn = e.target.closest('[data-yt-extract-idx]');
    if(dlBtn && !dlBtn.dataset.ytExtractIdx){
      const idx = parseInt(dlBtn.dataset.ytIdx);
      const v = _ytResults[idx];
      if(!v) return;
      const quality = document.getElementById('yt-quality').value;
      const keyword = document.getElementById('yt-query').value.trim();
      ytDownload(v.url, v.title, quality, keyword);
    } else if(exBtn){
      const idx = parseInt(exBtn.dataset.ytExtractIdx);
      const v = _ytResults[idx];
      if(!v) return;
      const quality = document.getElementById('yt-quality').value;
      const keyword = document.getElementById('yt-query').value.trim();
      if(!keyword){ alert('搜尋關鍵字將作為擷取人物名稱，請確認搜尋欄有輸入'); return; }
      ytDownloadAndExtract(v.url, v.title, quality, keyword);
    }
  };
}

function ytUpdateBatch(){
  const cbs = document.querySelectorAll('.yt-item-cb');
  const checked = document.querySelectorAll('.yt-item-cb:checked');
  const bar = document.getElementById('yt-batch-bar');
  const allCb = document.getElementById('yt-select-all');
  bar.style.display = checked.length > 0 ? 'flex' : 'none';
  document.getElementById('yt-batch-count').textContent = '已選 '+checked.length+' 部';
  allCb.checked = checked.length === cbs.length && cbs.length > 0;
  // highlight checked items
  cbs.forEach(cb=>{
    const item = cb.closest('.yt-item');
    if(cb.checked) item.classList.add('yt-checked');
    else item.classList.remove('yt-checked');
  });
}

function ytToggleAll(on){
  document.querySelectorAll('.yt-item-cb').forEach(cb=>{ cb.checked = on; });
  ytUpdateBatch();
}

let _ytBatchQueue = [];
let _ytBatchMode = '';
let _ytBatchDone = 0;
let _ytBatchTotal = 0;

function ytBatchAction(mode){
  const checked = document.querySelectorAll('.yt-item-cb:checked');
  if(!checked.length) return;
  const keyword = document.getElementById('yt-query').value.trim();
  if(mode === 'extract' && !keyword){
    alert('搜尋關鍵字將作為擷取人物名稱，請確認搜尋欄有輸入');
    return;
  }
  const quality = document.getElementById('yt-quality').value;
  const indices = Array.from(checked).map(cb => parseInt(cb.dataset.cbIdx));
  _ytBatchQueue = indices.map(i => _ytResults[i]).filter(Boolean);
  _ytBatchMode = mode;
  _ytBatchDone = 0;
  _ytBatchTotal = _ytBatchQueue.length;

  // Show progress
  const progCard = document.getElementById('yt-progress-card');
  progCard.style.display = '';
  document.getElementById('yt-dl-title').textContent = '📦 批量處理 0/'+_ytBatchTotal;
  document.getElementById('yt-prog-bar').style.width = '0%';
  document.getElementById('yt-prog-text').textContent = '0%';
  document.getElementById('yt-prog-msg').textContent = '準備中...';
  document.getElementById('yt-prog-msg').style.color = 'var(--txt2)';
  progCard.scrollIntoView({behavior:'smooth', block:'center'});

  _ytBatchNext(quality, keyword);
}

function _ytBatchNext(quality, keyword){
  if(_ytBatchDone >= _ytBatchTotal){
    document.getElementById('yt-dl-title').textContent = '📦 批量處理完成';
    document.getElementById('yt-prog-bar').style.width = '100%';
    document.getElementById('yt-prog-text').textContent = '100%';
    document.getElementById('yt-prog-msg').innerHTML = '✅ 全部 '+_ytBatchTotal+' 部處理完成！';
    ytLoadDownloads();
    return;
  }
  const v = _ytBatchQueue[_ytBatchDone];
  const num = (_ytBatchDone+1)+'/'+_ytBatchTotal;
  document.getElementById('yt-dl-title').textContent = '📦 批量處理 '+num+'：'+v.title;

  if(_ytBatchMode === 'extract'){
    _ytBatchDownloadAndExtract(v, quality, keyword);
  } else {
    _ytBatchDownloadOne(v, quality, keyword);
  }
}

async function _ytBatchDownloadOne(v, quality, keyword){
  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url:v.url, title:v.title, quality, keyword})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ '+v.title+': '+d.error;
      _ytBatchDone++;
      _ytBatchUpdateBar();
      _ytBatchNext(quality, keyword);
      return;
    }
    _ytBatchListenDL(d.task_id, quality, keyword, false);
  }catch(e){
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword);
  }
}

function _ytBatchListenDL(taskId, quality, keyword, chainExtract){
  if(ytES) ytES.close();
  ytES = new EventSource('/api/yt/progress/'+taskId);
  ytES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const base = (_ytBatchDone/_ytBatchTotal)*100;
        const slice = chainExtract ? 50 : 100;
        const pct = base + (d.percent||0)/100 * (slice/_ytBatchTotal);
        document.getElementById('yt-prog-bar').style.width = Math.round(pct)+'%';
        document.getElementById('yt-prog-text').textContent = Math.round(pct)+'%';
        document.getElementById('yt-prog-msg').textContent = (chainExtract?'下載：':'')+(d.message||'');
        break;
      case 'done':
        ytES.close(); ytES=null;
        if(chainExtract){
          document.getElementById('yt-prog-msg').textContent = '下載完成，開始擷取...';
          _ytBatchExtractOne(d.rel_path, keyword, quality);
        } else {
          _ytBatchDone++;
          _ytBatchUpdateBar();
          ytLoadDownloads();
          _ytBatchNext(quality, keyword);
        }
        break;
      case 'error':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-msg').textContent = '⚠️ 下載失敗：'+d.message;
        _ytBatchDone++;
        _ytBatchUpdateBar();
        _ytBatchNext(quality, keyword);
        break;
      case 'heartbeat': break;
    }
  };
  ytES.onerror = ()=>{
    ytES.close(); ytES=null;
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword);
  };
}

async function _ytBatchDownloadAndExtract(v, quality, keyword){
  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url:v.url, title:v.title, quality, keyword})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ '+v.title+': '+d.error;
      _ytBatchDone++;
      _ytBatchUpdateBar();
      _ytBatchNext(quality, keyword);
      return;
    }
    _ytBatchListenDL(d.task_id, quality, keyword, true);
  }catch(e){
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword);
  }
}

// ── 人物對應選擇 ──
let _mappingResolve = null;  // Promise resolve callback
let _mappingKeyword = '';

function _showMappingModal(keyword, folders, message){
  return new Promise((resolve)=>{
    _mappingResolve = resolve;
    _mappingKeyword = keyword;
    document.getElementById('mapping-msg').textContent = message || '找不到「'+keyword+'」的照片資料夾';
    const sel = document.getElementById('mapping-select');
    sel.innerHTML = '';
    // 把最可能的放前面（名稱包含關鍵字的部分）
    const kw = keyword.toLowerCase();
    const sorted = [...folders].sort((a,b)=>{
      const aMatch = kw.includes(a.toLowerCase()) || a.toLowerCase().includes(kw.split(' ')[kw.split(' ').length-1]);
      const bMatch = kw.includes(b.toLowerCase()) || b.toLowerCase().includes(kw.split(' ')[kw.split(' ').length-1]);
      if(aMatch && !bMatch) return -1;
      if(!aMatch && bMatch) return 1;
      return a.localeCompare(b);
    });
    sorted.forEach(f=>{
      const opt = document.createElement('option');
      opt.value = f; opt.textContent = f;
      sel.appendChild(opt);
    });
    document.getElementById('yt-mapping-modal').style.display = 'flex';
  });
}

function ytMappingCancel(){
  document.getElementById('yt-mapping-modal').style.display = 'none';
  if(_mappingResolve){ _mappingResolve(null); _mappingResolve = null; }
}

async function ytMappingConfirm(){
  const chosen = document.getElementById('mapping-select').value;
  document.getElementById('yt-mapping-modal').style.display = 'none';
  if(!chosen){ if(_mappingResolve){ _mappingResolve(null); _mappingResolve=null; } return; }
  // 加入別名
  try{
    await fetch('/api/alias',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({alias:_mappingKeyword, canonical:chosen})
    });
  }catch(e){}
  if(_mappingResolve){ _mappingResolve(chosen); _mappingResolve=null; }
}

async function _ytBatchExtractOne(relPath, keyword, quality){
  try{
    const r = await fetch('/api/yt/extract',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({filename:relPath, person:keyword})
    });
    const d = await r.json();
    // 找不到照片資料夾 → 彈出選擇對話框
    if(d.need_mapping){
      const chosen = await _showMappingModal(d.keyword, d.available_folders, d.message);
      if(!chosen){
        document.getElementById('yt-prog-msg').textContent = '⏭️ 已跳過擷取';
        _ytBatchDone++;
        _ytBatchUpdateBar();
        _ytBatchNext(quality, keyword);
        return;
      }
      // 用選擇的人物重新擷取
      const r2 = await fetch('/api/yt/extract',{
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({filename:relPath, person:chosen})
      });
      const d2 = await r2.json();
      if(d2.error){
        document.getElementById('yt-prog-msg').textContent = '⚠️ 擷取失敗：'+d2.error;
        _ytBatchDone++;
        _ytBatchUpdateBar();
        _ytBatchNext(quality, keyword);
        return;
      }
      _ytListenExtract(d2.task_id, quality, keyword);
      return;
    }
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ 擷取失敗：'+d.error;
      _ytBatchDone++;
      _ytBatchUpdateBar();
      _ytBatchNext(quality, keyword);
      return;
    }
    _ytListenExtract(d.task_id, quality, keyword);
  }catch(e){
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword);
  }
}

function _ytListenExtract(taskId, quality, keyword){
  const es = new EventSource('/api/yt/extract-progress/'+taskId);
  es.onmessage = (ev)=>{
    const dd = JSON.parse(ev.data);
    switch(dd.type){
      case 'progress':
        const base = (_ytBatchDone/_ytBatchTotal)*100;
        const pct = base + (50 + (dd.percent||0)/2) / _ytBatchTotal;
        document.getElementById('yt-prog-bar').style.width = Math.round(pct)+'%';
        document.getElementById('yt-prog-text').textContent = Math.round(pct)+'%';
        document.getElementById('yt-prog-msg').textContent = '擷取：'+(dd.message||'');
        break;
      case 'done':
        es.close();
        _ytBatchDone++;
        _ytBatchUpdateBar();
        ytLoadDownloads();
        _ytBatchNext(quality, keyword);
        break;
      case 'error':
        es.close();
        document.getElementById('yt-prog-msg').textContent = '⚠️ 擷取失敗：'+dd.message;
        _ytBatchDone++;
        _ytBatchUpdateBar();
        _ytBatchNext(quality, keyword);
        break;
      case 'heartbeat': break;
    }
  };
  es.onerror = ()=>{
    es.close();
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword);
  };
}

function _ytBatchUpdateBar(){
  const pct = Math.round((_ytBatchDone/_ytBatchTotal)*100);
  document.getElementById('yt-prog-bar').style.width = pct+'%';
  document.getElementById('yt-prog-text').textContent = pct+'%';
}

async function ytDownload(url, title, quality, keyword){
  const progCard = document.getElementById('yt-progress-card');
  progCard.style.display = '';
  document.getElementById('yt-dl-title').textContent = title;
  document.getElementById('yt-prog-bar').style.width = '0%';
  document.getElementById('yt-prog-text').textContent = '0%';
  document.getElementById('yt-prog-msg').textContent = '準備中...';
  document.getElementById('yt-prog-msg').style.color = 'var(--txt2)';
  progCard.scrollIntoView({behavior:'smooth', block:'center'});

  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url, title, quality, keyword})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '錯誤: '+d.error;
      document.getElementById('yt-prog-msg').style.color = 'var(--err)';
      return;
    }
    ytListenSSE(d.task_id);
  }catch(e){
    document.getElementById('yt-prog-msg').textContent = '連線失敗: '+e;
    document.getElementById('yt-prog-msg').style.color = 'var(--err)';
  }
}

function ytListenSSE(taskId){
  if(ytES) ytES.close();
  ytES = new EventSource('/api/yt/progress/'+taskId);
  ytES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        document.getElementById('yt-prog-bar').style.width = pct+'%';
        document.getElementById('yt-prog-text').textContent = pct+'%';
        document.getElementById('yt-prog-msg').textContent = d.message||'';
        break;
      case 'done':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-bar').style.width = '100%';
        document.getElementById('yt-prog-text').textContent = '100%';
        document.getElementById('yt-prog-msg').innerHTML =
          '✅ 完成！ <a href="'+d.file_url+'" target="_blank" style="color:var(--pri)">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        ytLoadDownloads();
        break;
      case 'error':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-msg').textContent = '❌ '+d.message;
        document.getElementById('yt-prog-msg').style.color = 'var(--err)';
        break;
      case 'heartbeat': break;
    }
  };
  ytES.onerror = ()=>{ ytES.close(); ytES=null; };
}

// ═══════════ 下載 + 自動擷取 ═══════════
async function ytDownloadAndExtract(url, title, quality, keyword){
  const progCard = document.getElementById('yt-progress-card');
  progCard.style.display = '';
  document.getElementById('yt-dl-title').textContent = '📥 ' + title;
  document.getElementById('yt-prog-bar').style.width = '0%';
  document.getElementById('yt-prog-text').textContent = '0%';
  document.getElementById('yt-prog-msg').textContent = '步驟 1/2：下載中...';
  document.getElementById('yt-prog-msg').style.color = 'var(--txt2)';
  progCard.scrollIntoView({behavior:'smooth', block:'center'});

  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url, title, quality, keyword})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '錯誤: '+d.error;
      document.getElementById('yt-prog-msg').style.color = 'var(--err)';
      return;
    }
    // Listen download SSE, then chain extract on done
    _chainDownloadThenExtract(d.task_id, keyword);
  }catch(e){
    document.getElementById('yt-prog-msg').textContent = '連線失敗: '+e;
    document.getElementById('yt-prog-msg').style.color = 'var(--err)';
  }
}

function _chainDownloadThenExtract(taskId, person){
  if(ytES) ytES.close();
  ytES = new EventSource('/api/yt/progress/'+taskId);
  ytES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round((d.percent||0) * 0.5);
        document.getElementById('yt-prog-bar').style.width = pct+'%';
        document.getElementById('yt-prog-text').textContent = pct+'%';
        document.getElementById('yt-prog-msg').textContent = '步驟 1/2 下載：'+(d.message||'');
        break;
      case 'done':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-bar').style.width = '50%';
        document.getElementById('yt-prog-text').textContent = '50%';
        document.getElementById('yt-prog-msg').textContent = '✅ 下載完成，開始擷取人物...';
        document.getElementById('yt-dl-title').textContent = '🎯 擷取中：' + (d.filename||'');
        ytLoadDownloads();
        // Chain: start extract
        _startExtractAfterDownload(d.rel_path, person);
        break;
      case 'error':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-msg').textContent = '❌ 下載失敗：'+d.message;
        document.getElementById('yt-prog-msg').style.color = 'var(--err)';
        break;
      case 'heartbeat': break;
    }
  };
  ytES.onerror = ()=>{ ytES.close(); ytES=null; };
}

async function _startExtractAfterDownload(relPath, person){
  try{
    const r = await fetch('/api/yt/extract',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({filename: relPath, person: person})
    });
    const d = await r.json();
    // 找不到照片資料夾 → 彈出選擇對話框
    if(d.need_mapping){
      const chosen = await _showMappingModal(d.keyword, d.available_folders, d.message);
      if(!chosen){
        document.getElementById('yt-prog-msg').textContent = '⏭️ 已跳過擷取';
        return;
      }
      const r2 = await fetch('/api/yt/extract',{
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({filename: relPath, person: chosen})
      });
      const d2 = await r2.json();
      if(d2.error){
        document.getElementById('yt-prog-msg').textContent = '⚠️ 擷取失敗：'+d2.error;
        document.getElementById('yt-prog-msg').style.color = 'var(--err)';
        return;
      }
      _chainExtractSSE(d2.task_id);
      return;
    }
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ 擷取失敗：'+d.error;
      document.getElementById('yt-prog-msg').style.color = 'var(--err)';
      return;
    }
    _chainExtractSSE(d.task_id);
  }catch(e){
    document.getElementById('yt-prog-msg').textContent = '擷取連線失敗: '+e;
    document.getElementById('yt-prog-msg').style.color = 'var(--err)';
  }
}

function _chainExtractSSE(taskId){
  const es = new EventSource('/api/yt/extract-progress/'+taskId);
  es.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = 50 + Math.round((d.percent||0) * 0.5);
        document.getElementById('yt-prog-bar').style.width = pct+'%';
        document.getElementById('yt-prog-text').textContent = pct+'%';
        document.getElementById('yt-prog-msg').textContent = '步驟 2/2 擷取：'+(d.message||'');
        break;
      case 'done':
        es.close();
        document.getElementById('yt-prog-bar').style.width = '100%';
        document.getElementById('yt-prog-text').textContent = '100%';
        document.getElementById('yt-prog-msg').innerHTML =
          '✅ 全部完成！找到 '+d.segments+' 個片段（'+d.duration+'秒）'
          +' <a href="'+d.file_url+'" target="_blank" style="color:var(--pri)">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        ytLoadDownloads();
        break;
      case 'error':
        es.close();
        document.getElementById('yt-prog-msg').textContent = '❌ 擷取失敗：'+d.message;
        document.getElementById('yt-prog-msg').style.color = 'var(--err)';
        break;
      case 'heartbeat': break;
    }
  };
  es.onerror = ()=>{ es.close(); };
}

async function ytLoadDownloads(){
  const el = document.getElementById('yt-downloads');
  try{
    const r = await fetch('/api/yt/downloads');
    const files = await r.json();
    if(!files.length){el.innerHTML='<span style="color:var(--txt2);font-size:.85em">尚無下載</span>'; return;}
    el.innerHTML = '';
    files.forEach(f=>{
      const isAudio = ['.mp3','.m4a','.opus'].includes(f.ext);
      const catIcon = f.category==='extracts' ? '🎯' : (isAudio ? '🎵' : '🎬');
      const personTag = f.person && f.person !== '_unsorted'
        ? '<span style="font-size:.7em;background:var(--bg2);padding:2px 6px;border-radius:3px;margin-right:4px">📁 '+f.person+'</span>'
        : '';
      const div = document.createElement('div');
      div.className = 'yt-dl-item';
      const relPath = (f.rel_path||f.filename).replace(/'/g,"\\'");
      div.innerHTML = '<span>'+catIcon+'</span>'
        +personTag
        +'<span class="fname" title="'+f.filename+'">'+f.filename+'</span>'
        +'<span style="color:var(--txt2);white-space:nowrap">'+f.size_mb+'MB · '+f.created+'</span>'
        +'<a href="'+f.url+'" target="_blank">▶ 播放</a>'
        +'<a href="'+f.url+'" download>⬇</a>'
        +(isAudio ? '' : '<button class="btn" style="font-size:.75em;padding:3px 10px" onclick="ytRotateVideo(\''+relPath+'\',this)">🔄 旋轉</button>')
        +(isAudio || f.category==='extracts' ? '' : '<button class="btn" style="font-size:.75em;padding:3px 10px" onclick="showExtract(\''+relPath+'\')">🎯 擷取人物</button>');
      el.appendChild(div);
    });
    hlLoadPersons();
  }catch(e){el.innerHTML='<span style="color:var(--err)">載入失敗</span>';}
}

// ═══════════ 影片旋轉 ═══════════════════════════════════
async function ytRotateVideo(relPath, btn){
  const choice = prompt('選擇旋轉方向：\n1) 順時針 90°\n2) 逆時針 90°\n3) 180°\n\n請輸入 1, 2, 或 3');
  if(!choice) return;
  const idx = parseInt(choice);
  if(idx < 1 || idx > 3){ alert('無效的選擇'); return; }
  const angle = [90, 270, 180][idx-1];
  const labels = ['順時針 90°','逆時針 90°','180°'];
  if(!confirm('確定要將影片旋轉 '+labels[idx-1]+'？\n（會覆蓋原始檔案，可能需要數分鐘）')) return;

  btn.disabled = true;
  const origText = btn.textContent;
  btn.textContent = '⏳ 旋轉中...';

  try{
    const r = await fetch('/api/yt/rotate',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rel_path:relPath, angle:angle})
    });
    const d = await r.json();
    if(d.error){
      alert('旋轉失敗: '+d.error);
    } else {
      alert('✅ 旋轉完成！('+d.size_mb+'MB)');
      ytLoadDownloads();
    }
  }catch(e){
    alert('旋轉失敗: '+e);
  }finally{
    btn.disabled = false;
    btn.textContent = origText;
  }
}

// ═══════════ 人臉辨識影片擷取 ═══════════════════════════
let extractES = null;

async function loadCelebrities(){
  try{
    const r = await fetch('/api/celebrities');
    const list = await r.json();
    const sel = document.getElementById('extract-person');
    sel.innerHTML = list.map(c=>{
      const name = typeof c === 'string' ? c : c.name;
      const count = c.count ? ' ('+c.count+'張)' : '';
      return '<option value="'+name+'">'+name+count+'</option>';
    }).join('');
  }catch(e){}
}

function showExtract(filename){
  const card = document.getElementById('yt-extract-card');
  card.style.display = '';
  document.getElementById('yt-extract-title').textContent = filename;
  card.dataset.filename = filename;
  document.getElementById('extract-prog-wrap').style.display = 'none';
  document.getElementById('extract-msg').textContent = '';
  document.getElementById('extract-msg').style.color = 'var(--txt2)';
  loadCelebrities();
  card.scrollIntoView({behavior:'smooth', block:'center'});
}

async function ytExtract(){
  const card = document.getElementById('yt-extract-card');
  const filename = card.dataset.filename;
  const person = document.getElementById('extract-person').value;
  if(!filename || !person) return alert('請選擇影片和人物');

  document.getElementById('btn-extract').disabled = true;
  document.getElementById('extract-prog-wrap').style.display = '';
  document.getElementById('extract-prog-bar').style.width = '0%';
  document.getElementById('extract-prog-text').textContent = '0%';
  document.getElementById('extract-msg').textContent = '啟動中...';
  document.getElementById('extract-msg').style.color = 'var(--txt2)';

  try{
    const r = await fetch('/api/yt/extract',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({filename, person})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('extract-msg').textContent = '錯誤: '+d.error;
      document.getElementById('extract-msg').style.color = 'var(--err)';
      document.getElementById('btn-extract').disabled = false;
      return;
    }
    extractListenSSE(d.task_id);
  }catch(e){
    document.getElementById('extract-msg').textContent = '連線失敗: '+e;
    document.getElementById('extract-msg').style.color = 'var(--err)';
    document.getElementById('btn-extract').disabled = false;
  }
}

function extractListenSSE(taskId){
  if(extractES) extractES.close();
  extractES = new EventSource('/api/yt/extract-progress/'+taskId);
  extractES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        document.getElementById('extract-prog-bar').style.width = pct+'%';
        document.getElementById('extract-prog-text').textContent = pct+'%';
        document.getElementById('extract-msg').textContent = d.message||'';
        break;
      case 'done':
        extractES.close(); extractES=null;
        document.getElementById('extract-prog-bar').style.width = '100%';
        document.getElementById('extract-prog-text').textContent = '100%';
        document.getElementById('extract-msg').innerHTML =
          '✅ 完成！找到 '+d.segments+' 個片段（'+d.duration+'秒）'
          +' <a href="'+d.file_url+'" target="_blank" style="color:var(--pri)">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        document.getElementById('btn-extract').disabled = false;
        ytLoadDownloads();
        break;
      case 'error':
        extractES.close(); extractES=null;
        document.getElementById('extract-msg').textContent = '❌ '+d.message;
        document.getElementById('extract-msg').style.color = 'var(--err)';
        document.getElementById('btn-extract').disabled = false;
        break;
      case 'heartbeat': break;
    }
  };
  extractES.onerror = ()=>{ extractES.close(); extractES=null;
    document.getElementById('btn-extract').disabled = false; };
}

// ═══════════ 精華影片 ═══════════════════════════════════
let hlES = null;

async function hlLoadPersons(){
  try{
    const r = await fetch('/api/yt/highlight-persons');
    const list = await r.json();
    const sel = document.getElementById('hl-person');
    if(!list.length){
      sel.innerHTML = '<option value="">（無可用影片）</option>';
      document.getElementById('hl-person-info').textContent = '請先擷取人物影片';
      return;
    }
    sel.innerHTML = list.map(p =>
      '<option value="'+p.name+'">'+p.name+' ('+p.video_count+'部)</option>'
    ).join('');
    hlLoadPersonInfo();
  }catch(e){}
}

async function hlLoadPersonInfo(){
  const sel = document.getElementById('hl-person');
  const info = document.getElementById('hl-person-info');
  const listDiv = document.getElementById('hl-video-list');
  const container = document.getElementById('hl-videos');
  if(!sel.value){
    listDiv.style.display = 'none';
    info.textContent = '';
    return;
  }
  // 載入該人物的影片清單
  try{
    const r = await fetch('/api/yt/highlight-videos/'+encodeURIComponent(sel.value));
    const videos = await r.json();
    if(!videos.length){
      listDiv.style.display = 'none';
      info.textContent = '此人物沒有擷取影片';
      return;
    }
    container.innerHTML = '';
    videos.forEach((v,i)=>{
      const durStr = v.duration ? (v.duration+'秒') : '';
      const div = document.createElement('div');
      div.style.cssText = 'display:flex;align-items:center;gap:6px;padding:4px 8px;background:var(--bg);border-radius:6px;font-size:.82em';
      div.innerHTML = '<input type="checkbox" class="hl-vid-cb" data-filename="'+v.filename+'" checked style="cursor:pointer" onchange="_hlUpdateInfo()">'
        +'<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+v.filename+'">'+v.filename+'</span>'
        +'<span style="color:var(--txt2);white-space:nowrap">'+v.size_mb+'MB'+(durStr?' · '+durStr:'')+'</span>'
        +'<a href="'+v.url+'" target="_blank" style="color:var(--pri);text-decoration:none;font-size:.9em">▶</a>';
      container.appendChild(div);
    });
    listDiv.style.display = '';
    document.getElementById('hl-select-all').checked = true;
    _hlUpdateInfo();
  }catch(e){
    info.textContent = '載入影片清單失敗';
    listDiv.style.display = 'none';
  }
}

function hlToggleAllVideos(on){
  document.querySelectorAll('.hl-vid-cb').forEach(cb=>{ cb.checked = on; });
  _hlUpdateInfo();
}

function _hlUpdateInfo(){
  const total = document.querySelectorAll('.hl-vid-cb').length;
  const checked = document.querySelectorAll('.hl-vid-cb:checked').length;
  const sel = document.getElementById('hl-person');
  const info = document.getElementById('hl-person-info');
  info.textContent = '將從 '+sel.value+' 的 '+checked+'/'+total+' 部擷取影片製作精華';
  document.getElementById('hl-select-all').checked = (checked === total);
}

async function ytHighlight(){
  const person = document.getElementById('hl-person').value;
  if(!person) return alert('請選擇人物');

  // 收集已勾選的影片
  const checkedCbs = document.querySelectorAll('.hl-vid-cb:checked');
  const allCbs = document.querySelectorAll('.hl-vid-cb');
  if(allCbs.length > 0 && checkedCbs.length === 0){
    return alert('請至少選擇一部影片');
  }
  const selectedVideos = allCbs.length > 0 && checkedCbs.length < allCbs.length
    ? Array.from(checkedCbs).map(cb => cb.dataset.filename)
    : null;  // null = 用全部

  const opts = {
    person,
    strategy: document.getElementById('hl-strategy').value,
    clip_duration: parseFloat(document.getElementById('hl-clip-dur').value),
    total_duration: parseFloat(document.getElementById('hl-total-dur').value),
    max_per_video: parseInt(document.getElementById('hl-max-per').value),
    transition: document.getElementById('hl-transition').value,
    transition_dur: parseFloat(document.getElementById('hl-xfade-val').value),
    resolution: document.getElementById('hl-resolution').value,
    audio_mode: document.getElementById('hl-audio-mode').value,
  };
  if(selectedVideos) opts.videos = selectedVideos;

  document.getElementById('btn-highlight').disabled = true;
  document.getElementById('hl-prog-wrap').style.display = '';
  document.getElementById('hl-prog-bar').style.width = '0%';
  document.getElementById('hl-prog-text').textContent = '0%';
  document.getElementById('hl-msg').textContent = '啟動中...';
  document.getElementById('hl-msg').style.color = 'var(--txt2)';

  try{
    const r = await fetch('/api/yt/highlight',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify(opts)
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('hl-msg').textContent = '錯誤: '+d.error;
      document.getElementById('hl-msg').style.color = 'var(--err)';
      document.getElementById('btn-highlight').disabled = false;
      return;
    }
    hlListenSSE(d.task_id);
  }catch(e){
    document.getElementById('hl-msg').textContent = '連線失敗: '+e;
    document.getElementById('hl-msg').style.color = 'var(--err)';
    document.getElementById('btn-highlight').disabled = false;
  }
}

function hlListenSSE(taskId){
  if(hlES) hlES.close();
  hlES = new EventSource('/api/yt/highlight-progress/'+taskId);
  hlES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        document.getElementById('hl-prog-bar').style.width = pct+'%';
        document.getElementById('hl-prog-text').textContent = pct+'%';
        document.getElementById('hl-msg').textContent = d.message||'';
        break;
      case 'done':
        hlES.close(); hlES=null;
        document.getElementById('hl-prog-bar').style.width = '100%';
        document.getElementById('hl-prog-text').textContent = '100%';
        document.getElementById('hl-msg').innerHTML =
          '✅ 完成！'+d.clips_count+' 個片段（'+d.duration+'秒）'
          +' <a href="'+d.file_url+'" target="_blank" style="color:var(--pri)">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        document.getElementById('btn-highlight').disabled = false;
        ytLoadDownloads();
        break;
      case 'error':
        hlES.close(); hlES=null;
        document.getElementById('hl-msg').textContent = '❌ '+d.message;
        document.getElementById('hl-msg').style.color = 'var(--err)';
        document.getElementById('btn-highlight').disabled = false;
        break;
      case 'heartbeat': break;
    }
  };
  hlES.onerror = ()=>{ hlES.close(); hlES=null;
    document.getElementById('btn-highlight').disabled = false; };
}

// 切換到 YouTube 分頁時載入精華人物列表
(function(){
  const origSwitch = window.switchTab;
  window.switchTab = function(tab){
    origSwitch(tab);
    if(tab === 'yt') hlLoadPersons();
  };
})();

// ═══════════ 一鍵影片生成 ═══════════════════════════════
let avES = null;

function _avGetResolution(){
  const orient = document.getElementById('av-orientation').value;
  const res = document.getElementById('av-resolution').value;
  return orient === 'vertical' ? res+'p_v' : res+'p';
}

async function autoVideoStart(){
  const person = document.getElementById('av-person').value.trim();
  if(!person) return alert('請輸入人物名稱');

  const keyword = document.getElementById('av-keyword').value.trim();
  const opts = {
    person: person,
    search_keyword: keyword || person,
    platform: document.getElementById('av-platform').value,
    max_videos: parseInt(document.getElementById('av-max-videos').value),
    strategy: document.getElementById('av-strategy').value,
    clip_duration: parseFloat(document.getElementById('av-clip-dur').value),
    total_duration: parseFloat(document.getElementById('av-total-dur').value),
    resolution: _avGetResolution(),
    transition: document.getElementById('av-transition').value,
    transition_dur: 0.5,
    audio_mode: document.getElementById('av-audio').value,
  };

  document.getElementById('btn-auto-video').disabled = true;
  document.getElementById('av-progress-card').style.display = '';
  document.getElementById('av-result-card').style.display = 'none';
  document.getElementById('av-prog-bar').style.width = '0%';
  document.getElementById('av-prog-text').textContent = '0%';
  document.getElementById('av-prog-msg').textContent = '啟動中...';
  document.getElementById('av-prog-msg').style.color = 'var(--txt2)';
  document.getElementById('av-log').innerHTML = '';
  document.getElementById('av-phase-title').textContent = '⏳ 處理中...';
  document.getElementById('av-progress-card').scrollIntoView({behavior:'smooth', block:'center'});

  try{
    const r = await fetch('/api/auto-video',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify(opts)
    });
    const d = await r.json();
    // 找不到照片資料夾 → 彈出選擇對話框
    if(d.need_mapping){
      const chosen = await _showMappingModal(d.keyword, d.available_folders, d.message);
      if(!chosen){
        document.getElementById('av-prog-msg').textContent = '⏭️ 已取消';
        document.getElementById('btn-auto-video').disabled = false;
        return;
      }
      opts.person = chosen;
      const r2 = await fetch('/api/auto-video',{
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify(opts)
      });
      const d2 = await r2.json();
      if(d2.error){
        document.getElementById('av-prog-msg').textContent = '❌ '+d2.error;
        document.getElementById('av-prog-msg').style.color = 'var(--err)';
        document.getElementById('btn-auto-video').disabled = false;
        return;
      }
      _avListenSSE(d2.task_id);
      return;
    }
    if(d.error){
      document.getElementById('av-prog-msg').textContent = '❌ '+d.error;
      document.getElementById('av-prog-msg').style.color = 'var(--err)';
      document.getElementById('btn-auto-video').disabled = false;
      return;
    }
    _avListenSSE(d.task_id);
  }catch(e){
    document.getElementById('av-prog-msg').textContent = '連線失敗: '+e;
    document.getElementById('av-prog-msg').style.color = 'var(--err)';
    document.getElementById('btn-auto-video').disabled = false;
  }
}

const _avPhaseLabels = {
  search: '🔍 搜尋中',
  download: '📥 下載中',
  extract: '🎯 擷取中',
  highlight: '🎬 剪輯中',
};

function _avListenSSE(taskId){
  if(avES) avES.close();
  const logEl = document.getElementById('av-log');
  let lastPhase = '';
  avES = new EventSource('/api/auto-video/progress/'+taskId);
  avES.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        document.getElementById('av-prog-bar').style.width = pct+'%';
        document.getElementById('av-prog-text').textContent = pct+'%';
        document.getElementById('av-prog-msg').textContent = d.message||'';
        // 更新標題
        if(d.phase && d.phase !== lastPhase){
          lastPhase = d.phase;
          document.getElementById('av-phase-title').textContent =
            (_avPhaseLabels[d.phase]||'⏳ 處理中') + '...';
        }
        // 加入日誌
        if(d.message){
          const line = document.createElement('div');
          const ts = new Date().toTimeString().slice(0,8);
          line.style.color = d.message.startsWith('⚠️') ? 'var(--warn)' :
                             d.message.startsWith('❌') ? 'var(--err)' :
                             d.message.startsWith('✅') ? 'var(--ok)' : 'var(--txt2)';
          line.textContent = '['+ts+'] '+d.message;
          logEl.appendChild(line);
          logEl.scrollTop = logEl.scrollHeight;
        }
        break;
      case 'done':
        avES.close(); avES=null;
        document.getElementById('av-prog-bar').style.width = '100%';
        document.getElementById('av-prog-text').textContent = '100%';
        document.getElementById('av-phase-title').textContent = '🎉 完成！';
        document.getElementById('av-prog-msg').innerHTML =
          '✅ 影片生成完成！ <a href="'+d.file_url+'" target="_blank" style="color:var(--pri);font-weight:600">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        // 顯示結果卡
        document.getElementById('av-result-card').style.display = '';
        document.getElementById('av-result-info').innerHTML =
          '<div style="display:flex;gap:16px;flex-wrap:wrap;font-size:.88em">'
          +'<span>📥 下載 <b>'+d.downloaded+'</b> 部</span>'
          +'<span>🎯 擷取 <b>'+d.extracted+'</b> 部</span>'
          +'<span>✂️ <b>'+d.clips_count+'</b> 個片段</span>'
          +'<span>⏱️ <b>'+d.duration+'</b> 秒</span>'
          +'<span>📦 <b>'+d.file_size_mb+'</b> MB</span>'
          +'</div>';
        document.getElementById('av-result-video').innerHTML =
          '<video src="'+d.file_url+'" controls style="max-width:100%;max-height:480px;border-radius:12px;margin-top:10px"></video>'
          +'<div style="margin-top:8px"><a href="'+d.file_url+'" download class="btn btn-pri" style="font-size:.88em">⬇️ 下載影片</a></div>';
        document.getElementById('av-result-card').scrollIntoView({behavior:'smooth', block:'center'});
        document.getElementById('btn-auto-video').disabled = false;
        // 加入日誌
        const line = document.createElement('div');
        line.style.color = 'var(--ok)';
        line.style.fontWeight = '600';
        line.textContent = '['+new Date().toTimeString().slice(0,8)+'] ✅ 完成！'+d.filename;
        logEl.appendChild(line);
        logEl.scrollTop = logEl.scrollHeight;
        break;
      case 'error':
        avES.close(); avES=null;
        document.getElementById('av-phase-title').textContent = '❌ 失敗';
        document.getElementById('av-prog-msg').textContent = '❌ '+d.message;
        document.getElementById('av-prog-msg').style.color = 'var(--err)';
        document.getElementById('btn-auto-video').disabled = false;
        const eline = document.createElement('div');
        eline.style.color = 'var(--err)';
        eline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ❌ '+d.message;
        logEl.appendChild(eline);
        break;
      case 'heartbeat': break;
    }
  };
  avES.onerror = ()=>{
    avES.close(); avES=null;
    document.getElementById('btn-auto-video').disabled = false;
    document.getElementById('av-phase-title').textContent = '⚠️ 連線中斷';
  };
}
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════
#  啟動
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print("=" * 50)
    print("  Media Downloader Web 版")
    print("=" * 50)
    print(f"  本機存取: http://localhost:{PORT}")
    print(f"  區網存取: http://{local_ip}:{PORT}")
    print(f"  照片路徑: {DOWNLOAD_ROOT}")
    print(f"  影片路徑: {YT_ROOT}")
    print(f"  感知雜湊: {'已啟用' if HAS_IMAGEHASH else '未安裝 (pip install imagehash)'}")
    print("=" * 50)
    print("  按 Ctrl+C 停止伺服器")
    print()

    try:
        from waitress import serve
        serve(app, host="0.0.0.0", port=PORT, threads=8)
    except ImportError:
        app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
