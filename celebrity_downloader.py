# -*- coding: utf-8 -*-
"""
明星照片下載器 v1.0
Celebrity Photo Downloader - 智能去重、批次下載
"""

import os
import sys
import re
import json
import time
import hashlib
import sqlite3
import threading
import logging
import html as html_mod
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext

# ── 檢查必要套件 ──────────────────────────────────────────
_missing = []
try:
    import requests
except ImportError:
    _missing.append("requests")
try:
    from PIL import Image, ImageTk
except ImportError:
    _missing.append("Pillow")
if _missing:
    _r = tk.Tk(); _r.withdraw()
    messagebox.showerror("缺少套件", f"請執行:\npip install {' '.join(_missing)}")
    sys.exit(1)

try:
    import imagehash
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False

# ── 設定 ──────────────────────────────────────────────────
DEFAULT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Photos")
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "history.db")
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")
PHASH_THRESHOLD = 8
MAX_FILE_SIZE = 20 * 1024 * 1024

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
    "不限": "",
    "大圖 (>500px)": "+filterui:imagesize-large",
    "超大 (>1024px)": "+filterui:imagesize-wallpaper",
}

logging.basicConfig(
    filename=os.path.join(DATA_DIR, "app.log") if os.path.isdir(DATA_DIR) else None,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ══════════════════════════════════════════════════════════
#  資料庫管理
# ══════════════════════════════════════════════════════════
class DatabaseManager:
    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS downloads (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    celebrity TEXT NOT NULL,
                    url      TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    md5      TEXT,
                    phash    TEXT,
                    size     INTEGER,
                    width    INTEGER,
                    height   INTEGER,
                    source   TEXT,
                    ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_url   ON downloads(url);
                CREATE INDEX IF NOT EXISTS idx_md5   ON downloads(md5);
                CREATE INDEX IF NOT EXISTS idx_celeb ON downloads(celebrity);
            """)

    def url_exists(self, url):
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM downloads WHERE url=? LIMIT 1", (url,)
            ).fetchone()
            return row is not None

    def md5_exists(self, md5):
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM downloads WHERE md5=? LIMIT 1", (md5,)
            ).fetchone()
            return row is not None

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

    def close(self):
        self.conn.close()


# ══════════════════════════════════════════════════════════
#  Bing 圖片搜尋爬蟲
# ══════════════════════════════════════════════════════════
class BingImageScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        """搜尋 Bing 圖片，回傳 URL 清單"""
        urls = []
        seen = set()
        offset = 0
        empty_pages = 0

        while len(urls) < max_num and empty_pages < 3:
            try:
                page_urls = self._fetch_page(keyword, offset, size_filter)
            except Exception as e:
                logging.error(f"搜尋錯誤: {e}")
                empty_pages += 1
                offset += 35
                time.sleep(1)
                continue

            new = 0
            for u in page_urls:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
                    new += 1
                    if callback:
                        callback(f"搜尋中... 找到 {len(urls)} 個連結")
                    if len(urls) >= max_num:
                        break

            if new == 0:
                empty_pages += 1
            else:
                empty_pages = 0

            offset += 35
            time.sleep(0.4)

        return urls[:max_num]

    def _fetch_page(self, keyword, offset, size_filter=""):
        qft = "+filterui:photo-photo"
        if size_filter:
            qft += size_filter

        params = {
            "q": keyword,
            "first": offset,
            "count": 35,
            "qft": qft,
            "form": "IRFLTR",
            "scenario": "ImageBasicHover",
        }
        resp = self.session.get(
            "https://www.bing.com/images/search", params=params, timeout=15
        )
        resp.raise_for_status()

        result = []

        # 方法1: 從 class="iusc" 元素的 m 屬性提取 murl（最可靠）
        m_attrs = re.findall(r'class="iusc"[^>]*m="([^"]+)"', resp.text)
        for m_raw in m_attrs:
            try:
                m_json = json.loads(html_mod.unescape(m_raw))
                murl = m_json.get("murl", "")
                if murl.startswith("http"):
                    result.append(murl)
            except Exception:
                continue

        # 方法2: 備用 - 直接搜尋 murl 欄位
        if not result:
            for m in re.finditer(r'"murl"\s*:\s*"(https?://[^"]+)"', resp.text):
                try:
                    url = json.loads(f'"{m.group(1)}"')
                    result.append(url)
                except Exception:
                    result.append(m.group(1))

        return result


# ══════════════════════════════════════════════════════════
#  Google 圖片搜尋
# ══════════════════════════════════════════════════════════
class GoogleImageScraper:
    def __init__(self):
        self.session = requests.Session()
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
        for m in re.finditer(
            r'\["(https?://[^"]{10,})",[0-9]+,[0-9]+\]', resp.text
        ):
            u = m.group(1).replace("\\u003d", "=").replace("\\u0026", "&")
            if not any(x in u for x in (
                "google.com", "gstatic.com", "googleapis.com",
                "googleusercontent.com", "ytimg.com",
            )):
                result.append(u)
        if not result:
            for m in re.finditer(r'"ou":"(https?://[^"]+)"', resp.text):
                result.append(m.group(1))
        return result


# ══════════════════════════════════════════════════════════
#  DuckDuckGo 圖片搜尋
# ══════════════════════════════════════════════════════════
class DuckDuckGoImageScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
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
#  Pinterest 圖片搜尋（實驗性）
# ══════════════════════════════════════════════════════════
class PinterestImageScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, keyword, max_num=100, size_filter="", callback=None):
        urls, seen = [], set()
        try:
            resp = self.session.get(
                "https://www.pinterest.com/search/pins/",
                params={"q": keyword},
                timeout=15,
            )
            for m in re.finditer(
                r'"url":"(https://i\.pinimg\.com/originals/[^"]+)"', resp.text
            ):
                u = m.group(1)
                if u not in seen:
                    seen.add(u); urls.append(u)
                    if callback:
                        callback(f"Pinterest 搜尋中... 找到 {len(urls)} 個連結")
                    if len(urls) >= max_num:
                        break
            if len(urls) < max_num:
                for m in re.finditer(
                    r'"url":"(https://i\.pinimg\.com/(?:736x|564x)/[^"]+)"',
                    resp.text,
                ):
                    u = m.group(1)
                    if u not in seen:
                        seen.add(u); urls.append(u)
                        if callback:
                            callback(f"Pinterest 搜尋中... 找到 {len(urls)} 個連結")
                        if len(urls) >= max_num:
                            break
        except Exception:
            pass
        return urls[:max_num]


SCRAPERS = {
    "Bing": BingImageScraper,
    "Google": GoogleImageScraper,
    "DuckDuckGo": DuckDuckGoImageScraper,
    "Pinterest": PinterestImageScraper,
}


# ══════════════════════════════════════════════════════════
#  圖片下載器（含去重）
# ══════════════════════════════════════════════════════════
class ImageDownloader:
    def __init__(self, db, download_root, celebrity, source="bing"):
        self.db = db
        self.celebrity = celebrity
        self.source = source
        self.celebrity_dir = os.path.join(
            download_root, self._sanitize(celebrity)
        )
        os.makedirs(self.celebrity_dir, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._stop = threading.Event()

        # 統計
        self.stats = {
            "downloaded": 0,
            "skip_url": 0,
            "skip_md5": 0,
            "skip_phash": 0,
            "failed": 0,
        }

        # 載入此明星既有的 pHash 快取
        self._phash_cache = []
        if HAS_IMAGEHASH:
            for h in db.get_phashes(celebrity):
                try:
                    self._phash_cache.append(imagehash.hex_to_hash(h))
                except Exception:
                    pass

        # 決定起始編號
        self._next_num = db.count(celebrity) + 1

    @staticmethod
    def _sanitize(name):
        invalid = r'<>:"/\|?*'
        result = "".join(c for c in name if c not in invalid)
        return result.strip(". ") or "unknown"

    def stop(self):
        self._stop.set()

    def download_all(self, urls, dedup_url=True, dedup_md5=True,
                     dedup_phash=True, progress_cb=None, log_cb=None):
        total = len(urls)
        for i, url in enumerate(urls):
            if self._stop.is_set():
                if log_cb:
                    log_cb("stop", "使用者中止下載", {})
                break

            result = self._download_one(url, dedup_url, dedup_md5, dedup_phash)

            if progress_cb:
                progress_cb(i + 1, total, self.stats)
            if log_cb:
                log_cb(result["status"], result.get("msg", ""), result)

        return self.stats

    def _download_one(self, url, dedup_url, dedup_md5, dedup_phash):
        # 1) URL 去重
        if dedup_url and self.db.url_exists(url):
            self.stats["skip_url"] += 1
            return {"status": "skip_url", "msg": "URL 已存在，跳過"}

        # 2) 下載圖片到記憶體
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            self.stats["failed"] += 1
            return {"status": "error", "msg": f"下載失敗: {e}"}

        if len(data) > MAX_FILE_SIZE:
            self.stats["failed"] += 1
            return {"status": "error", "msg": "檔案過大，跳過"}

        if len(data) < 1000:
            self.stats["failed"] += 1
            return {"status": "error", "msg": "檔案過小，可能非圖片"}

        # 3) 驗證是否為有效圖片
        try:
            img = Image.open(BytesIO(data))
            img.verify()
            img = Image.open(BytesIO(data))  # verify 後需重新開啟
            width, height = img.size
        except Exception:
            self.stats["failed"] += 1
            return {"status": "error", "msg": "非有效圖片，跳過"}

        # 4) MD5 去重
        md5 = hashlib.md5(data).hexdigest()
        if dedup_md5 and self.db.md5_exists(md5):
            self.stats["skip_md5"] += 1
            return {"status": "skip_md5", "msg": "MD5 重複，跳過"}

        # 5) pHash 去重
        phash_str = None
        if HAS_IMAGEHASH and dedup_phash:
            try:
                phash_val = imagehash.phash(img)
                phash_str = str(phash_val)
                for existing in self._phash_cache:
                    if abs(phash_val - existing) < PHASH_THRESHOLD:
                        self.stats["skip_phash"] += 1
                        return {"status": "skip_phash", "msg": "相似圖片已存在，跳過"}
            except Exception:
                pass

        # 6) 決定副檔名
        ext = self._detect_ext(url, data)

        # 7) 儲存檔案
        filename = f"{self._next_num:05d}{ext}"
        filepath = os.path.join(self.celebrity_dir, filename)
        while os.path.exists(filepath):
            self._next_num += 1
            filename = f"{self._next_num:05d}{ext}"
            filepath = os.path.join(self.celebrity_dir, filename)

        with open(filepath, "wb") as f:
            f.write(data)

        # 8) 記錄到資料庫
        self.db.add(
            self.celebrity, url, filename, md5,
            phash_str, len(data), width, height, self.source,
        )

        # 更新快取
        if phash_str and HAS_IMAGEHASH:
            self._phash_cache.append(imagehash.hex_to_hash(phash_str))

        self._next_num += 1
        self.stats["downloaded"] += 1

        size_kb = len(data) / 1024
        return {
            "status": "ok",
            "msg": f"{filename} ({width}x{height}, {size_kb:.0f}KB)",
            "filename": filename,
        }

    @staticmethod
    def _detect_ext(url, data):
        path = urlparse(url).path.lower()
        for ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"):
            if path.endswith(ext):
                return ".jpg" if ext == ".jpeg" else ext

        if data[:3] == b"\xff\xd8\xff":
            return ".jpg"
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            return ".png"
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return ".webp"
        if data[:3] == b"GIF":
            return ".gif"
        return ".jpg"


# ══════════════════════════════════════════════════════════
#  設定檔讀寫
# ══════════════════════════════════════════════════════════
def load_settings():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_settings(data):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════
#  主介面 (GUI)
# ══════════════════════════════════════════════════════════
class DownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("明星照片下載器 v1.0")
        self.root.geometry("820x780")
        self.root.minsize(700, 600)

        style = ttk.Style()
        style.theme_use("vista" if "vista" in style.theme_names() else "clam")

        self.db = DatabaseManager(DB_PATH)
        self.is_downloading = False
        self.current_downloader = None

        self._init_vars()
        self._build_ui()
        self._load_settings()
        self._refresh_list()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── 變數初始化 ────────────────────────────────────────
    def _init_vars(self):
        self.v_name = tk.StringVar()
        self.v_count = tk.IntVar(value=100)
        self.v_root = tk.StringVar(value=DEFAULT_ROOT)
        self.v_size = tk.StringVar(value="不限")
        self.v_append = tk.BooleanVar(value=True)
        self.v_face = tk.BooleanVar(value=False)
        self.v_dedup_url = tk.BooleanVar(value=True)
        self.v_dedup_md5 = tk.BooleanVar(value=True)
        self.v_dedup_phash = tk.BooleanVar(value=HAS_IMAGEHASH)
        self.v_progress = tk.DoubleVar(value=0)
        self.v_src_bing = tk.BooleanVar(value=True)
        self.v_src_google = tk.BooleanVar(value=False)
        self.v_src_ddg = tk.BooleanVar(value=False)
        self.v_src_pinterest = tk.BooleanVar(value=False)

    # ── 建立介面 ──────────────────────────────────────────
    def _build_ui(self):
        pad = {"padx": 8, "pady": 3}

        # ── 搜尋設定 ──
        frm_search = ttk.LabelFrame(self.root, text=" 搜尋設定 ", padding=8)
        frm_search.pack(fill="x", **pad)

        r = 0
        ttk.Label(frm_search, text="明星名稱:").grid(row=r, column=0, sticky="w")
        self.ent_name = ttk.Entry(frm_search, textvariable=self.v_name, width=30)
        self.ent_name.grid(row=r, column=1, sticky="ew", padx=4)
        self.ent_name.bind("<Return>", lambda e: self._start())
        ttk.Checkbutton(
            frm_search, text='附加 "photo"', variable=self.v_append
        ).grid(row=r, column=2, padx=4)
        ttk.Checkbutton(
            frm_search, text="僅人物照", variable=self.v_face
        ).grid(row=r, column=3, padx=4)

        r = 1
        ttk.Label(frm_search, text="下載數量:").grid(row=r, column=0, sticky="w")
        spn = ttk.Spinbox(
            frm_search, from_=10, to=1000, increment=10,
            textvariable=self.v_count, width=8
        )
        spn.grid(row=r, column=1, sticky="w", padx=4)
        ttk.Label(frm_search, text="圖片大小:").grid(row=r, column=2, sticky="w")
        ttk.Combobox(
            frm_search, textvariable=self.v_size,
            values=list(SIZE_FILTERS.keys()), state="readonly", width=14
        ).grid(row=r, column=3, sticky="w", padx=4)

        r = 2
        ttk.Label(frm_search, text="圖片來源:").grid(row=r, column=0, sticky="w")
        frm_src = ttk.Frame(frm_search)
        frm_src.grid(row=r, column=1, columnspan=3, sticky="w", padx=4)
        ttk.Checkbutton(frm_src, text="Bing", variable=self.v_src_bing).pack(side="left", padx=4)
        ttk.Checkbutton(frm_src, text="Google", variable=self.v_src_google).pack(side="left", padx=4)
        ttk.Checkbutton(frm_src, text="DuckDuckGo", variable=self.v_src_ddg).pack(side="left", padx=4)
        ttk.Checkbutton(frm_src, text="Pinterest", variable=self.v_src_pinterest).pack(side="left", padx=4)

        frm_search.columnconfigure(1, weight=1)

        # ── 去重設定 ──
        frm_dedup = ttk.LabelFrame(self.root, text=" 去重設定 ", padding=8)
        frm_dedup.pack(fill="x", **pad)

        ttk.Checkbutton(
            frm_dedup, text="URL 去重", variable=self.v_dedup_url
        ).pack(side="left", padx=8)
        ttk.Checkbutton(
            frm_dedup, text="MD5 去重 (完全相同)", variable=self.v_dedup_md5
        ).pack(side="left", padx=8)
        cb_phash = ttk.Checkbutton(
            frm_dedup, text="感知雜湊去重 (相似圖片)", variable=self.v_dedup_phash
        )
        cb_phash.pack(side="left", padx=8)
        if not HAS_IMAGEHASH:
            cb_phash.config(state="disabled")
            ttk.Label(
                frm_dedup, text="(需安裝 imagehash)", foreground="gray"
            ).pack(side="left")

        # ── 下載路徑 ──
        frm_path = ttk.LabelFrame(self.root, text=" 下載路徑 ", padding=8)
        frm_path.pack(fill="x", **pad)

        ttk.Entry(frm_path, textvariable=self.v_root).pack(
            side="left", fill="x", expand=True, padx=(0, 4)
        )
        ttk.Button(frm_path, text="瀏覽...", command=self._browse).pack(side="left")

        # ── 操作按鈕 ──
        frm_btn = ttk.Frame(self.root, padding=4)
        frm_btn.pack(fill="x", **pad)

        self.btn_start = ttk.Button(
            frm_btn, text="▶  開始下載", command=self._start
        )
        self.btn_start.pack(side="left", padx=4)

        self.btn_stop = ttk.Button(
            frm_btn, text="■  停止", command=self._stop, state="disabled"
        )
        self.btn_stop.pack(side="left", padx=4)

        ttk.Button(
            frm_btn, text="📁 開啟資料夾", command=self._open_folder
        ).pack(side="left", padx=4)

        ttk.Button(
            frm_btn, text="批次下載", command=self._batch_dialog
        ).pack(side="left", padx=4)

        # ── 進度 ──
        frm_prog = ttk.LabelFrame(self.root, text=" 進度 ", padding=8)
        frm_prog.pack(fill="x", **pad)

        self.progress_bar = ttk.Progressbar(
            frm_prog, variable=self.v_progress, maximum=100
        )
        self.progress_bar.pack(fill="x")
        self.lbl_status = ttk.Label(frm_prog, text="就緒")
        self.lbl_status.pack(fill="x", pady=(4, 0))

        # ── 下載日誌 ──
        frm_log = ttk.LabelFrame(self.root, text=" 下載日誌 ", padding=4)
        frm_log.pack(fill="both", expand=True, **pad)

        self.log_text = scrolledtext.ScrolledText(
            frm_log, height=10, font=("Consolas", 9), wrap="word"
        )
        self.log_text.pack(fill="both", expand=True)
        self.log_text.tag_configure("ok", foreground="#228B22")
        self.log_text.tag_configure("skip", foreground="#B8860B")
        self.log_text.tag_configure("error", foreground="#CD3333")
        self.log_text.tag_configure("info", foreground="#4682B4")
        self.log_text.tag_configure("stop", foreground="#888888")

        # ── 已下載明星列表 ──
        frm_list = ttk.LabelFrame(self.root, text=" 已下載明星 ", padding=4)
        frm_list.pack(fill="x", **pad)

        cols = ("celebrity", "count", "last")
        self.tree = ttk.Treeview(
            frm_list, columns=cols, show="headings", height=5
        )
        self.tree.heading("celebrity", text="名稱")
        self.tree.heading("count", text="數量")
        self.tree.heading("last", text="最後下載")
        self.tree.column("celebrity", width=200)
        self.tree.column("count", width=80, anchor="center")
        self.tree.column("last", width=160, anchor="center")
        self.tree.pack(fill="x")
        self.tree.bind("<Double-1>", self._on_tree_double_click)

        self.lbl_total = ttk.Label(frm_list, text="")
        self.lbl_total.pack(anchor="w", pady=(4, 0))

    # ── 設定讀取/儲存 ─────────────────────────────────────
    def _load_settings(self):
        s = load_settings()
        if "root" in s:
            self.v_root.set(s["root"])
        if "count" in s:
            self.v_count.set(s["count"])
        if "size" in s:
            self.v_size.set(s["size"])
        if "append_photo" in s:
            self.v_append.set(s["append_photo"])
        if "face_only" in s:
            self.v_face.set(s["face_only"])

    def _save_settings(self):
        save_settings({
            "root": self.v_root.get(),
            "count": self.v_count.get(),
            "size": self.v_size.get(),
            "append_photo": self.v_append.get(),
            "face_only": self.v_face.get(),
        })

    # ── 操作方法 ──────────────────────────────────────────
    def _browse(self):
        d = filedialog.askdirectory(initialdir=self.v_root.get())
        if d:
            self.v_root.set(d)

    def _open_folder(self):
        name = self.v_name.get().strip()
        root = self.v_root.get()
        if name:
            path = os.path.join(root, ImageDownloader._sanitize(name))
        else:
            path = root
        if os.path.isdir(path):
            os.startfile(path)
        elif os.path.isdir(root):
            os.startfile(root)
        else:
            messagebox.showinfo("提示", f"資料夾不存在:\n{path}")

    def _on_tree_double_click(self, event):
        sel = self.tree.selection()
        if sel:
            name = self.tree.item(sel[0])["values"][0]
            path = os.path.join(self.v_root.get(), ImageDownloader._sanitize(name))
            if os.path.isdir(path):
                os.startfile(path)

    def _log(self, msg, tag="info"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{ts}] {msg}\n", tag)
        self.log_text.see("end")

    def _refresh_list(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for name, cnt, ts in self.db.celebrities():
            ts_short = ts[:16] if ts else ""
            self.tree.insert("", "end", values=(name, cnt, ts_short))
        total = self.db.count()
        celeb_count = len(self.db.celebrities())
        self.lbl_total.config(text=f"共 {celeb_count} 位明星，{total} 張照片")

    def _toggle_controls(self, enabled):
        state = "normal" if enabled else "disabled"
        self.btn_start.config(state=state)
        self.ent_name.config(state=state)
        self.btn_stop.config(state="disabled" if enabled else "normal")

    # ── 下載流程 ──────────────────────────────────────────
    def _start(self):
        name = self.v_name.get().strip()
        if not name:
            messagebox.showwarning("提示", "請輸入明星名稱")
            return
        if self.is_downloading:
            return
        self._start_download(name)

    def _start_download(self, celebrity):
        self.is_downloading = True
        self._toggle_controls(False)
        self.v_progress.set(0)
        self.log_text.delete("1.0", "end")
        self._save_settings()

        t = threading.Thread(target=self._download_thread, args=(celebrity,), daemon=True)
        t.start()

    def _stop(self):
        if self.current_downloader:
            self.current_downloader.stop()
        self.root.after(0, lambda: self._log("正在停止...", "stop"))

    def _download_thread(self, celebrity):
        try:
            # 建立搜尋關鍵字
            keyword = celebrity
            if self.v_append.get():
                keyword += " photo"

            # 搜尋設定
            size_filter = SIZE_FILTERS.get(self.v_size.get(), "")
            if self.v_face.get():
                size_filter += "+filterui:face-face"

            max_num = self.v_count.get()

            # 收集選取的來源
            sources = []
            if self.v_src_bing.get(): sources.append("Bing")
            if self.v_src_google.get(): sources.append("Google")
            if self.v_src_ddg.get(): sources.append("DuckDuckGo")
            if self.v_src_pinterest.get(): sources.append("Pinterest")
            if not sources: sources.append("Bing")

            self.root.after(0, lambda: self._log(
                f"開始搜尋: {keyword} (數量: {max_num}, 來源: {', '.join(sources)})", "info"
            ))

            # Phase 1: 從多個來源搜尋圖片 URL
            all_urls = []
            seen_urls = set()
            for src in sources:
                scraper_cls = SCRAPERS.get(src)
                if not scraper_cls:
                    continue
                self.root.after(0, lambda s=src: self._log(f"從 {s} 搜尋...", "info"))
                try:
                    scraper = scraper_cls()
                    found = scraper.search(
                        keyword, max_num, size_filter,
                        callback=lambda msg: self.root.after(0, lambda m=msg: self._log(m, "info")),
                    )
                    new = 0
                    for u in found:
                        if u not in seen_urls:
                            seen_urls.add(u); all_urls.append(u); new += 1
                    self.root.after(0, lambda s=src, n=new: self._log(f"{s}: 找到 {n} 個新連結", "info"))
                except Exception as e:
                    self.root.after(0, lambda s=src, err=e: self._log(f"{s} 搜尋失敗: {err}", "error"))

            urls = all_urls
            if not urls:
                self.root.after(0, lambda: self._log("未找到任何圖片連結", "error"))
                return

            self.root.after(0, lambda: self._log(
                f"共找到 {len(urls)} 個連結，開始下載...", "info"
            ))

            # Phase 2: 下載
            downloader = ImageDownloader(
                self.db, self.v_root.get(), celebrity, ",".join(sources)
            )
            self.current_downloader = downloader

            def progress_cb(current, total, stats):
                pct = current / total * 100 if total else 0
                self.root.after(0, lambda: self.v_progress.set(pct))
                status = (
                    f"{current}/{total}  |  "
                    f"新增: {stats['downloaded']}  "
                    f"URL重複: {stats['skip_url']}  "
                    f"MD5重複: {stats['skip_md5']}  "
                    f"相似: {stats['skip_phash']}  "
                    f"失敗: {stats['failed']}"
                )
                self.root.after(0, lambda s=status: self.lbl_status.config(text=s))

            def log_cb(status, msg, detail):
                tag_map = {
                    "ok": "ok",
                    "skip_url": "skip",
                    "skip_md5": "skip",
                    "skip_phash": "skip",
                    "error": "error",
                    "stop": "stop",
                }
                tag = tag_map.get(status, "info")
                prefix = {
                    "ok": "✓",
                    "skip_url": "⊘",
                    "skip_md5": "⊘",
                    "skip_phash": "⊘",
                    "error": "✗",
                    "stop": "■",
                }.get(status, "→")
                self.root.after(0, lambda: self._log(f"{prefix} {msg}", tag))

            stats = downloader.download_all(
                urls,
                dedup_url=self.v_dedup_url.get(),
                dedup_md5=self.v_dedup_md5.get(),
                dedup_phash=self.v_dedup_phash.get(),
                progress_cb=progress_cb,
                log_cb=log_cb,
            )

            # Phase 3: 完成
            summary = (
                f"下載完成！ 成功: {stats['downloaded']}  "
                f"URL跳過: {stats['skip_url']}  "
                f"MD5跳過: {stats['skip_md5']}  "
                f"相似跳過: {stats['skip_phash']}  "
                f"失敗: {stats['failed']}"
            )
            self.root.after(0, lambda: self._log("=" * 55, "info"))
            self.root.after(0, lambda: self._log(summary, "ok"))

        except Exception as e:
            self.root.after(0, lambda: self._log(f"嚴重錯誤: {e}", "error"))
            logging.exception("Download thread error")
        finally:
            self.is_downloading = False
            self.current_downloader = None
            self.root.after(0, lambda: self._toggle_controls(True))
            self.root.after(0, self._refresh_list)

    # ── 批次下載 ──────────────────────────────────────────
    def _batch_dialog(self):
        if self.is_downloading:
            messagebox.showinfo("提示", "請等待目前下載完成")
            return

        dlg = tk.Toplevel(self.root)
        dlg.title("批次下載")
        dlg.geometry("400x300")
        dlg.transient(self.root)
        dlg.grab_set()

        ttk.Label(dlg, text="每行輸入一位明星名稱:").pack(padx=10, pady=(10, 4), anchor="w")
        txt = scrolledtext.ScrolledText(dlg, height=10, font=("Consolas", 10))
        txt.pack(fill="both", expand=True, padx=10, pady=4)

        def do_batch():
            names = [n.strip() for n in txt.get("1.0", "end").strip().split("\n") if n.strip()]
            if not names:
                messagebox.showwarning("提示", "請至少輸入一位明星名稱", parent=dlg)
                return
            dlg.destroy()
            self._run_batch(names)

        ttk.Button(dlg, text="開始批次下載", command=do_batch).pack(pady=10)

    def _run_batch(self, names):
        self.is_downloading = True
        self._toggle_controls(False)
        self.log_text.delete("1.0", "end")
        self._save_settings()

        def batch_thread():
            for i, name in enumerate(names):
                if not self.is_downloading:
                    break
                self.root.after(0, lambda n=name, idx=i: self._log(
                    f"━━━ 批次 {idx+1}/{len(names)}: {n} ━━━", "info"
                ))
                self._download_thread(name)
                if i < len(names) - 1:
                    time.sleep(1)
            self.root.after(0, lambda: self._log("批次下載全部完成！", "ok"))

        t = threading.Thread(target=batch_thread, daemon=True)
        t.start()

    # ── 關閉 ──────────────────────────────────────────────
    def _on_close(self):
        if self.is_downloading:
            if not messagebox.askyesno("確認", "下載進行中，確定要關閉嗎？"):
                return
            if self.current_downloader:
                self.current_downloader.stop()
        self._save_settings()
        self.db.close()
        self.root.destroy()


# ══════════════════════════════════════════════════════════
#  啟動
# ══════════════════════════════════════════════════════════
def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    root = tk.Tk()
    DownloaderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
