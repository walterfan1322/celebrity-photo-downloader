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
CELEB_GROUPS_FILE = os.path.join(DATA_DIR, "celeb_groups.json")

# 內建對照表：人物 → 團名（lowercase key）
# 使用者可透過 /api/celeb-groups POST 覆寫/新增
_DEFAULT_CELEB_GROUPS = {
    "wonhee": "illit",
    "iroha": "illit",
    "moka": "illit",
    "yunah": "illit",
    "minju": "illit",
    "wonyoung": "ive",
    "yujin": "ive",
    "anyujin": "ive",
    "gaeul": "ive",
    "rei": "ive",
    "leeseo": "ive",
    "liz": "ive",
    "yuna": "itzy",
    "yeji": "itzy",
    "lia": "itzy",
    "ryujin": "itzy",
    "chaeryeong": "itzy",
    "ahyeon": "babymonster",
    "ruka": "babymonster",
    "pharita": "babymonster",
    "asa": "babymonster",
    "rami": "babymonster",
    "rora": "babymonster",
    "chiquita": "babymonster",
    "chodan": "qwer",
    "magenta": "qwer",
    "hina": "qwer",
    "siyeon": "qwer",
    "jiwoo": "nmixx",
    "haewon": "nmixx",
    "sullyoon": "nmixx",
    "bae": "nmixx",
    "kyujin": "nmixx",
    "karina": "aespa",
    "winter": "aespa",
    "giselle": "aespa",
    "ningning": "aespa",
    "hayoung": "fromis_9",
    "iu": "",  # solo
}


def _load_celeb_groups():
    """讀使用者覆寫檔，合併內建對照表。回傳 dict: name(lower) → group"""
    groups = dict(_DEFAULT_CELEB_GROUPS)
    try:
        if os.path.isfile(CELEB_GROUPS_FILE):
            with open(CELEB_GROUPS_FILE, "r", encoding="utf-8") as f:
                user_groups = json.load(f)
            for k, v in user_groups.items():
                groups[k.lower().strip()] = (v or "").strip()
    except Exception as e:
        logging.warning(f"celeb_groups load failed: {e}")
    return groups


def _save_celeb_groups(user_groups):
    """只存使用者自訂的 overrides"""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        clean = {k.lower().strip(): (v or "").strip() for k, v in user_groups.items() if k.strip()}
        with open(CELEB_GROUPS_FILE, "w", encoding="utf-8") as f:
            json.dump(clean, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logging.warning(f"celeb_groups save failed: {e}")
        return False


def _generate_caption(person):
    """根據格式 `{name} #{name} #{group}` 生成文案與 hashtags。

    回傳 dict: {"caption": str, "hashtags": [str, ...], "text": str}
    """
    name = (person or "").strip()
    if not name:
        return {"caption": "", "hashtags": [], "text": ""}
    key = name.lower()
    groups = _load_celeb_groups()
    group = groups.get(key, "")
    tags = [name]
    if group:
        tags.append(group)
    text = name + " " + " ".join(f"#{t}" for t in tags)
    return {
        "caption": name,
        "hashtags": tags,
        "text": text,
    }


# 各語言常見停用字與不應出現在文案的詞
_CAPTION_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "of", "for", "with", "on", "in", "at",
    "to", "from", "by", "is", "are", "was", "were", "be", "been", "being", "it",
    "this", "that", "these", "those", "my", "your", "his", "her", "their", "our",
    "video", "official", "full", "vs", "ft", "feat", "featuring", "mv", "live",
    "hd", "4k", "new", "tv", "show", "ep", "eng", "sub", "shorts", "tiktok",
    "youtube", "ins", "instagram", "reels", "reel", "fancam", "kpop", "kfan",
    # 中日韓常見
    "的", "了", "是", "我", "你", "他", "她", "在", "和", "與", "及",
    "完整", "版", "版本", "影片", "直拍", "直擊",
}


def _extract_hashtags_from_text(text):
    """從文字抓 #hashtag（支援英數、中文、韓日字符、底線）"""
    if not text:
        return []
    # 支援 # 後面的英數、中日韓字符、底線
    pattern = r'#([\w\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]+)'
    import re as _re
    return _re.findall(pattern, text)


def _load_video_info_json(source_video_path):
    """尋找並讀取 {id}.info.json。source_video 可能是原始下載或擷取後檔案。

    擷取檔名格式：{original_id}_{person}.mp4
    原始檔名格式：{id}.mp4 位於 downloads/{person}/
    """
    try:
        base = os.path.basename(source_video_path)
        stem, _ = os.path.splitext(base)
        # 移除擷取後綴（_{person}）
        parts = stem.rsplit("_", 1)
        _cand_stems = [stem]
        if len(parts) == 2:
            _cand_stems.append(parts[0])

        # 搜尋位置：擷取檔同目錄、downloads/* 各 person 子資料夾
        search_dirs = [os.path.dirname(source_video_path)]
        if os.path.isdir(YT_DOWNLOADS):
            search_dirs.append(YT_DOWNLOADS)
            for sub in os.listdir(YT_DOWNLOADS):
                sd = os.path.join(YT_DOWNLOADS, sub)
                if os.path.isdir(sd):
                    search_dirs.append(sd)

        for d in search_dirs:
            for s in _cand_stems:
                candidate = os.path.join(d, f"{s}.info.json")
                if os.path.isfile(candidate):
                    with open(candidate, "r", encoding="utf-8") as f:
                        return json.load(f)
    except Exception as e:
        logging.debug(f"load_video_info_json failed for {source_video_path}: {e}")
    return None


def _synthesize_caption_from_clips(person, clips):
    """從實際使用的片段對應的影片 metadata 聚合 hashtags + 關鍵字，組出新文案。

    策略：
    1. 基礎 `{name} #{name} #{group}` 一定保留
    2. 從所有來源影片的 title/description 抽 hashtags
    3. 統計出現頻率，扣掉 stopwords、person 名字、group 名字
    4. 取前 5 個最常見的 tag 加入
    5. 如果沒有任何 hashtag，退回基礎文案
    """
    base = _generate_caption(person)
    if not clips:
        return base

    tag_counts = {}
    titles_seen = []
    try:
        seen_videos = set()
        for c in clips:
            vpath = c.get("video") if isinstance(c, dict) else None
            if not vpath or vpath in seen_videos:
                continue
            seen_videos.add(vpath)
            info = _load_video_info_json(vpath)
            if not info:
                continue
            title = (info.get("title") or "").strip()
            desc = (info.get("description") or "").strip()
            if title:
                titles_seen.append(title)
            combined = f"{title}\n{desc}"
            # yt_dlp 可能也提供 tags 列表
            for t in (info.get("tags") or []):
                combined += " #" + str(t)
            for tag in _extract_hashtags_from_text(combined):
                key = tag.lower().strip()
                if not key or len(key) < 2 or len(key) > 30:
                    continue
                if key in _CAPTION_STOPWORDS:
                    continue
                # 不重複基礎 tag
                if key in (t.lower() for t in base["hashtags"]):
                    continue
                if key.isdigit():
                    continue
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
    except Exception as e:
        logging.warning(f"synthesize caption failed: {e}")
        return base

    if not tag_counts:
        return base

    # 取最常見的前 5 個（但有出現 >= 2 次的才優先；只出現 1 次的保留最多 3 個）
    sorted_tags = sorted(tag_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    multi_tags = [t for t, c in sorted_tags if c >= 2]
    single_tags = [t for t, c in sorted_tags if c == 1]
    extra = multi_tags[:5]
    if len(extra) < 5:
        extra += single_tags[: (5 - len(extra))]

    all_tags = list(base["hashtags"]) + extra
    # 去重（保持順序）
    seen = set()
    uniq_tags = []
    for t in all_tags:
        k = t.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq_tags.append(t)

    text = base["caption"] + " " + " ".join(f"#{t}" for t in uniq_tags)
    return {
        "caption": base["caption"],
        "hashtags": uniq_tags,
        "text": text,
        "sources": len(titles_seen),
    }
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


def _infer_video_type_from_path(video_path, person_folder):
    """從影片路徑推斷類型：downloads/ahyeon/fancam/xxx.mp4 → 'fancam'"""
    dl_person = os.path.join(YT_DOWNLOADS, person_folder)
    try:
        rel = os.path.relpath(video_path, dl_person).replace("\\", "/")
        parts = rel.split("/")
        if len(parts) > 1:
            return parts[0]  # e.g. "fancam", "dance", "stage"
    except (ValueError, TypeError):
        pass
    return ""


def _scan_extract_files(person_folder, person_suffix=None):
    """掃描 extracts/<person>/ 下所有擷取檔（含子資料夾），回傳 {basename: full_path}"""
    ext_base = os.path.join(YT_EXTRACTS, person_folder)
    result = {}
    if not os.path.isdir(ext_base):
        return result
    for root, dirs, files in os.walk(ext_base):
        for f in files:
            if not f.endswith(".mp4") or f.startswith("auto_"):
                continue
            if person_suffix and not f.endswith(person_suffix):
                continue
            fp = os.path.join(root, f)
            if os.path.isfile(fp) and os.path.getsize(fp) > 10240:
                # basename key: 去掉 _person 後綴
                if person_suffix:
                    bk = f[:-len(person_suffix)]
                else:
                    bk = os.path.splitext(f)[0]
                result[bk] = fp
    return result


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

    def delete_photo(self, celebrity, filename):
        """刪除單張照片的 DB 記錄"""
        with self.lock:
            self.conn.execute(
                "DELETE FROM downloads WHERE celebrity=? AND filename=?",
                (celebrity, filename),
            )
            self.conn.execute(
                "DELETE FROM photo_usage WHERE celebrity=? AND filename=?",
                (celebrity, filename),
            )
            self.conn.commit()

    def delete_celebrity(self, celebrity):
        """刪除整個 celebrity 的所有 DB 記錄"""
        with self.lock:
            self.conn.execute("DELETE FROM downloads WHERE celebrity=?", (celebrity,))
            self.conn.execute("DELETE FROM photo_usage WHERE celebrity=?", (celebrity,))
            self.conn.execute("DELETE FROM used_keywords WHERE celebrity=?", (celebrity,))
            self.conn.commit()

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
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB 上傳上限
db = DatabaseManager(DB_PATH)

# 任務管理
tasks = {}  # task_id -> {queue, downloader, thread, status}
tasks_lock = threading.Lock()


@app.route("/")
def index():
    return HTML_PAGE


_IMG_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}

@app.route("/api/celebrities")
def api_celebrities():
    """列出所有 Photo — 數量從檔案系統即時計算"""
    db_rows = {r[0]: r[2] for r in db.celebrities()}  # name -> last ts
    result = []
    if os.path.isdir(DOWNLOAD_ROOT):
        for name in sorted(os.listdir(DOWNLOAD_ROOT)):
            d = os.path.join(DOWNLOAD_ROOT, name)
            if not os.path.isdir(d):
                continue
            count = sum(1 for f in os.listdir(d)
                       if os.path.isfile(os.path.join(d, f))
                       and os.path.splitext(f)[1].lower() in _IMG_EXTS)
            if count == 0:
                continue
            last = db_rows.get(name, "")
            if last:
                last = last[:16]
            else:
                # 沒有 DB 記錄就用資料夾修改時間
                mt = os.path.getmtime(d)
                last = datetime.fromtimestamp(mt).strftime("%Y-%m-%d %H:%M")
            result.append({"name": name, "count": count, "last": last})
    result.sort(key=lambda x: x["last"], reverse=True)
    return jsonify(result)


@app.route("/api/celeb-groups", methods=["GET", "POST"])
def api_celeb_groups():
    """讀寫人物→團名對照表"""
    if request.method == "GET":
        return jsonify(_load_celeb_groups())
    # POST: 儲存使用者 overrides（會與內建表合併）
    data = request.json or {}
    # 只存與 default 不同的
    default = _DEFAULT_CELEB_GROUPS
    overrides = {}
    for k, v in data.items():
        k_norm = (k or "").lower().strip()
        v_norm = (v or "").strip()
        if not k_norm:
            continue
        if default.get(k_norm, "") != v_norm:
            overrides[k_norm] = v_norm
    ok = _save_celeb_groups(overrides)
    return jsonify({"ok": ok, "count": len(overrides)})


@app.route("/api/caption/<person>")
def api_caption(person):
    """預覽文案"""
    return jsonify(_generate_caption(person))


@app.route("/api/stats")
def api_stats():
    celebs = api_celebrities().get_json()
    return jsonify({
        "total_celebs": len(celebs),
        "total_photos": sum(c["count"] for c in celebs),
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


@app.route("/api/photos/delete", methods=["POST"])
def api_delete_photo():
    """刪除單張照片"""
    data = request.get_json(force=True)
    celebrity = sanitize_name((data.get("celebrity") or "").strip())
    filename = (data.get("filename") or "").strip()
    if not celebrity or not filename:
        return jsonify({"error": "missing celebrity or filename"}), 400
    # 安全檢查：防止路徑穿越
    if "/" in filename or "\\" in filename or ".." in filename:
        return jsonify({"error": "invalid filename"}), 400
    photo_dir = os.path.join(DOWNLOAD_ROOT, celebrity)
    fp = os.path.join(photo_dir, filename)
    if not os.path.isfile(fp):
        return jsonify({"error": "檔案不存在"}), 404
    try:
        os.remove(fp)
        db.delete_photo(celebrity, filename)
        remaining = len([f for f in os.listdir(photo_dir)
                        if os.path.isfile(os.path.join(photo_dir, f))
                        and os.path.splitext(f)[1].lower() in
                        {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}])
        logging.info(f"Deleted photo: {fp} (remaining: {remaining})")
        return jsonify({"ok": True, "remaining": remaining})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/photos/delete-folder", methods=["POST"])
def api_delete_photo_folder():
    """刪除整個 Photo 資料夾"""
    data = request.get_json(force=True)
    celebrity = sanitize_name((data.get("celebrity") or "").strip())
    if not celebrity:
        return jsonify({"error": "missing celebrity"}), 400
    photo_dir = os.path.join(DOWNLOAD_ROOT, celebrity)
    if not os.path.isdir(photo_dir):
        return jsonify({"error": "資料夾不存在"}), 404
    try:
        count = len([f for f in os.listdir(photo_dir)
                    if os.path.isfile(os.path.join(photo_dir, f))])
        shutil.rmtree(photo_dir)
        db.delete_celebrity(celebrity)
        logging.info(f"Deleted photo folder: {photo_dir} ({count} files)")
        return jsonify({"ok": True, "deleted_count": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

def _tiktok_search(keyword, max_results=10, progress_cb=None):
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
        if progress_cb:
            progress_cb(0, 0, max_results, "連接瀏覽器...")
        page = ChromiumPage(co)
        # 導航到 TikTok 搜尋
        search_url = f"https://www.tiktok.com/search/video?q={quote(keyword)}"
        page.get(search_url)
        if progress_cb:
            progress_cb(0, 0, max_results, "載入搜尋頁面...")
        _time.sleep(7)
        # 嘗試關閉可能的登入彈窗或 cookie 提示
        try:
            # TikTok 的 cookie 同意按鈕
            cookie_btn = page.ele('css:button[data-testid="cookie-banner-accept"]', timeout=2)
            if cookie_btn:
                cookie_btn.click()
                _time.sleep(1)
        except Exception:
            pass
        try:
            # 關閉登入彈窗（如果有的話）
            close_btns = page.eles('css:[data-e2e="modal-close-inner-button"]')
            for btn in close_btns:
                try:
                    btn.click()
                    _time.sleep(0.5)
                except Exception:
                    pass
        except Exception:
            pass
        # 動態滾動：一直滾到抓夠影片為止
        # TikTok 用 #grid-main 做內部滾動容器，不是 window scroll
        seen = set()
        unique = []
        max_scroll_rounds = max(15, max_results * 3)
        stall_count = 0
        prev_count = 0
        # 找出可滾動容器的 CSS selector
        _scroll_selector = page.run_js_loaded("""
            var el = document.getElementById('grid-main');
            if (el && el.scrollHeight > el.clientHeight) return '#grid-main';
            var all = document.querySelectorAll('*');
            for (var i = 0; i < all.length; i++) {
                var s = window.getComputedStyle(all[i]);
                if ((s.overflowY==='scroll'||s.overflowY==='auto') && all[i].scrollHeight > all[i].clientHeight + 50) {
                    if (all[i].id) return '#' + all[i].id;
                    return all[i].tagName.toLowerCase();
                }
            }
            return '';
        """) or '#grid-main'
        logging.info(f"TikTok search: scroll container = {_scroll_selector}")
        for _scroll_i in range(max_scroll_rounds):
            html = page.html
            video_links = _re.findall(
                r'href="(https://www\.tiktok\.com/@([^"/@]+)/video/(\d+))"', html
            )
            for full_url, author, vid_id in video_links:
                if vid_id not in seen:
                    seen.add(vid_id)
                    unique.append({"url": full_url, "author": author, "id": vid_id})
            # 回報進度
            if progress_cb:
                pct = min(90, int(len(unique) / max_results * 90)) if max_results > 0 else 0
                progress_cb(pct, len(unique), max_results,
                           f"滾動搜尋中... 已找到 {len(unique)}/{max_results} 個影片")
            if len(unique) >= max_results:
                break
            # 偵測停滯
            if len(unique) == prev_count:
                stall_count += 1
                if stall_count >= 8:
                    logging.info(f"TikTok search: stalled after {stall_count} scrolls at {len(unique)} results")
                    break
            else:
                stall_count = 0
            prev_count = len(unique)
            try:
                # 滾動內部容器（#grid-main）
                _sel = _scroll_selector.replace("'", "\\'")
                page.run_js(f"var e=document.querySelector('{_sel}');if(e)e.scrollBy(0,2000)")
                _time.sleep(2.5)
            except Exception:
                break
        # 最後再抓一次（可能最後一次滾動後載入了新的）
        if len(unique) < max_results:
            html = page.html
            video_links = _re.findall(
                r'href="(https://www\.tiktok\.com/@([^"/@]+)/video/(\d+))"', html
            )
            for full_url, author, vid_id in video_links:
                if vid_id not in seen:
                    seen.add(vid_id)
                    unique.append({"url": full_url, "author": author, "id": vid_id})
        logging.info(f"TikTok search: {len(unique)} unique videos after {_scroll_i+1} scrolls")
        # 取得影片描述 / 觀看數元素
        desc_els = page.eles('css:[data-e2e="search-card-desc"]')
        view_els = page.eles('css:[data-e2e="video-views"]')
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

    # 影片類型關鍵字對照
    VIDEO_TYPE_KEYWORDS = {
        "stage": "stage performance live",
        "fancam": "fancam 직캠",
        "dance": "dance 舞蹈",
        "music_show": "Music Core OR Music Bank OR Inkigayo OR M Countdown",
        "mv": "MV official",
        "cute": "cute moments 可愛",
        "vlog": "vlog 日常",
        "challenge": "challenge 挑戰",
    }
    VIDEO_TYPE_KEYWORDS_TT = {
        "stage": "舞台 stage performance",
        "fancam": "직캠 fancam 直拍",
        "dance": "舞蹈 dance",
        "mv": "MV official",
        "cute": "可愛 cute",
        "vlog": "vlog 日常",
        "challenge": "挑戰 challenge",
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
                    search_query_tt = query
                    if video_type in VIDEO_TYPE_KEYWORDS_TT:
                        search_query_tt = f"{query} {VIDEO_TYPE_KEYWORDS_TT[video_type]}"
                    entries = []
                    tiktok_results = _tiktok_search(search_query_tt, max_results)
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


# ── TikTok 搜尋 SSE（支援進度回報）────────────────────
yt_search_tasks = {}
yt_search_tasks_lock = threading.Lock()


@app.route("/api/yt/search-sse", methods=["POST"])
def api_yt_search_sse():
    """啟動 TikTok/YouTube 搜尋任務（SSE 進度）"""
    if not HAS_YTDLP:
        return jsonify({"error": "yt-dlp 未安裝"}), 500
    data = request.json
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"error": "請輸入搜尋關鍵字"}), 400
    max_results = int(data.get("max_results", 10))
    video_type = (data.get("video_type") or "all").strip()
    platform = (data.get("platform") or "youtube").strip().lower()

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with yt_search_tasks_lock:
        yt_search_tasks[task_id] = {"queue": q}

    def worker():
        try:
            _yt_search_worker(q, query, max_results, video_type, platform)
        except Exception as e:
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id})


def _yt_search_worker(q, query, max_results, video_type, platform):
    """搜尋 worker（在背景執行緒中）"""

    def _get_existing_ids(keyword):
        """掃描已下載 / 已擷取的影片 ID，用於標記搜尋結果"""
        downloaded_ids = set()
        extracted_ids = set()
        person_folder = sanitize_name(resolve_celebrity(keyword))

        # 掃描 downloads/<person>/ （含子資料夾）
        dl_base = os.path.join(YT_DOWNLOADS, person_folder)
        if os.path.isdir(dl_base):
            for root, dirs, files in os.walk(dl_base):
                for f in files:
                    base = os.path.splitext(f)[0]
                    # yt-dlp 格式: "VIDEO_ID.ext" 或 "title [VIDEO_ID].ext"
                    m = re.search(r'\[([A-Za-z0-9_-]+)\]$', base)
                    if m:
                        downloaded_ids.add(m.group(1))
                    elif re.match(r'^[A-Za-z0-9_-]+$', base):
                        downloaded_ids.add(base)

        # 掃描 extracts/<person>/（遞迴含子資料夾）
        ext_base = os.path.join(YT_EXTRACTS, person_folder)
        if os.path.isdir(ext_base):
            for root, dirs, files in os.walk(ext_base):
                for f in files:
                    if not f.endswith(".mp4") or f.startswith("auto_"):
                        continue
                    base = os.path.splitext(f)[0]
                    m = re.search(r'\[([A-Za-z0-9_-]+)\]', base)
                    if m:
                        extracted_ids.add(m.group(1))
                    else:
                        parts = base.rsplit(f"_{person_folder}", 1)
                        if len(parts) == 2 and parts[0]:
                            extracted_ids.add(parts[0])

        return downloaded_ids, extracted_ids

    VIDEO_TYPE_KEYWORDS = {
        "stage": "stage performance live",
        "fancam": "fancam 직캠",
        "dance": "dance 舞蹈",
        "music_show": "Music Core OR Music Bank OR Inkigayo OR M Countdown",
        "mv": "MV official",
        "cute": "cute moments 可愛",
        "vlog": "vlog 日常",
        "challenge": "challenge 挑戰",
    }
    VIDEO_TYPE_KEYWORDS_TT = {
        "stage": "舞台 stage performance",
        "fancam": "직캠 fancam 直拍",
        "dance": "舞蹈 dance",
        "mv": "MV official",
        "cute": "可愛 cute",
        "vlog": "vlog 日常",
        "challenge": "挑戰 challenge",
    }

    is_url = query.startswith("http://") or query.startswith("https://")
    if is_url:
        if "tiktok.com" in query:
            platform = "tiktok"
        elif "youtube.com" in query or "youtu.be" in query:
            platform = "youtube"

    q.put({"type": "progress", "percent": 0, "found": 0, "target": max_results,
           "message": f"🔍 搜尋 {platform.upper()}: {query[:40]}..."})

    try:
        ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
        if is_url:
            ydl_opts["noplaylist"] = True

        if platform == "tiktok" and not is_url and not query.startswith("@"):
            # TikTok 關鍵字搜尋 — 用 DrissionPage（帶進度）
            search_query_tt = query
            if video_type in VIDEO_TYPE_KEYWORDS_TT:
                search_query_tt = f"{query} {VIDEO_TYPE_KEYWORDS_TT[video_type]}"

            def search_progress(pct, found, target, msg):
                q.put({"type": "progress", "percent": pct, "found": found,
                       "target": target, "message": msg})

            tiktok_results = _tiktok_search(search_query_tt, max_results,
                                            progress_cb=search_progress)
            if tiktok_results:
                # 標記已下載 / 已擷取
                dl_ids, ex_ids = _get_existing_ids(query)
                for r in tiktok_results:
                    vid = r.get("id", "")
                    r["downloaded"] = vid in dl_ids
                    r["extracted"] = vid in ex_ids
                n_dl = sum(1 for r in tiktok_results if r.get("downloaded"))
                msg = f"找到 {len(tiktok_results)} 個結果"
                if n_dl:
                    msg += f"（{n_dl} 個已下載）"
                q.put({"type": "done", "results": tiktok_results, "message": msg})
            else:
                q.put({"type": "done", "results": [],
                       "hint": "TikTok 搜尋暫時無法取得結果，請貼上影片網址或輸入 @用戶名"})
            return

        # YouTube 或 URL — 用 yt-dlp（通常很快）
        q.put({"type": "progress", "percent": 30, "found": 0, "target": max_results,
               "message": "正在查詢..."})

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            if is_url:
                info = ydl.extract_info(query, download=False)
                entries = [info] if info.get("_type") != "playlist" else (info.get("entries") or [])
            elif platform == "tiktok" and query.startswith("@"):
                tiktok_url = f"https://www.tiktok.com/{query}"
                info = ydl.extract_info(tiktok_url, download=False)
                entries = (info.get("entries") or [])[:max_results]
            else:
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
                "thumbnail": e.get("thumbnail") or (
                    f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg" if platform == "youtube" else ""),
                "platform": platform,
            })

        # 標記已下載 / 已擷取
        dl_ids, ex_ids = _get_existing_ids(query)
        for r in results:
            vid = r.get("id", "")
            r["downloaded"] = vid in dl_ids
            r["extracted"] = vid in ex_ids
        n_dl = sum(1 for r in results if r.get("downloaded"))
        msg = f"找到 {len(results)} 個結果"
        if n_dl:
            msg += f"（{n_dl} 個已下載）"
        q.put({"type": "done", "results": results, "message": msg})

    except Exception as e:
        q.put({"type": "error", "message": str(e)})


@app.route("/api/yt/search-progress/<task_id>")
def api_yt_search_progress(task_id):
    """搜尋 SSE 進度"""
    def generate():
        with yt_search_tasks_lock:
            task = yt_search_tasks.get(task_id)
        if not task:
            yield f"data: {json.dumps({'type': 'error', 'message': '任務不存在'})}\n\n"
            return
        q = task["queue"]
        while True:
            try:
                evt = q.get(timeout=120)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        with yt_search_tasks_lock:
            yt_search_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/yt/download", methods=["POST"])
def api_yt_download():
    if not HAS_YTDLP:
        return jsonify({"error": "yt-dlp 未安裝"}), 500
    data = request.json
    url = (data.get("url") or "").strip()
    title = (data.get("title") or "video").strip()
    quality = data.get("quality", "bestvideo[height<=720]+bestaudio/best")
    keyword = (data.get("keyword") or "").strip()
    video_type = (data.get("video_type") or "all").strip()
    audio_only = "bestaudio" in quality and "bestvideo" not in quality

    if not url:
        return jsonify({"error": "請提供影片網址"}), 400

    # 依搜尋關鍵字歸檔到對應人名子資料夾
    if keyword:
        person_folder = sanitize_name(resolve_celebrity(keyword))
    else:
        person_folder = "_unsorted"
    dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
    # 有指定類型時，存到類型子資料夾
    if video_type and video_type != "all":
        dl_dir = os.path.join(dl_dir, video_type)
    os.makedirs(dl_dir, exist_ok=True)
    logging.info(f"yt-download: url={url[:60]}, keyword={keyword}, video_type={video_type}, dl_dir={dl_dir}")

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with yt_tasks_lock:
        yt_tasks[task_id] = {"queue": q, "status": "running"}

    is_tiktok = "tiktok.com" in url

    def worker():
        import subprocess as _sp
        try:
            q.put({"type": "progress", "percent": 0, "message": f"開始下載: {title}"})
            out_tpl = os.path.join(dl_dir, "%(id)s.%(ext)s")

            if is_tiktok:
                # ═══ TikTok: 用 subprocess 避免 Waitress 執行緒的編碼問題 ═══
                cmd = [
                    sys.executable, "-m", "yt_dlp",
                    "--no-playlist",
                    "--windows-filenames",
                    "--restrict-filenames",
                    "--file-access-retries", "10",
                    "-f", "best[height<=720]/best",
                    "-o", out_tpl,
                    "--newline",   # 每行輸出進度，方便解析
                ]
                if FFMPEG_LOCATION:
                    cmd += ["--ffmpeg-location", FFMPEG_LOCATION]
                cmd.append(url)

                logging.info(f"yt-download subprocess: {url}")
                proc = _sp.Popen(
                    cmd, stdout=_sp.PIPE, stderr=_sp.STDOUT,
                    cwd=dl_dir,
                    env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                )
                # 解析進度
                for raw_line in proc.stdout:
                    line = raw_line.decode("utf-8", errors="replace").strip()
                    # yt-dlp 進度行: [download]  45.2% of  12.34MiB at  1.23MiB/s ETA 00:05
                    m = re.search(r'\[download\]\s+([\d.]+)%', line)
                    if m:
                        pct = min(int(float(m.group(1))), 95)
                        # 提取速度和 ETA
                        speed_m = re.search(r'at\s+([\d.]+\s*\S+/s)', line)
                        eta_m = re.search(r'ETA\s+(\S+)', line)
                        msg_parts = [f"下載中 {pct}%"]
                        if speed_m:
                            msg_parts.append(speed_m.group(1))
                        if eta_m:
                            msg_parts.append(f"剩餘 {eta_m.group(1)}")
                        q.put({"type": "progress", "percent": pct,
                               "message": " ".join(msg_parts)})
                    elif "[Merger]" in line or "Merging" in line:
                        q.put({"type": "progress", "percent": 96,
                               "message": "合併/轉檔中..."})

                proc.wait(timeout=600)
                if proc.returncode != 0:
                    logging.warning(f"yt-download subprocess failed: rc={proc.returncode}")
                    q.put({"type": "error", "message": f"下載失敗 (exit code {proc.returncode})"})
                    return

                info = {"id": "", "title": title}
                # 從 URL 提取 video id
                id_m = re.search(r'/video/(\d+)', url)
                if id_m:
                    info["id"] = id_m.group(1)

            else:
                # ═══ YouTube: 用 yt-dlp 函式庫（支援進度回呼）═══
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

                ydl_opts = {
                    "format": quality,
                    "outtmpl": out_tpl,
                    "progress_hooks": [progress_hook],
                    "merge_output_format": "mp4" if not audio_only else None,
                    "quiet": True,
                    "no_warnings": True,
                    "noplaylist": True,
                    "windowsfilenames": True,
                    "restrictfilenames": True,
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
                for attempt, fmt in enumerate([quality, "best"]):
                    if attempt > 0 and (audio_only or fmt == quality):
                        break
                    try:
                        ydl_opts_try = dict(ydl_opts, format=fmt)
                        with yt_dlp.YoutubeDL(ydl_opts_try) as ydl:
                            info = ydl.extract_info(url, download=True)
                            info = ydl.sanitize_info(info)
                        vid_id_check = info.get("id", "")
                        found_complete = False
                        for fname in os.listdir(dl_dir):
                            fp_chk = os.path.join(dl_dir, fname)
                            if os.path.isfile(fp_chk) and vid_id_check in fname and not fname.endswith(".part"):
                                if os.path.getsize(fp_chk) > 10240:
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

            # ═══ 共用：找到實際輸出檔案 ═══
            actual_file = None
            vid_id = info.get("id", "")
            for f in sorted(os.listdir(dl_dir),
                            key=lambda x: (os.path.getmtime(os.path.join(dl_dir, x)) if os.path.exists(os.path.join(dl_dir, x)) else 0),
                            reverse=True):
                fp_c = os.path.join(dl_dir, f)
                if os.path.isfile(fp_c) and not f.endswith(".part") and os.path.getsize(fp_c) > 10240:
                    if vid_id and vid_id in f:
                        actual_file = f
                        break
            if not actual_file:
                # fallback: 找最新的完整檔案
                for f in sorted(os.listdir(dl_dir),
                                key=lambda x: (os.path.getmtime(os.path.join(dl_dir, x)) if os.path.exists(os.path.join(dl_dir, x)) else 0),
                                reverse=True):
                    fp_c = os.path.join(dl_dir, f)
                    if os.path.isfile(fp_c) and not f.endswith(".part") and os.path.getsize(fp_c) > 10240:
                        actual_file = f
                        break

            if actual_file:
                fp = os.path.join(dl_dir, actual_file)
                rel_path = os.path.relpath(fp, YT_ROOT).replace("\\", "/")
                file_size = os.path.getsize(fp)
                q.put({
                    "type": "done",
                    "filename": actual_file,
                    "rel_path": rel_path,
                    "file_url": f"/yt-files/{rel_path}",
                    "file_size_mb": round(file_size / 1024 / 1024, 1),
                    "title": info.get("title", title),
                    "duration": info.get("duration"),
                    "person": person_folder,
                })
            else:
                if vid_id:
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
            # 遞迴掃描（支援類型子資料夾）
            for root, _dirs, fnames in os.walk(person_dir):
                for f in fnames:
                    fp = os.path.join(root, f)
                    if os.path.splitext(f)[1].lower() not in exts:
                        continue
                    rel_path = os.path.relpath(fp, YT_ROOT).replace("\\", "/")
                    # 取得子類型（如 fancam, dance 等）
                    sub = os.path.relpath(root, person_dir).replace("\\", "/")
                    video_type_label = sub if sub != "." else ""
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
                        "video_type": video_type_label,
                    })
    files.sort(key=lambda x: x["created"], reverse=True)
    return jsonify(files)


@app.route("/yt-files/<path:filename>")
def serve_yt_file(filename):
    safe = os.path.normpath(os.path.join(YT_ROOT, filename))
    if not safe.startswith(os.path.normpath(YT_ROOT)):
        return "Forbidden", 403
    return send_from_directory(YT_ROOT, filename)


@app.route("/api/yt/delete-output", methods=["POST"])
def api_yt_delete_output():
    """刪除一鍵生成的 output 影片"""
    data = request.get_json(force=True)
    rel_path = (data.get("rel_path") or "").strip()
    if not rel_path:
        return jsonify({"error": "missing rel_path"}), 400
    # 安全檢查：只允許刪除 output/ 下的檔案
    full = os.path.normpath(os.path.join(YT_ROOT, rel_path))
    output_root = os.path.normpath(os.path.join(YT_ROOT, "output"))
    if not full.startswith(output_root):
        return jsonify({"error": "只能刪除 output 目錄下的檔案"}), 403
    if not os.path.isfile(full):
        return jsonify({"error": "檔案不存在"}), 404
    try:
        os.remove(full)
        logging.info(f"Deleted output file: {full}")
        return jsonify({"ok": True, "deleted": rel_path})
    except Exception as e:
        logging.error(f"Delete failed: {e}")
        return jsonify({"error": str(e)}), 500


# ── 影片 → 照片擷取 ──────────────────────────────────────
UPLOAD_TMP = os.path.join(APP_DIR, "tmp_uploads")
os.makedirs(UPLOAD_TMP, exist_ok=True)


@app.route("/api/photos/from-video", methods=["POST"])
def api_photos_from_video():
    """從上傳的影片擷取畫面存為照片"""
    if "video" not in request.files:
        return jsonify({"error": "未上傳影片"}), 400
    celebrity = sanitize_name((request.form.get("celebrity") or "").strip())
    if not celebrity:
        return jsonify({"error": "請輸入人物名稱"}), 400
    interval = float(request.form.get("interval", 2))
    interval = max(0.5, min(interval, 10))

    vf = request.files["video"]
    ext = os.path.splitext(vf.filename)[1].lower() or ".mp4"
    tmp_path = os.path.join(UPLOAD_TMP, f"upload_{uuid.uuid4().hex[:8]}{ext}")
    vf.save(tmp_path)

    task_id = uuid.uuid4().hex[:12]
    q = Queue()

    def worker():
        import subprocess
        try:
            photo_dir = os.path.join(DOWNLOAD_ROOT, celebrity)
            os.makedirs(photo_dir, exist_ok=True)

            # 取得影片資訊
            ffmpeg = _get_ffmpeg()
            ffprobe = ffmpeg.replace("ffmpeg", "ffprobe")

            # 取得時長
            dur_cmd = [ffprobe, "-v", "quiet", "-show_entries",
                       "format=duration", "-of", "csv=p=0", tmp_path]
            try:
                dur_out = subprocess.run(dur_cmd, capture_output=True, text=True, timeout=10)
                duration = float(dur_out.stdout.strip())
            except Exception:
                duration = 0

            total_frames = int(duration / interval) if duration > 0 else 50
            q.put({"type": "progress", "percent": 5,
                   "message": f"影片長度 {duration:.0f}秒，預計擷取約 {total_frames} 張"})

            # 使用 ffmpeg 擷取畫面
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            pattern = os.path.join(photo_dir, f"vid_{ts}_%04d.jpg")
            cmd = [ffmpeg, "-i", tmp_path, "-vf", f"fps=1/{interval}",
                   "-q:v", "2", "-y", pattern]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _, stderr_out = proc.communicate(timeout=300)

            # 計算擷取了多少張
            extracted = []
            for f in sorted(os.listdir(photo_dir)):
                if f.startswith(f"vid_{ts}_") and f.endswith(".jpg"):
                    extracted.append(f)

            if not extracted:
                q.put({"type": "error", "message": "擷取失敗，ffmpeg 未產生任何圖片"})
                return

            q.put({"type": "progress", "percent": 80,
                   "message": f"✅ 擷取 {len(extracted)} 張，登記到資料庫..."})

            # 登記到 DB
            for fname in extracted:
                fp = os.path.join(photo_dir, fname)
                sz = os.path.getsize(fp) if os.path.isfile(fp) else 0
                md5 = hashlib.md5(open(fp, "rb").read()).hexdigest() if sz > 0 else ""
                db.add(celebrity, f"video_extract://{vf.filename}", fname, md5, "", sz, 0, 0, "video_extract")

            q.put({"type": "done", "count": len(extracted),
                   "message": f"✅ 從影片擷取 {len(extracted)} 張照片到 {celebrity}"})
        except Exception as e:
            logging.error(f"from-video error: {e}")
            q.put({"type": "error", "message": str(e)})
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    _from_video_tasks[task_id] = q
    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"ok": True, "task_id": task_id})


_from_video_tasks = {}


@app.route("/api/photos/from-video/progress/<task_id>")
def api_photos_from_video_progress(task_id):
    q = _from_video_tasks.get(task_id)
    if not q:
        return Response("data: {\"type\":\"error\",\"message\":\"task not found\"}\n\n",
                        mimetype="text/event-stream")

    def generate():
        while True:
            try:
                evt = q.get(timeout=30)
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
                if evt.get("type") in ("done", "error"):
                    _from_video_tasks.pop(task_id, None)
                    break
            except Empty:
                yield f"data: {{\"type\":\"heartbeat\"}}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


# ── 片段瀏覽 & 手動合成 ──────────────────────────────────
@app.route("/api/yt/clips/<person>")
def api_yt_clips(person):
    """列出某人的所有擷取片段（含縮圖路徑和時長）"""
    person = sanitize_name(person)
    ext_dir = os.path.join(YT_EXTRACTS, person)
    if not os.path.isdir(ext_dir):
        return jsonify([])

    clips = []
    for root, dirs, files in os.walk(ext_dir):
        for f in files:
            if not f.endswith(".mp4") or f.startswith("auto_"):
                continue
            fp = os.path.join(root, f)
            if os.path.getsize(fp) < 10240:
                continue
            rel = os.path.relpath(fp, YT_ROOT).replace("\\", "/")
            # 取得時長
            dur = 0
            try:
                cap = cv2.VideoCapture(fp)
                if cap.isOpened():
                    fps_ = cap.get(cv2.CAP_PROP_FPS) or 30
                    fc_ = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    dur = round(fc_ / fps_, 1) if fps_ > 0 else 0
                    cap.release()
            except Exception:
                pass
            vtype = _infer_video_type_from_path(fp, person)
            clips.append({
                "filename": f,
                "rel_path": rel,
                "url": f"/yt-files/{rel}",
                "thumb_url": f"/api/yt/clip-thumb/{rel}",
                "duration": dur,
                "size_kb": round(os.path.getsize(fp) / 1024),
                "type": vtype or "other",
            })
    clips.sort(key=lambda x: x["filename"])
    return jsonify(clips)


# 片段縮圖（擷取第 1 秒的畫面）
_thumb_cache = {}

@app.route("/api/yt/clip-thumb/<path:rel_path>")
def api_yt_clip_thumb(rel_path):
    """產生影片片段的縮圖"""
    full = os.path.normpath(os.path.join(YT_ROOT, rel_path))
    if not full.startswith(os.path.normpath(YT_ROOT)):
        return "Forbidden", 403
    if not os.path.isfile(full):
        return "Not found", 404

    # 快取
    mtime = os.path.getmtime(full)
    cache_key = f"{full}:{mtime}"
    if cache_key in _thumb_cache:
        return Response(_thumb_cache[cache_key], mimetype="image/jpeg",
                        headers={"Cache-Control": "public, max-age=86400"})

    try:
        cap = cv2.VideoCapture(full)
        cap.set(cv2.CAP_PROP_POS_MSEC, 1000)
        ret, frame = cap.read()
        cap.release()
        if not ret:
            return "Failed", 500
        # 縮小到 240px 寬
        h, w = frame.shape[:2]
        new_w = 240
        new_h = int(h * new_w / w)
        frame = cv2.resize(frame, (new_w, new_h))
        _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
        data = buf.tobytes()
        _thumb_cache[cache_key] = data
        return Response(data, mimetype="image/jpeg",
                        headers={"Cache-Control": "public, max-age=86400"})
    except Exception as e:
        return str(e), 500


@app.route("/api/yt/compile-selected", methods=["POST"])
def api_yt_compile_selected():
    """從手動選擇的片段合成影片"""
    data = request.get_json(force=True)
    person = sanitize_name((data.get("person") or "").strip())
    clips_input = data.get("clips", [])  # [{rel_path, start, duration}]
    clip_duration = float(data.get("clip_duration", 3))
    resolution = data.get("resolution", "720p")
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    audio_mode = data.get("audio_mode", "original")

    if not person or not clips_input:
        return jsonify({"error": "缺少人物名稱或片段"}), 400

    task_id = uuid.uuid4().hex[:12]
    q = Queue()

    def worker():
        try:
            # 建立 clips 列表（格式和 _compile_highlight 需要的一樣）
            clips = []
            for ci in clips_input:
                rp = ci.get("rel_path", "")
                fp = os.path.normpath(os.path.join(YT_ROOT, rp))
                if not fp.startswith(os.path.normpath(YT_ROOT)):
                    continue
                if not os.path.isfile(fp):
                    continue
                clips.append({
                    "video": fp,
                    "time": float(ci.get("start", 0)),
                    "score": 1.0,
                })

            if not clips:
                q.put({"type": "error", "message": "沒有有效的片段"})
                return

            q.put({"type": "progress", "phase": "highlight", "percent": 10,
                   "message": f"📦 準備合成 {len(clips)} 個片段..."})

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_name = f"manual_{person}_{ts}.mp4"
            output_dir = os.path.join(YT_ROOT, "output", person)
            os.makedirs(output_dir, exist_ok=True)
            out_path = os.path.join(output_dir, out_name)

            def compile_cb(pct):
                p = 10 + int(pct * 85)
                q.put({"type": "progress", "phase": "highlight", "percent": min(p, 95),
                       "message": f"合成影片 {int(pct*100)}%"})

            result = _compile_highlight(
                clips, clip_duration, out_path, transition,
                transition_dur, resolution, compile_cb,
                audio_mode=audio_mode,
            )

            if result and os.path.isfile(result):
                rel_path = os.path.relpath(result, YT_ROOT).replace("\\", "/")
                fsize = os.path.getsize(result)
                if transition in ("crossfade", "fade") and transition_dur > 0 and len(clips) > 1:
                    actual_dur = len(clips) * clip_duration - (len(clips) - 1) * transition_dur
                else:
                    actual_dur = len(clips) * clip_duration
                q.put({
                    "type": "done",
                    "filename": out_name,
                    "rel_path": rel_path,
                    "file_url": f"/yt-files/{rel_path}",
                    "file_size_mb": round(fsize / 1024 / 1024, 1),
                    "clips_count": len(clips),
                    "duration": round(actual_dur, 1),
                    "downloaded": 0,
                    "extracted": 0,
                })
            else:
                q.put({"type": "error", "message": "FFmpeg 合成失敗"})
        except Exception as e:
            logging.error(f"compile-selected error: {e}")
            q.put({"type": "error", "message": str(e)})

    with auto_video_tasks_lock:
        auto_video_tasks[task_id] = q
    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"ok": True, "task_id": task_id})


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
    "similarity_threshold": 0.40, # SFace cosine similarity 門檻 (SFace 建議 0.363，提高到 0.40 減少誤判)
    "merge_gap": 3.0,             # 時間段合併間隔（秒）
    "padding": 0.5,               # 每段前後緩衝（秒）
    "max_ref_photos": 80,         # 最多用幾張參考照
    "top_k_confirm": 3,           # 要求 top-K 張參考照都同意才算匹配（減少單張照片雜訊）
}


# ── 輸出平台預設：自動設定解析度、時長、響度、音訊編碼 ──
# 使用者選 preset 時，resolution/total_duration/loudnorm 會被覆寫為該平台建議值
OUTPUT_PRESETS = {
    "tiktok": {
        "label": "TikTok",
        "resolution": "1080p_v",   # 1080x1920 垂直
        "total_duration": 30,      # 預設 30 秒
        "loudnorm_I": -14,         # -14 LUFS（TikTok 標準）
        "loudnorm_TP": -1.5,
        "loudnorm_LRA": 11,
        "fps": 30,
    },
    "yt_shorts": {
        "label": "YouTube Shorts",
        "resolution": "1080p_v",   # 1080x1920
        "total_duration": 45,      # 60 秒內
        "loudnorm_I": -14,         # YouTube 標準
        "loudnorm_TP": -1.5,
        "loudnorm_LRA": 11,
        "fps": 30,
    },
    "ig_reels": {
        "label": "Instagram Reels",
        "resolution": "1080p_v",   # 1080x1920
        "total_duration": 30,      # 90 秒內
        "loudnorm_I": -14,         # Meta 標準
        "loudnorm_TP": -1.5,
        "loudnorm_LRA": 11,
        "fps": 30,
    },
    "youtube": {
        "label": "YouTube 橫式",
        "resolution": "1080p",     # 1920x1080
        "total_duration": 60,
        "loudnorm_I": -14,
        "loudnorm_TP": -1.5,
        "loudnorm_LRA": 11,
        "fps": 30,
    },
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


def _build_neg_embeddings(person_name):
    """
    建立「反參考」臉部特徵，用於拒絕其他人被誤判為目標。

    優先順序：
      1. {person_folder}/.negatives/ 手動放的照片
      2. 自動：從 DOWNLOAD_ROOT 下其他人物的 .face_embeddings.npy 合併
         （所有 sibling 資料夾的正參考 = 本人的反參考）
    """
    photo_dir = os.path.join(DOWNLOAD_ROOT, person_name, ".negatives")

    # ── 方案 1：手動 .negatives/ 資料夾 ──
    if os.path.isdir(photo_dir):
        cache_path = os.path.join(photo_dir, ".neg_embeddings.npy")
        if os.path.isfile(cache_path):
            try:
                return np.load(cache_path)
            except Exception:
                pass

        import glob as _glob
        import random as _random
        exts = ["*.jpg", "*.jpeg", "*.png", "*.webp"]
        files = []
        for ext in exts:
            files.extend(_glob.glob(os.path.join(photo_dir, "**", ext), recursive=True))
        if len(files) >= 2:
            max_photos = EXTRACT_CONFIG["max_ref_photos"]
            if len(files) > max_photos:
                _random.shuffle(files)
                files = files[:max_photos]

            detector, recognizer = _build_face_models()
            embeddings = []
            for fpath in files:
                try:
                    img = cv2.imread(fpath)
                    if img is None:
                        continue
                    h, w = img.shape[:2]
                    detector.setInputSize((w, h))
                    _, faces = detector.detect(img)
                    if faces is None or len(faces) == 0:
                        continue
                    areas = [f[2] * f[3] for f in faces]
                    best = faces[int(np.argmax(areas))]
                    aligned = recognizer.alignCrop(img, best)
                    emb = recognizer.feature(aligned).flatten()
                    embeddings.append(emb)
                except Exception:
                    continue

            if len(embeddings) >= 2:
                mat = np.stack(embeddings)
                norms = np.linalg.norm(mat, axis=1, keepdims=True)
                norms[norms == 0] = 1
                mat = mat / norms
                try:
                    np.save(cache_path, mat)
                except Exception:
                    pass
                return mat

    # ── 方案 2：自動從 sibling 人物資料夾收集 ──
    # 把 DOWNLOAD_ROOT 下其他人物的已快取 .face_embeddings.npy 合併成反參考
    try:
        all_neg = []
        person_lower = person_name.lower()
        for entry in os.listdir(DOWNLOAD_ROOT):
            if entry.lower() == person_lower:
                continue
            sibling_emb = os.path.join(DOWNLOAD_ROOT, entry, ".face_embeddings.npy")
            if os.path.isfile(sibling_emb):
                try:
                    mat = np.load(sibling_emb)
                    if mat.ndim == 2 and mat.shape[0] >= 3:
                        # 每個 sibling 最多取 10 張，避免反參考過大
                        if mat.shape[0] > 10:
                            idx = np.random.choice(mat.shape[0], 10, replace=False)
                            mat = mat[idx]
                        all_neg.append(mat)
                except Exception:
                    continue
        if all_neg:
            combined = np.vstack(all_neg)
            norms = np.linalg.norm(combined, axis=1, keepdims=True)
            norms[norms == 0] = 1
            combined = combined / norms
            logging.info(f"auto-neg for {person_name}: {len(all_neg)} siblings, "
                         f"{combined.shape[0]} total embeddings")
            return combined
    except Exception:
        pass

    return None


# 反參考門檻：目標分數必須比最接近的反參考至少高出這個值才算通過
NEG_MARGIN = 0.08


def _passes_negative_check(emb, ref_embeddings, neg_embeddings, target_sim):
    """
    判斷某個 embedding 是否「明顯比較像目標人物而非反參考群」。
    emb: 正規化後的 embedding (shape=(D,))
    ref_embeddings: 正參考 (shape=(N, D), 已正規化)
    neg_embeddings: 反參考 (shape=(M, D), 已正規化), None 時直接通過
    target_sim: 該 emb 對 ref 的最大相似度（已算好）
    """
    if neg_embeddings is None or len(neg_embeddings) == 0:
        return True
    try:
        neg_sims = neg_embeddings @ emb
        max_neg = float(np.max(neg_sims))
        return (target_sim - max_neg) >= NEG_MARGIN
    except Exception:
        return True


def _scan_video_for_person(video_path, ref_embeddings, progress_cb=None, neg_embeddings=None):
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
            frame_area = h * w
            top_k = min(EXTRACT_CONFIG.get("top_k_confirm", 3), len(ref_embeddings))
            for face in faces:
                try:
                    # 跳過太小的臉（< 0.8% 畫面面積），避免匹配遠處背景中的人
                    face_area = face[2] * face[3]
                    if face_area / frame_area < 0.008:
                        continue
                    aligned = recognizer.alignCrop(frame, face)
                    emb = recognizer.feature(aligned).flatten()
                    emb_norm = np.linalg.norm(emb)
                    if emb_norm > 0:
                        emb = emb / emb_norm
                    # cosine similarity vs all references
                    sims = ref_embeddings @ emb
                    max_sim = float(np.max(sims))
                    if max_sim >= threshold:
                        # 額外檢查：要求 top-K 張參考照都有一定相似度
                        # 避免僅靠單張低品質照片就通過門檻造成誤判
                        top_k_mean = float(np.sort(sims)[-top_k:].mean())
                        if top_k_mean >= threshold * 0.85:
                            # 反參考檢查：必須「明顯比較像目標」而非團員
                            if _passes_negative_check(emb, ref_embeddings, neg_embeddings, max_sim):
                                timestamps.append(t)
                                break
                except Exception:
                    continue

        t += interval
        if progress_cb:
            progress_cb(t / duration if duration > 0 else 1)

    cap.release()
    return timestamps


def _fetch_youtube_thumbnail(video_id, timeout=5):
    """下載 YouTube 影片縮圖。優先 maxres，失敗 fallback 到 hq。回傳 np.ndarray (BGR) 或 None。"""
    if not video_id:
        return None
    import urllib.request
    urls = [
        f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            if not data or len(data) < 500:
                continue
            arr = np.frombuffer(data, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is None:
                continue
            # YouTube 預設縮圖有時會回傳 120x90 的灰色佔位圖，檢查一下尺寸
            h, w = img.shape[:2]
            if h < 100 or w < 100:
                continue
            return img
        except Exception:
            continue
    return None


def _thumbnail_face_match(img, ref_embeddings, detector, recognizer,
                           threshold=0.35, neg_embeddings=None):
    """
    檢查縮圖是否含有目標人物。
    回傳 (status, score):
        status == "match"    → 通過（縮圖有目標人物且明顯比反參考更像）
        status == "reject"   → 淘汰（縮圖有臉但不像或比較像反參考）
        status == "no_face"  → 跳過（縮圖沒偵測到臉，不淘汰）
        status == "error"    → 跳過（處理失敗，不淘汰）
    """
    try:
        h, w = img.shape[:2]
        detector.setInputSize((w, h))
        _, faces = detector.detect(img)
        if faces is None or len(faces) == 0:
            return "no_face", 0.0
        best_score = 0.0
        best_passes_neg = False
        for face in faces:
            try:
                aligned = recognizer.alignCrop(img, face)
                emb = recognizer.feature(aligned).flatten()
                norm = np.linalg.norm(emb)
                if norm > 0:
                    emb = emb / norm
                sims = ref_embeddings @ emb
                score = float(np.max(sims))
                if score >= threshold:
                    passes = _passes_negative_check(emb, ref_embeddings, neg_embeddings, score)
                    if passes and score > best_score:
                        best_score = score
                        best_passes_neg = True
                elif score > best_score and not best_passes_neg:
                    best_score = score
            except Exception:
                continue
        if best_score >= threshold and best_passes_neg:
            return "match", best_score
        return "reject", best_score
    except Exception:
        return "error", 0.0


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
            person_folder = sanitize_name(person)
            vtype = _infer_video_type_from_path(video_path, person_folder)
            if vtype:
                extract_dir = os.path.join(YT_EXTRACTS, person_folder, vtype)
            else:
                extract_dir = os.path.join(YT_EXTRACTS, person_folder)
            os.makedirs(extract_dir, exist_ok=True)
            out_path = os.path.join(extract_dir, out_name)
            result = _extract_segments(video_path, segments, out_path)

            if result and os.path.isfile(result):
                rel_path = os.path.relpath(result, YT_ROOT).replace("\\", "/")
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


@app.route("/api/yt/extract-batch", methods=["POST"])
def api_yt_extract_batch():
    """批次擷取所有未擷取的本地影片"""
    if not HAS_FACE_MODELS or not HAS_CV2:
        return jsonify({"error": "人臉模型或 OpenCV 未安裝"}), 500

    data = request.json
    person = (data.get("person") or "").strip()
    if not person:
        return jsonify({"error": "需要 person"}), 400
    person = resolve_celebrity(person)
    person_folder = sanitize_name(person)

    # 找所有本地影片
    dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
    exts = {".mp4", ".mkv", ".webm"}
    all_videos = []
    if os.path.isdir(dl_dir):
        for root, dirs, files in os.walk(dl_dir):
            for f in files:
                fp = os.path.join(root, f)
                if os.path.splitext(f)[1].lower() in exts \
                   and os.path.getsize(fp) > 10240 and not f.endswith(".part"):
                    all_videos.append(fp)

    # 找已擷取的（遞迴掃描，含子資料夾）
    _psuffix = f"_{person_folder}.mp4"
    existing = set(_scan_extract_files(person_folder, _psuffix).keys())

    need = [(fp, os.path.basename(fp)) for fp in all_videos
            if os.path.splitext(os.path.basename(fp))[0] not in existing]

    if not need:
        return jsonify({"message": f"全部 {len(all_videos)} 部影片都已擷取完成", "remaining": 0})

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    with extract_tasks_lock:
        extract_tasks[task_id] = {"queue": q}

    def worker():
        try:
            total = len(need)
            q.put({"type": "progress", "phase": "embeddings", "percent": 0,
                   "message": f"批次擷取 {total} 部影片，建立臉部特徵..."})

            def emb_cb(pct):
                q.put({"type": "progress", "phase": "embeddings",
                       "percent": int(pct * 5),
                       "message": f"建立臉部特徵 {int(pct*100)}%"})

            ref = _build_reference_embeddings(person, emb_cb)
            if ref is None or len(ref) < 3:
                q.put({"type": "error",
                       "message": f"{person} 的照片中找不到足夠的臉部特徵（至少需要 3 張）"})
                return

            q.put({"type": "progress", "phase": "scanning", "percent": 5,
                   "message": f"使用 {len(ref)} 張臉部特徵，開始掃描 {total} 部影片..."})

            success = 0
            for vi, (video_path, vname) in enumerate(need):
                pct_base = 5 + int(vi / total * 90)
                q.put({"type": "progress", "phase": "scanning", "percent": pct_base,
                       "message": f"掃描 {vi+1}/{total}: {vname[:40]}..."})

                try:
                    def scan_cb(pct, _vi=vi):
                        p = 5 + int((_vi + pct) / total * 90)
                        q.put({"type": "progress", "phase": "scanning",
                               "percent": min(p, 95),
                               "message": f"掃描 {vname[:30]}... {int(pct*100)}%"})

                    timestamps = _scan_video_for_person(video_path, ref, scan_cb)
                    if not timestamps:
                        q.put({"type": "progress", "phase": "scanning",
                               "percent": pct_base + 2,
                               "message": f"⏭️ {vname[:40]} 中未偵測到 {person}"})
                        continue

                    segments = _merge_segments(timestamps)
                    # 去除片尾
                    try:
                        _cap = cv2.VideoCapture(video_path)
                        if _cap.isOpened():
                            _fps = _cap.get(cv2.CAP_PROP_FPS) or 30
                            _fc = _cap.get(cv2.CAP_PROP_FRAME_COUNT)
                            _src_dur = _fc / _fps if _fps > 0 else None
                            _cap.release()
                            if _src_dur and _src_dur > 10:
                                cutoff = _src_dur - 5.0
                                segments = [(s, min(e, cutoff)) for s, e in segments
                                            if s < cutoff and min(e, cutoff) - s >= 1.0]
                    except Exception:
                        pass

                    if not segments:
                        continue

                    base = os.path.splitext(vname)[0]
                    out_name = f"{base}_{person_folder}.mp4"
                    vtype = _infer_video_type_from_path(video_path, person_folder)
                    if vtype:
                        _typed_dir = os.path.join(YT_EXTRACTS, person_folder, vtype)
                    else:
                        _typed_dir = os.path.join(YT_EXTRACTS, person_folder)
                    os.makedirs(_typed_dir, exist_ok=True)
                    out_path = os.path.join(_typed_dir, out_name)
                    result = _extract_segments(video_path, segments, out_path)
                    if result and os.path.isfile(result):
                        success += 1
                        total_dur = sum(e - s for s, e in segments)
                        q.put({"type": "progress", "phase": "scanning",
                               "percent": pct_base + 3,
                               "message": f"✅ {vname[:30]}: {len(segments)} 段 ({total_dur:.0f}秒)"})
                except Exception as e:
                    logging.warning(f"batch-extract error on {vname}: {e}")
                    continue

            q.put({"type": "done",
                   "message": f"批次擷取完成：{success}/{total} 部成功",
                   "success": success, "total": total})

        except Exception as e:
            q.put({"type": "error", "message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id, "remaining": len(need),
                    "message": f"開始批次擷取 {len(need)} 部影片"})


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
    "closeup":  {"face_weight": 0.8, "motion_weight": 0.0, "audio_weight": 0.2, "label": "特寫優先"},
    "dynamic":  {"face_weight": 0.0, "motion_weight": 0.7, "audio_weight": 0.3, "label": "動態優先"},
    "balanced": {"face_weight": 0.45, "motion_weight": 0.30, "audio_weight": 0.25, "label": "均衡"},
    "random":   {"face_weight": 0.0, "motion_weight": 0.0, "audio_weight": 0.0, "label": "隨機"},
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


def _detect_video_vertical(video_path):
    """偵測影片是否為直式內容（含處理黑邊和旋轉 metadata）。
    比單純 frame.shape h > w 更準確，能處理 TikTok 在橫式容器中放直式內容的情況。
    使用嚴格黑邊偵測：要求邊緣是「均勻純黑」（低亮度 + 低變異），避免誤判暗色內容。
    """
    import subprocess as _sp

    # 方法1: ffprobe 檢查影片實際尺寸和旋轉 metadata
    probe_w, probe_h = 0, 0
    try:
        ffmpeg = _get_ffmpeg()
        ffprobe = ffmpeg.replace("ffmpeg", "ffprobe")
        if not os.path.isfile(ffprobe):
            _fp = shutil.which("ffprobe")
            if _fp:
                ffprobe = _fp
        cmd = [ffprobe, '-v', 'quiet', '-print_format', 'json',
               '-show_streams', video_path]
        r = _sp.run(cmd, capture_output=True, timeout=10)
        if r.returncode == 0:
            info = json.loads(r.stdout.decode('utf-8', errors='replace'))
            for stream in info.get('streams', []):
                if stream.get('codec_type') == 'video':
                    probe_w = int(stream.get('width', 0))
                    probe_h = int(stream.get('height', 0))
                    rotation = 0
                    # 檢查 tags 中的 rotate
                    rotation = abs(int(stream.get('tags', {}).get('rotate', '0')))
                    # 檢查 side_data_list 中的 rotation
                    for sd in stream.get('side_data_list', []):
                        if 'rotation' in sd:
                            rotation = abs(int(sd['rotation']))
                    if rotation in (90, 270):
                        probe_w, probe_h = probe_h, probe_w
                    # ffprobe 明確顯示直式 → 直接回傳
                    if probe_w > 0 and probe_h > 0 and probe_h > probe_w:
                        return True
                    break
    except Exception:
        pass

    # 方法2: 分析畫面內容，偵測黑邊（直式內容放在橫式容器中）
    # 嚴格條件：黑邊必須是「均勻的純黑」（mean < 10, stddev < 8），
    # 且內容寬度要比容器窄超過 30%，才判定為直式
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return False

        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        duration = total_frames / fps if fps > 0 else 0

        # 多點取樣，需要多數同意
        sample_times = [duration * p for p in (0.25, 0.5, 0.7) if duration * p > 0.5]
        if not sample_times:
            sample_times = [0.5]

        vertical_votes = 0
        total_votes = 0

        for sample_time in sample_times:
            cap.set(cv2.CAP_PROP_POS_MSEC, sample_time * 1000)
            ret, frame = cap.read()
            if not ret:
                continue
            total_votes += 1

            h, w = frame.shape[:2]

            # 已經是直式 container
            if h > w:
                cap.release()
                return True

            # 轉灰階分析黑邊
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # 檢查左右邊緣是否為「均勻純黑」（letterbox 特徵）
            edge_w = max(2, w // 15)  # ~7% of width
            left_strip = gray[:, :edge_w]
            right_strip = gray[:, -edge_w:]

            left_mean = float(left_strip.mean())
            right_mean = float(right_strip.mean())
            left_std = float(left_strip.std())
            right_std = float(right_strip.std())

            # 嚴格條件：真正的黑邊是均勻且非常暗的
            # 暗色內容（如深色頭髮、衣服）通常有較高的 std
            is_black_bars = (left_mean < 10 and right_mean < 10
                            and left_std < 8 and right_std < 8)

            if is_black_bars:
                # 找出實際內容邊界（用更嚴格的亮度門檻）
                col_means = gray.mean(axis=0)

                content_left = 0
                for c in range(w):
                    if col_means[c] > 15:
                        content_left = c
                        break

                content_right = w - 1
                for c in range(w - 1, -1, -1):
                    if col_means[c] > 15:
                        content_right = c
                        break

                content_width = content_right - content_left + 1
                # 直式內容在橫式容器：寬度通常只佔 ~56% (9:16 in 16:9)
                # 要求內容寬度比容器窄超過 25%，且高度明顯大於內容寬度
                if content_width < w * 0.75 and h > content_width * 1.2:
                    vertical_votes += 1

        cap.release()

        # 需要多數取樣點同意才判定為直式
        if total_votes > 0 and vertical_votes > total_votes / 2:
            return True

    except Exception:
        pass

    return False


def _extract_audio_energy(video_path, interval=1.0):
    """
    用 ffmpeg 抽取單聲道 16kHz PCM，計算每 interval 秒的 RMS 能量。
    用途：
      1. 評分 — 副歌/高潮段通常能量較大（作為 highlight 強訊號）
      2. 切點對齊 — 把切點對齊到局部最小能量（自然的安靜間隙）
    回傳 np.ndarray shape=(num_windows,) 正規化到 [0, 1]，失敗則 None。
    """
    try:
        import subprocess as _sp
        ffmpeg = _get_ffmpeg()
        cmd = [ffmpeg, "-i", video_path, "-vn", "-ac", "1", "-ar", "16000",
               "-f", "s16le", "-hide_banner", "-loglevel", "error", "-"]
        proc = _sp.run(cmd, capture_output=True, timeout=180)
        if proc.returncode != 0 or not proc.stdout:
            return None
        samples = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
        sr = 16000
        window = int(sr * interval)
        if window <= 0 or len(samples) < window:
            return None
        num = len(samples) // window
        if num == 0:
            return None
        trimmed = samples[:num * window]
        arr = trimmed.reshape(num, window)
        rms = np.sqrt(np.mean(arr ** 2, axis=1))
        peak = float(np.max(rms))
        if peak > 0:
            rms = rms / peak
        return rms.astype(np.float32)
    except Exception as e:
        logging.warning(f"audio energy extract failed for {video_path}: {e}")
        return None


def _snap_cut_to_quiet(audio_energy, t_sec, window_sec=1.0):
    """
    把切點 t_sec 對齊到 ±window_sec 範圍內音訊能量最低的位置（最安靜的瞬間）。
    這接近「在歌曲自然停頓處切」，比在節拍中間硬切更自然。
    若無音訊資料就回傳原始 t_sec。
    """
    if audio_energy is None or len(audio_energy) == 0:
        return t_sec
    idx = int(round(t_sec))
    w = max(1, int(round(window_sec)))
    lo = max(0, idx - w)
    hi = min(len(audio_energy), idx + w + 1)
    if hi <= lo:
        return t_sec
    local = audio_energy[lo:hi]
    min_idx = int(np.argmin(local))
    return float(lo + min_idx)


def _score_video_highlights(video_path, strategy, progress_cb=None,
                              ref_embeddings=None, neg_embeddings=None):
    """對影片每秒評分，找出精彩時刻。
    ref_embeddings: 如提供，使用人臉辨識只計算「目標人物」的臉部面積（而非任何人的臉）。
    neg_embeddings: 如提供，用反參考拒絕其他團員被誤判為目標人物。
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    duration = total_frames / fps if fps > 0 else 0
    if duration < 2:
        cap.release()
        return []

    # 偵測影片方向（只需一次，比每幀 h > w 更準確）
    is_vertical = _detect_video_vertical(video_path)

    # 跳過最後 5 秒，避免 TikTok 品牌片尾（黑底 + Logo 動畫）
    usable_end = max(2.0, duration - 5.0)

    # 舞台類型強制改用 closeup 策略：音樂節目常見群舞/遠景/轉場卡，
    # 若 face_weight 不夠大，動作 + 音訊會把沒人的幀推高。
    _norm_path = video_path.replace("\\", "/").lower()
    effective_strategy = strategy
    if strategy != "random" and ("/stage/" in _norm_path or _norm_path.endswith("/stage")):
        effective_strategy = "closeup"
        logging.info(f"stage video → closeup strategy: {os.path.basename(video_path)}")

    weights = HIGHLIGHT_STRATEGIES.get(effective_strategy, HIGHLIGHT_STRATEGIES["balanced"])
    face_w = weights["face_weight"]
    motion_w = weights["motion_weight"]
    is_random = (effective_strategy == "random")

    detector = None
    recognizer = None
    if face_w > 0 and HAS_CV2:
        detector, recognizer = _build_face_models()
    # 人臉辨識門檻（用於 scoring 時辨認目標人物，略低於 extraction 門檻以增加容錯）
    _face_threshold = EXTRACT_CONFIG["similarity_threshold"] * 0.90
    _has_person_ref = (ref_embeddings is not None and len(ref_embeddings) > 0
                       and recognizer is not None)

    # 抽取音訊能量（每秒一個 RMS 值，正規化到 [0,1]）— 用於副歌偵測 + 切點對齊
    audio_w = weights.get("audio_weight", 0.0)
    audio_energy = None
    if audio_w > 0 or True:  # 即使權重為 0 也抽取，切點對齊階段會用到
        audio_energy = _extract_audio_energy(video_path, interval=1.0)

    scores = []
    prev_gray = None
    t = 0.0

    while t < usable_end:
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
        ret, frame = cap.read()
        if not ret:
            t += 1.0
            continue

        h, w = frame.shape[:2]
        frame_area = h * w

        # 偵測暗黑畫面（TikTok 片尾通常是黑色背景，mean < 30）
        mean_brightness = frame.mean()
        if mean_brightness < 30:
            t += 1.0
            if progress_cb:
                progress_cb(t / duration)
            continue

        # 臉部評分
        face_score = 0.0
        n_faces = 0
        target_area = 0
        other_max_area = 0
        if detector and face_w > 0:
            detector.setInputSize((w, h))
            _, faces = detector.detect(frame)
            if faces is not None and len(faces) > 0:
                n_faces = int(len(faces))
                if _has_person_ref:
                    # ★ 人物專屬評分：只計算「目標人物」的臉部面積
                    # 同時記錄其他臉的最大面積供 per-clip 多人偵測使用
                    best_person_area = 0
                    for f in faces:
                        area = f[2] * f[3]
                        is_target = False
                        try:
                            aligned = recognizer.alignCrop(frame, f)
                            emb = recognizer.feature(aligned).flatten()
                            emb_norm = np.linalg.norm(emb)
                            if emb_norm > 0:
                                emb = emb / emb_norm
                            sims = ref_embeddings @ emb
                            max_sim = float(np.max(sims))
                            if max_sim >= _face_threshold:
                                if _passes_negative_check(emb, ref_embeddings,
                                                           neg_embeddings, max_sim):
                                    is_target = True
                                    best_person_area = max(best_person_area, area)
                        except Exception:
                            pass
                        if not is_target and area > other_max_area:
                            other_max_area = area
                    target_area = best_person_area
                    face_score = min(best_person_area / frame_area * 10, 1.0)
                else:
                    # 通用評分：任何人的最大臉
                    areas = [f[2] * f[3] for f in faces]
                    face_score = min(max(areas) / frame_area * 10, 1.0)
                    target_area = max(areas)

        # 動態評分：幀間差異
        motion_score = 0.0
        if motion_w > 0:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            if prev_gray is not None:
                diff = cv2.absdiff(gray, prev_gray)
                motion_score = min(diff.mean() / 30.0, 1.0)
            prev_gray = gray

        # 音訊能量評分
        audio_score = 0.0
        if audio_energy is not None:
            ai = int(t)
            if 0 <= ai < len(audio_energy):
                audio_score = float(audio_energy[ai])

        if is_random:
            import random as _rng
            combined = _rng.random()
        else:
            combined = (face_w * face_score + motion_w * motion_score
                        + audio_w * audio_score)
            # 硬否決：如有參考照片卻完全沒偵測到目標臉（或目標臉太小），直接 0 分
            # 避免轉場卡、遠景舞台、群舞中目標不明的幀被動作/音訊推高
            # 0.12：擋掉手機螢幕內小臉、嵌入 PIP、群拍遠景
            # （0.15 太狠會把舞台廣播切到 < 20s；0.10 又會放過 PIP 小臉）
            if _has_person_ref and face_w > 0 and face_score < 0.12:
                combined = 0.0

        scores.append({"time": t, "score": combined, "duration": duration,
                       "is_vertical": is_vertical,
                       "audio_energy": audio_score,
                       "n_faces": n_faces,
                       "target_area": float(target_area) / max(frame_area, 1),
                       "other_max_area": float(other_max_area) / max(frame_area, 1)})
        t += 1.0
        if progress_cb:
            progress_cb(t / duration)

    cap.release()
    return scores


def _embeddings_sig(emb):
    """產生 embeddings 的輕量指紋（shape + sum），用來偵測照片庫是否變動。"""
    if emb is None:
        return "none"
    try:
        return f"{emb.shape[0]}x{emb.shape[1]}_{float(emb.sum()):.4f}"
    except Exception:
        return "unknown"


def _video_cache_path(video_path):
    """每個影片對應的掃描/評分快取檔路徑。"""
    return video_path + ".scan.json"


def _load_video_cache(video_path):
    """載入影片的掃描/評分快取。失效或不存在回傳 None。"""
    import json
    cache_path = _video_cache_path(video_path)
    if not os.path.isfile(cache_path):
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    # 驗證影片 mtime（如果影片被替換就作廢）
    try:
        cur_mtime = os.path.getmtime(video_path)
    except OSError:
        return None
    if abs(data.get("mtime", 0) - cur_mtime) > 1.0:
        return None
    return data


def _save_video_cache(video_path, updates):
    """增量更新影片快取（只覆寫提供的 key）。"""
    import json
    cache_path = _video_cache_path(video_path)
    data = {}
    if os.path.isfile(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                data = existing
        except Exception:
            data = {}
    try:
        data["mtime"] = os.path.getmtime(video_path)
    except OSError:
        return
    data.update(updates)
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logging.debug(f"save video cache failed: {e}")


def _clip_history_path(person):
    """取得片段使用記錄檔路徑"""
    if not person:
        return None
    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)
    return os.path.join(extract_dir, "_clip_history.json")


def _load_clip_history(person):
    """載入已使用的片段記錄 → set of (basename, rounded_time)"""
    import json
    path = _clip_history_path(person)
    if not path or not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {(e["video"], e["time"]) for e in data if "video" in e and "time" in e}
    except Exception:
        return set()


def _save_clip_history(person, clips):
    """將本次使用的片段追加到記錄"""
    import json
    path = _clip_history_path(person)
    if not path:
        return
    # 載入既有記錄
    existing = []
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
    # 追加新片段
    for c in clips:
        existing.append({
            "video": os.path.basename(c["video"]),
            "time": round(c["time"]),
            "ts": datetime.now().isoformat(),
        })
    # 只保留最近 500 筆，避免無限增長
    existing = existing[-500:]
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False)
    except Exception:
        pass


def _clip_blacklist_path(person):
    """取得片段黑名單檔路徑"""
    if not person:
        return None
    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)
    return os.path.join(extract_dir, "_clip_blacklist.json")


def _load_clip_blacklist(person):
    """載入黑名單 → set of (basename, rounded_time)"""
    path = _clip_blacklist_path(person)
    if not path or not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {(e["video"], e["time"]) for e in data if "video" in e and "time" in e}
    except Exception:
        return set()


def _add_to_clip_blacklist(person, video_basename, time_sec):
    """將片段加入黑名單（下次不再出現）"""
    path = _clip_blacklist_path(person)
    if not path:
        return
    existing = []
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
    # 避免重複
    key = (video_basename, round(time_sec))
    if any(e.get("video") == key[0] and e.get("time") == key[1] for e in existing):
        return
    existing.append({
        "video": video_basename,
        "time": round(time_sec),
        "ts": datetime.now().isoformat(),
    })
    # 保留最近 1000 筆
    existing = existing[-1000:]
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False)
    except Exception:
        pass


def _detect_face_center(video_path, t_sec, clip_dur=0):
    """偵測 clip 區間內多幀的主臉中心中位數，讓裁切框更穩定。
    取 clip 的 0%, 25%, 50%, 75% 四個時間點偵測臉部位置，
    回傳 x 中位數的 (cx, cy, W, H) 或 None。
    """
    try:
        import cv2 as _cv2
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None
        fps = cap.get(_cv2.CAP_PROP_FPS) or 25
        h = int(cap.get(_cv2.CAP_PROP_FRAME_HEIGHT))
        w = int(cap.get(_cv2.CAP_PROP_FRAME_WIDTH))
        if w <= 0 or h <= 0:
            cap.release()
            return None
        detector = _cv2.FaceDetectorYN.create(YUNET_MODEL, "", (w, h), 0.7, 0.3, 5000)

        # 多幀取樣：clip 的 0%, 25%, 50%, 75%
        if clip_dur > 1.0:
            sample_offsets = [0.0, 0.25, 0.50, 0.75]
        else:
            sample_offsets = [0.5]  # 短 clip 只取中間

        cx_list, cy_list = [], []
        for ratio in sample_offsets:
            sample_t = max(0, t_sec - clip_dur / 2 + clip_dur * ratio)
            cap.set(_cv2.CAP_PROP_POS_FRAMES, int(sample_t * fps))
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            _, faces = detector.detect(frame)
            if faces is None or len(faces) == 0:
                continue
            face = max(faces, key=lambda f: f[2] * f[3])
            cx_list.append(float(face[0] + face[2] / 2))
            cy_list.append(float(face[1] + face[3] / 2))

        cap.release()
        if not cx_list:
            return None
        # 取中位數 — 比平均更抗離群值
        cx_list.sort()
        cy_list.sort()
        mid = len(cx_list) // 2
        cx = cx_list[mid]
        cy = cy_list[mid]
        return (cx, cy, w, h)
    except Exception:
        return None


def _pick_cover_frame(clips, output_video_path):
    """從已選片段裡挑一幀當封面，存成 {output}.jpg。

    評分：face_area × face_centrality × sharpness。取最高分的 frame 存檔。
    """
    try:
        import cv2 as _cv2
        import numpy as _np
        if not clips:
            return None
        detector = _cv2.FaceDetectorYN.create(YUNET_MODEL, "", (320, 320), 0.7, 0.3, 5000)

        best_score = -1.0
        best_frame = None
        for c in clips[:8]:  # 只看前 8 個（主選夠了）
            vpath = c.get("video") if isinstance(c, dict) else None
            if not vpath or not os.path.isfile(vpath):
                continue
            cap = _cv2.VideoCapture(vpath)
            if not cap.isOpened():
                continue
            fps = cap.get(_cv2.CAP_PROP_FPS) or 25
            # 在片段中間 ± 0.5 秒取 3 幀
            mid_t = c["time"] + c.get("src_duration", 3) / 2 if False else c["time"] + 1.5
            sample_ts = [mid_t - 0.5, mid_t, mid_t + 0.5]
            for t in sample_ts:
                if t < 0:
                    continue
                cap.set(_cv2.CAP_PROP_POS_FRAMES, int(t * fps))
                ok, frame = cap.read()
                if not ok or frame is None:
                    continue
                h, w = frame.shape[:2]
                detector.setInputSize((w, h))
                _, faces = detector.detect(frame)
                if faces is None or len(faces) == 0:
                    continue
                # 取最大的臉
                face = max(faces, key=lambda f: f[2] * f[3])
                fx, fy, fw, fh = face[0], face[1], face[2], face[3]
                face_area_ratio = (fw * fh) / (w * h)
                # 中心度：臉中心距畫面中心的距離
                cx_face = fx + fw / 2
                cy_face = fy + fh / 2
                cx_img = w / 2
                cy_img = h / 2
                dist = ((cx_face - cx_img) ** 2 + (cy_face - cy_img) ** 2) ** 0.5
                max_dist = (cx_img ** 2 + cy_img ** 2) ** 0.5
                centrality = 1.0 - min(dist / max_dist, 1.0)
                # 清晰度（Laplacian variance）
                gray = _cv2.cvtColor(frame, _cv2.COLOR_BGR2GRAY)
                sharp = float(_cv2.Laplacian(gray, _cv2.CV_64F).var())
                sharp_norm = min(sharp / 500.0, 1.0)
                # 綜合分
                score = (face_area_ratio ** 0.5) * 0.5 + centrality * 0.3 + sharp_norm * 0.2
                if score > best_score:
                    best_score = score
                    best_frame = frame.copy()
            cap.release()

        if best_frame is None:
            return None
        cover_path = os.path.splitext(output_video_path)[0] + "_cover.jpg"
        _cv2.imwrite(cover_path, best_frame, [_cv2.IMWRITE_JPEG_QUALITY, 92])
        logging.info(f"cover picked, score={best_score:.3f} → {cover_path}")
        return cover_path
    except Exception as e:
        logging.warning(f"pick_cover_frame failed: {e}")
        return None


def _compute_frame_phash(video_path, t_sec):
    """擷取指定時間的影格，計算 64-bit pHash（DCT-based perceptual hash）。回傳 int 或 None。"""
    try:
        import cv2 as _cv2
        import numpy as _np
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None
        fps = cap.get(_cv2.CAP_PROP_FPS) or 25
        frame_idx = int(max(0, t_sec * fps))
        cap.set(_cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ok, frame = cap.read()
        cap.release()
        if not ok or frame is None:
            return None
        gray = _cv2.cvtColor(frame, _cv2.COLOR_BGR2GRAY)
        resized = _cv2.resize(gray, (32, 32), interpolation=_cv2.INTER_AREA).astype(_np.float32)
        dct = _cv2.dct(resized)
        # 取左上 8x8（低頻，排除 DC）
        dct_low = dct[:8, :8]
        med = _np.median(dct_low[1:].flatten() if dct_low.size > 1 else dct_low.flatten())
        bits = (dct_low > med).flatten()
        h = 0
        for b in bits[:64]:
            h = (h << 1) | int(b)
        return h
    except Exception:
        return None


def _phash_hamming(a, b):
    if a is None or b is None:
        return 64
    return bin(a ^ b).count("1")


# ── Fan-edit / 二次加工偵測 ──
# 避免抓到粉絲剪輯合集、split-screen 比較、套模板貼字的二次加工內容
_FAN_EDIT_KEYWORDS = (
    " edit", " edits", "edited", "compilation", "mashup", "mash-up",
    "fmv", "f.m.v", "fan edit", "fanedit",
    " vs ", " vs.", " vs:", "versus", " v.s ",
    "transition", " comp ", " comp.",
    "best moments", "cute moments", "funny moments",
    "best of ", "top 10", "top10", "top 5", "top5",
    "reaction", " meme", "memes",
    "tiktok edit", "tiktok comp", "see the difference",
    "parody", "cover by", " cover ", "duet",
    "raising the expectations",
    # 中韓日
    "편집", "모음", "편집본", "剪輯", "合集", "比較", "まとめ", "커버",
)
_FAN_EDIT_SAFE_KEYWORDS = (
    "fancam", "fan cam", "직캠",
    "stage", "live stage", "performance",
    "dance practice", "practice",
    "showcase", "music video", "official",
    "behind", "making", "rehearsal",
)


def _check_fan_edit_keywords(info):
    """從 yt-dlp info.json 找二次加工關鍵字。True 表示疑似 edit"""
    if not info:
        return False
    title = (info.get("title") or "").lower()
    desc = (info.get("description") or "").lower()[:500]
    tags = " ".join(str(t).lower() for t in (info.get("tags") or []))
    text = f" {title} {tags} {desc} "
    if not any(kw in text for kw in _FAN_EDIT_KEYWORDS):
        return False
    # 若 title 同時出現「安全」關鍵字，放行（避免誤判 fancam / practice / official MV）
    if any(kw in title for kw in _FAN_EDIT_SAFE_KEYWORDS):
        return False
    return True


def _clip_has_burned_subtitles(video_path, t_sec, clip_dur):
    """Clip 層級 burned-in 字幕/文字帶偵測：抽 4 幀，判定為字幕的條件（任一成立）：
      (A) 某帶狀區在所有幀都有高邊緣密度（字幕文字會變，但每幀都有大量文字筆劃）
      (B) 某帶狀區像素跨幀穩定（固定 logo / 浮水印）
    同時要求密度比中段高出不少，避免誤判整體高對比背景。"""
    try:
        import cv2 as _cv2
        import numpy as _np
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return False
        try:
            edges_list = []
            for r in (0.1, 0.35, 0.65, 0.9):
                cap.set(_cv2.CAP_PROP_POS_MSEC, (float(t_sec) + float(clip_dur) * r) * 1000.0)
                ok, frame = cap.read()
                if not ok or frame is None:
                    continue
                gray = _cv2.cvtColor(frame, _cv2.COLOR_BGR2GRAY)
                edges_list.append(_cv2.Canny(gray, 100, 200))
            if len(edges_list) < 3:
                return False
            h = edges_list[0].shape[0]
            # 底部字幕帶：下 28%（涵蓋多行字幕 + 雙語），頂部帶：上 15%
            top_slice = slice(0, int(h * 0.15))
            bot_slice = slice(int(h * 0.72), h)
            mid_slice = slice(int(h * 0.25), int(h * 0.70))

            def _stable_watermark(slc):
                # 像素穩定：固定 logo / 浮水印
                bands = [(e[slc] > 0).astype(_np.uint8) for e in edges_list]
                base = bands[0]
                if base.sum() < base.size * 0.02:
                    return False
                stable_px = base.copy()
                for b in bands[1:]:
                    stable_px = stable_px & b
                stable_ratio = stable_px.sum() / max(base.sum(), 1)
                stable_density = stable_px.sum() / base.size
                return stable_density > 0.02 and stable_ratio > 0.70

            # 底部字幕：要求每幀邊緣密度 ≥ 7% 且至少中段 2 倍
            #（dance 舞台鏡頭的觀眾頭部通常 3-5%，真正字幕 7-15%）
            bot_densities = [float((e[bot_slice] > 0).mean()) for e in edges_list]
            mid_densities = [float((e[mid_slice] > 0).mean()) for e in edges_list]
            bot_text = all(
                d >= 0.07 and d >= md * 2.0
                for d, md in zip(bot_densities, mid_densities)
            )
            if bot_text:
                return True
            # 其他位置只偵測固定 logo/watermark（穩定像素），不做單純密度判定
            if _stable_watermark(bot_slice) or _stable_watermark(top_slice):
                return True
            return False
        finally:
            cap.release()
    except Exception:
        return False


def _detect_split_screen(video_path):
    """取 8 幀偵測二次加工線索。

    單幀（符合任一視為 edit-frame）：
      - H-split: 上下半直方圖 L1 > 0.85 且 row-wise 亮度跳變 > 10
      - V-split: 左右半直方圖 L1 > 0.75 且 col-wise 亮度跳變 > 10
      - Edge-band text: 上 / 下帶狀 Canny 邊緣密度 / 中段 > 2.2

    跨幀（針對嵌入式剪輯署名 / 角落浮水印 / PIP 螢幕）：
      - Stable center text: 80% 以上的幀都同時是邊緣的 pixel，過水平 opening
        後若中央 50% × 50% 區密度 >= 1.0% → 真實 burned-in 文字
        （水平 opening 過濾掉縱向 blob 避免身形輪廓誤判）
      - Stable corner watermark: 左上/左下/右下角任一密度 >= 5%（右上排除
        因為是系統加的 anyujin tag）
    """
    try:
        import cv2 as _cv2
        import numpy as _np
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return False
        total = int(cap.get(_cv2.CAP_PROP_FRAME_COUNT) or 0)
        if total < 30:
            cap.release()
            return False
        hits = 0
        samples = 0
        kern = _np.ones(5) / 5.0
        edge_stack = []  # 用於跨幀穩定性檢查
        SH, SW = 240, 160
        kern33 = _cv2.getStructuringElement(_cv2.MORPH_RECT, (3, 3))
        for r in (0.10, 0.22, 0.34, 0.46, 0.58, 0.70, 0.82, 0.94):
            cap.set(_cv2.CAP_PROP_POS_FRAMES, int(total * r))
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            samples += 1
            gray = _cv2.cvtColor(frame, _cv2.COLOR_BGR2GRAY)
            h, w = gray.shape
            is_edit_frame = False

            # ① 水平拼接
            row_band = gray[int(h * 0.20):int(h * 0.80)]
            row_means = row_band.mean(axis=1)
            if len(row_means) >= 15:
                sm = _np.convolve(row_means, kern, mode="valid")
                h_jump = float(_np.abs(_np.diff(sm)).max())
                hu = _cv2.calcHist([gray[: h // 2]], [0], None, [32], [0, 256]).flatten()
                hl = _cv2.calcHist([gray[h // 2:]], [0], None, [32], [0, 256]).flatten()
                hu = hu / (hu.sum() + 1e-6)
                hl = hl / (hl.sum() + 1e-6)
                h_hist = float(_np.abs(hu - hl).sum())
                if h_hist > 0.85 and h_jump > 10:
                    is_edit_frame = True

            # ② 垂直拼接
            if not is_edit_frame:
                col_band = gray[:, int(w * 0.20):int(w * 0.80)]
                col_means = col_band.mean(axis=0)
                if len(col_means) >= 15:
                    sm = _np.convolve(col_means, kern, mode="valid")
                    v_jump = float(_np.abs(_np.diff(sm)).max())
                    hL = _cv2.calcHist([gray[:, : w // 2]], [0], None, [32], [0, 256]).flatten()
                    hR = _cv2.calcHist([gray[:, w // 2:]], [0], None, [32], [0, 256]).flatten()
                    hL = hL / (hL.sum() + 1e-6)
                    hR = hR / (hR.sum() + 1e-6)
                    v_hist = float(_np.abs(hL - hR).sum())
                    if v_hist > 0.75 and v_jump > 10:
                        is_edit_frame = True

            # ③ 邊緣帶狀文字 overlay —— 單幀判定已移除（粉絲直拍常有字幕/intro
            # title 會誤判）；僅保留 edges 供 ④ 跨幀穩定性檢查使用
            edges = _cv2.Canny(gray, 100, 200)

            # 收集 dilation 後邊緣圖（縮到 SW×SH 對齊），供跨幀穩定性檢查
            try:
                e_small = _cv2.resize(edges, (SW, SH), interpolation=_cv2.INTER_AREA)
                e_d = _cv2.dilate((e_small > 0).astype(_np.uint8), kern33)
                edge_stack.append(e_d)
            except Exception:
                pass

            if is_edit_frame:
                hits += 1
        cap.release()

        # ④ 跨幀穩定 burned-in 內容（剪輯署名 / hardcoded watermark / PIP）
        if samples >= 5 and len(edge_stack) >= 5:
            try:
                stk = _np.stack(edge_stack, axis=0).astype(_np.float32)
                hit_ratio = stk.mean(axis=0)
                stable = (hit_ratio >= 0.80).astype(_np.uint8)
                # 排除右上角系統 anyujin tag
                stable[:int(SH * 0.10), int(SW * 0.55):] = 0

                # 中央文字：水平 opening 把縱向身形 blob 砍掉，只留水平文字串
                h_kern = _cv2.getStructuringElement(_cv2.MORPH_RECT, (9, 1))
                text_only = _cv2.morphologyEx(stable, _cv2.MORPH_OPEN, h_kern)
                cy0, cy1 = int(SH * 0.25), int(SH * 0.75)
                cx0, cx1 = int(SW * 0.25), int(SW * 0.75)
                text_score = float(text_only[cy0:cy1, cx0:cx1].mean())

                # 角落浮水印（左上 / 左下 / 右下；右上已排除）
                cs = int(SH * 0.12)
                cw = int(SW * 0.30)
                c_LT = float(stable[:cs, :cw].mean())
                c_LB = float(stable[-cs:, :cw].mean())
                c_RB = float(stable[-cs:, -cw:].mean())
                corner_score = max(c_LT, c_LB, c_RB)

                if text_score >= 0.010:
                    hits += 2  # 強訊號：中央有持續水平文字
                if corner_score >= 0.10:
                    hits += 2  # 強訊號：角落有 burned-in 浮水印（門檻提高避免頻道 logo 誤判）
            except Exception:
                pass

        return samples >= 3 and hits >= 2
    except Exception:
        return False


_FAN_EDIT_CACHE = {}


def _is_fan_edit(video_path):
    """綜合判定 yt-dlp metadata + 畫面 split-screen，結果會快取。"""
    if video_path in _FAN_EDIT_CACHE:
        return _FAN_EDIT_CACHE[video_path]
    verdict = False
    try:
        info = _load_video_info_json(video_path)
        if _check_fan_edit_keywords(info):
            verdict = True
        elif _detect_split_screen(video_path):
            verdict = True
    except Exception:
        pass
    _FAN_EDIT_CACHE[video_path] = verdict
    if verdict:
        logging.info(f"fan-edit detected → down-rank: {os.path.basename(video_path)}")
    return verdict


def _select_highlight_clips(all_scores, clip_duration, total_duration, max_per_video,
                            person=None, prefer_vertical=False):
    """從所有影片中挑選最佳片段（優先選未使用過的，排除黑名單）"""
    # 載入歷史記錄和黑名單
    used_set = _load_clip_history(person) if person else set()
    blacklist = _load_clip_blacklist(person) if person else set()

    # 先掃描所有來源影片，標記 fan-edit（快取避免重算）
    fan_edit_videos = set()
    for video_path, _ in all_scores:
        if _is_fan_edit(video_path):
            fan_edit_videos.add(video_path)
    total_videos = len(all_scores)
    edit_count = len(fan_edit_videos)
    clean_count = total_videos - edit_count
    if fan_edit_videos:
        logging.info(f"fan-edit pool: {edit_count} / {total_videos} videos flagged as edits")
    # HARD reject：clean sources 夠覆蓋時一律拒絕 fan-edit（多半是多成員混剪），
    # 寧可輸出短片也不要把其他團員的畫面放進成片
    import math
    min_clips_needed = max(2, math.ceil(total_duration / max(clip_duration, 1.0)))
    _MIN_CLEAN_SOURCES = max(2, math.ceil(min_clips_needed / 2))
    hard_reject_edits = clean_count >= _MIN_CLEAN_SOURCES
    if hard_reject_edits and edit_count:
        logging.info(f"hard-rejecting {edit_count} fan-edit video(s); using {clean_count} clean source(s)")
    elif edit_count and clean_count < _MIN_CLEAN_SOURCES:
        logging.warning(f"only {clean_count} clean source(s), need ≥{_MIN_CLEAN_SOURCES} to cover "
                        f"{min_clips_needed} clip(s) — soft deprioritize (edits still usable)")
    elif edit_count == total_videos and total_videos:
        logging.warning("ALL sources look like fan edits — falling back with soft deprioritization")

    candidates = []
    for video_path, scores in all_scores:
        vbase = os.path.basename(video_path)
        is_edit = video_path in fan_edit_videos
        # 硬過濾：乾淨池不為空時直接跳過 edit 影片
        if hard_reject_edits and is_edit:
            continue
        # 建 time→score 索引，用來算 clip window 平均分
        time_score_map = {round(s["time"], 1): s["score"] for s in scores}
        for s in scores:
            vid_dur = s.get("duration", 9999)
            # 確保片段起點 + clip_duration 不會超過影片長度
            # 對短片段（< 30秒）用較小的尾部緩衝，避免過度過濾
            tail_buffer = min(5.0, vid_dur * 0.15) if vid_dur < 30 else 5.0
            usable_end = max(clip_duration, vid_dur - tail_buffer)
            if s["time"] + clip_duration > usable_end:
                continue
            # 跳過黑名單中的片段（使用者曾按 ✕ 拒絕的）
            if (vbase, round(s["time"])) in blacklist:
                continue
            # E: clip window 品質檢查 — clip 內所有秒的平均分 >= 下限
            # 避免「只有 1 秒有臉、其他 7 秒是廣角/別人」的低品質 clip
            clip_scores = []
            for dt in range(int(clip_duration)):
                tt = round(s["time"] + dt, 1)
                clip_scores.append(time_score_map.get(tt, 0.0))
            clip_avg = sum(clip_scores) / max(len(clip_scores), 1)
            # 人物需在 clip 內穩定出現：至少 1/3 的秒數偵測到目標人物（不是同團他人）。
            # 硬過濾 + 寧可片段短也不要填入別人的 clip
            _CLIP_AVG_FLOOR = 0.10
            _face_sec_count = sum(1 for sc in clip_scores if sc > 0)
            _face_presence_ratio = _face_sec_count / max(len(clip_scores), 1)
            if clip_avg < _CLIP_AVG_FLOOR and s["score"] > 0:
                continue
            if _face_presence_ratio < 0.34:
                continue  # 目標人物在 clip 內不到 1/3 秒數，剔除
            _weighted_score = clip_avg * (0.5 + 0.5 * _face_presence_ratio)
            # 檢查是否曾使用過
            is_used = (vbase, round(s["time"])) in used_set
            is_vertical = s.get("is_vertical", False)
            candidates.append({
                "video": video_path,
                "time": s["time"],
                "score": _weighted_score,  # 面部出現比例加權後的分數
                "src_duration": vid_dur,
                "is_vertical": is_vertical,
                "_used": is_used,
                "_fan_edit": is_edit,
            })

    if not candidates:
        return []

    # 排序：未使用 → 非二次加工 → 方向偏好 → 分數
    # _fan_edit 是次於 _used 的次序鍵：先用乾淨原片，實在沒有才退回 edit
    if prefer_vertical:
        candidates.sort(key=lambda x: (x["_used"], x["_fan_edit"],
                                       not x.get("is_vertical", False), -x["score"]))
    else:
        candidates.sort(key=lambda x: (x["_used"], x["_fan_edit"], -x["score"]))

    # ── Per-clip 多人偵測索引：video → {second: score_dict} ──
    # 用來判斷一個 clip 內有多少秒同時滿足 n_faces≥2 且其他人臉 ≥ target 一半
    _score_index = {}
    for video_path, scores in all_scores:
        _score_index[video_path] = {int(s["time"]): s for s in scores if isinstance(s, dict)}

    def _clip_is_multi_member(video_path, t_start, dur):
        """若 clip 內 ≥50% 秒數同時滿足：n_faces≥2 且 other_max_area ≥ target_area×0.5
        則視為多人片段（如 fan-edit 中的合舞、split-screen）。"""
        idx = _score_index.get(video_path)
        if not idx:
            return False
        multi = 0
        total_sec = 0
        for sec in range(int(t_start), int(t_start + dur) + 1):
            s = idx.get(sec)
            if not s:
                continue
            total_sec += 1
            n = s.get("n_faces", 0)
            ta = s.get("target_area", 0.0)
            oa = s.get("other_max_area", 0.0)
            if n >= 2 and oa >= max(ta * 0.5, 0.005):
                multi += 1
        if total_sec == 0:
            return False
        return multi / total_sec >= 0.5

    selected = []
    video_counts = {}
    total = 0.0
    PHASH_THRESHOLD = 10  # Hamming distance: <10 視為視覺過於相似
    selected_phashes = []  # list of int
    _rej = {"cap": 0, "overlap": 0, "phash": 0, "subtitle": 0, "multi": 0}

    for c in candidates:
        if total >= total_duration:
            break

        v = c["video"]
        if max_per_video > 0 and video_counts.get(v, 0) >= max_per_video:
            _rej["cap"] += 1
            continue

        # 避免同一部影片的片段重疊
        overlap = False
        for s in selected:
            if s["video"] == v and abs(s["time"] - c["time"]) < clip_duration + 1:
                overlap = True
                break
        if overlap:
            _rej["overlap"] += 1
            continue

        # pHash 跨片段去重：抓片段中間一幀 vs 已選片段
        mid_t = c["time"] + clip_duration / 2
        ph = _compute_frame_phash(v, mid_t)
        if ph is not None and selected_phashes:
            too_similar = any(_phash_hamming(ph, ep) < PHASH_THRESHOLD for ep in selected_phashes)
            if too_similar:
                _rej["phash"] += 1
                continue
        c["_phash"] = ph

        # Burned-in 字幕偵測：在這 6 秒內取 3 幀，檢查上/下帶狀邊緣密度是否遠高於中段
        if _clip_has_burned_subtitles(v, c["time"], clip_duration):
            _rej["subtitle"] += 1
            logging.info(f"subtitle rejected: {os.path.basename(v)} @{c['time']:.1f}s")
            continue

        # Per-clip 多人偵測：若整個 clip 都是多人合舞/split-screen，丟棄
        if _clip_is_multi_member(v, c["time"], clip_duration):
            _rej["multi"] += 1
            logging.info(f"multi-member rejected: {os.path.basename(v)} @{c['time']:.1f}s")
            continue

        selected.append(c)
        if ph is not None:
            selected_phashes.append(ph)
        video_counts[v] = video_counts.get(v, 0) + 1
        total += clip_duration
    logging.info(f"selection stats: {len(candidates)} candidates, {len(selected)} selected; "
                 f"rejected by cap={_rej['cap']}, overlap={_rej['overlap']}, "
                 f"phash={_rej['phash']}, subtitle={_rej['subtitle']}, multi={_rej['multi']}")

    # 如果未使用的片段不夠，允許重複使用（上面排序已處理）
    unused_count = sum(1 for s in selected if not s.get("_used"))
    used_count = sum(1 for s in selected if s.get("_used"))
    if used_count > 0:
        logging.info(f"clip selection: {unused_count} new + {used_count} reused clips")

    # ── 切點對齊到安靜處（beat-aware cutting）──
    # 為每個選中片段把起點對齊到 ±1.5 秒內音訊能量最低的位置
    energy_by_video = {}
    for video_path, scores in all_scores:
        if scores and isinstance(scores[0], dict) and "audio_energy" in scores[0]:
            energy_by_video[video_path] = np.array(
                [s.get("audio_energy", 0.0) for s in scores], dtype=np.float32)
    for c in selected:
        energy = energy_by_video.get(c["video"])
        if energy is None or len(energy) == 0:
            continue
        original_t = c["time"]
        new_t = _snap_cut_to_quiet(energy, original_t, window_sec=1.5)
        vid_dur = c.get("src_duration", 9999)
        tail_buffer_snap = min(5.0, vid_dur * 0.15) if vid_dur < 30 else 5.0
        usable_end_snap = max(clip_duration, vid_dur - tail_buffer_snap)
        if 0 <= new_t and new_t + clip_duration <= usable_end_snap:
            c["time"] = new_t

    # 對齊後可能造成同一部影片的片段位置接近，重新檢查重疊並剔除
    selected.sort(key=lambda x: (x["video"], x["time"]))
    deduped = []
    last_by_video = {}
    for c in selected:
        v = c["video"]
        prev_t = last_by_video.get(v)
        if prev_t is not None and abs(c["time"] - prev_t) < clip_duration + 0.5:
            continue  # 與前一個重疊，捨棄
        deduped.append(c)
        last_by_video[v] = c["time"]
    selected = deduped

    # ── Best-clip-first ordering ──
    # TikTok 71% 用戶在 3 秒決定要不要留下；把分數最高的片段放 t=0
    # 保留分數排序後前 N 為主選，其餘為替補，呼叫端會切片
    selected.sort(key=lambda x: -x.get("score", 0))

    # ── Loop closure ──
    # 讓結尾最後一個片段的中間幀，視覺上接近首片段，形成 seamless loop
    # 做法：計算首片段 mid frame pHash，再從主選剩下的片段裡
    # 挑 Hamming distance 最小的放到最後一個位置
    try:
        # 先估主選數量：上限是 total_duration/clip_duration，這裡取 selected 的 80% 作為「主選區」
        # 呼叫端會自己切 selected[:needed]，我們不知道 needed 但可用 _extra_factor 反推
        # 簡化：只要 selected 長度 > 2，就在前 N 裡找最像首片段的移到末尾
        n_main = max(2, len(selected) // 3 if len(selected) >= 6 else len(selected))
        if n_main >= 3:
            first_c = selected[0]
            first_ph = _compute_frame_phash(first_c["video"], first_c["time"] + clip_duration / 2)
            if first_ph is not None:
                best_idx = None
                best_dist = 65
                # 在第 1 位到 n_main-1 位裡找（排除首片段）
                for i in range(1, n_main):
                    c = selected[i]
                    ph = _compute_frame_phash(c["video"], c["time"] + clip_duration / 2)
                    if ph is None:
                        continue
                    d = _phash_hamming(first_ph, ph)
                    # 要夠像（< 18）但不要太像（> 6，避免跟首片段幾乎重複）
                    if 6 < d < 18 and d < best_dist:
                        best_dist = d
                        best_idx = i
                if best_idx is not None and best_idx != n_main - 1:
                    # 把 best 移到主選區的最後一個位置
                    loop_clip = selected.pop(best_idx)
                    selected.insert(n_main - 1, loop_clip)
                    logging.info(f"loop closure: moved clip idx {best_idx} to position "
                                 f"{n_main-1}, Hamming={best_dist}")
    except Exception as e:
        logging.debug(f"loop closure skipped: {e}")

    # 清理內部欄位
    for s in selected:
        s.pop("_used", None)
        s.pop("_phash", None)
        s.pop("_fan_edit", None)

    return selected


def _compile_highlight(clips, clip_duration, output_path, transition,
                       transition_dur, resolution, progress_cb=None,
                       audio_mode="original", loudnorm_params=None,
                       name_tag=None):
    """用 FFmpeg 把片段合成精華影片"""
    ffmpeg = _get_ffmpeg()
    import tempfile, subprocess

    logging.info(f"compile_highlight: {len(clips)} clips, clip_dur={clip_duration}, "
                 f"transition={transition}, resolution={resolution}, audio={audio_mode}")

    res_map = {"720p": (1280, 720), "1080p": (1920, 1080),
                "720p_v": (720, 1280), "1080p_v": (1080, 1920)}
    tw, th = res_map.get(resolution, (1280, 720))

    temp_dir = tempfile.mkdtemp()
    try:
        # Phase 1: 擷取每個片段並統一解析度（精確裁切到 clip_duration 秒）
        # 快取各來源影片的實際時長，避免重複探測
        _src_dur_cache = {}
        seg_files = []
        # ── Clip duration variation ──
        # 節奏變化：短-長交替，讓剪接有呼吸感
        # 第一個片段永遠是「標準長度」確保強 hook；後面套用節奏 pattern
        _rhythm_pattern = [1.0, 0.8, 1.15, 0.75, 1.2, 0.85, 1.1, 0.9]
        for i, clip in enumerate(clips):
            seg_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            start = max(0, clip["time"] - 0.3)

            # 探測來源影片的實際時長，確保不會超出範圍（防止凍結畫面）
            src_video = clip["video"]
            if src_video not in _src_dur_cache:
                _src_dur_cache[src_video] = _probe_duration(ffmpeg, src_video)
            src_dur = _src_dur_cache[src_video]

            # 節奏變化：第 0 個用標準長度（hook），其餘套 pattern
            if i == 0:
                target_dur = clip_duration
            else:
                target_dur = clip_duration * _rhythm_pattern[(i - 1) % len(_rhythm_pattern)]
            target_dur = max(1.5, target_dur)  # 下限 1.5 秒

            actual_clip_dur = target_dur
            if src_dur:
                available = src_dur - start - 0.5  # 預留 0.5 秒安全邊距
                if available < 1.0:
                    logging.warning(f"  clip {i}: skip, not enough content "
                                    f"(start={start:.1f}, src_dur={src_dur:.1f})")
                    if progress_cb:
                        progress_cb(i / len(clips))
                    continue
                actual_clip_dur = min(target_dur, available)

            # 統一畫布合成
            # 若來源是橫式 (W > H) 且目標是垂直 (tw < th)，用 face-centered crop
            # 填滿畫面並把臉放在上三分點（垂直視覺層級法則）；肩膀以上保留
            # 否則用原本的 blurred background + fit-within 方案
            _face_crop_chain = None
            if tw < th:  # 目標是直式
                _fc = _detect_face_center(src_video, clip["time"] + actual_clip_dur / 2,
                                          clip_dur=actual_clip_dur)
                if _fc is not None:
                    _cx, _cy, _sw, _sh = _fc
                    if _sw > _sh:  # 來源是橫式才做 face crop
                        # 等比縮放到「剛好覆蓋」目標畫布
                        _scale = max(tw / _sw, th / _sh)
                        _nw = int(_sw * _scale)
                        _nh = int(_sh * _scale)
                        _cx_s = _cx * _scale
                        _cy_s = _cy * _scale
                        # 水平：臉置中；垂直：臉置於上三分點
                        _cx_crop = max(0, min(_nw - tw, int(_cx_s - tw / 2)))
                        _cy_crop = max(0, min(_nh - th, int(_cy_s - th / 3)))
                        _face_crop_chain = (
                            f"[0:v]scale={_nw}:{_nh},"
                            f"crop={tw}:{th}:{_cx_crop}:{_cy_crop},"
                            f"setpts=PTS-STARTPTS"
                        )

            if _face_crop_chain is not None:
                _base_chain = _face_crop_chain
            else:
                _base_chain = (
                    f"[0:v]split=2[main][bg];"
                    f"[bg]scale={tw}:{th}:force_original_aspect_ratio=increase,"
                    f"crop={tw}:{th},boxblur=20:5[blur];"
                    f"[main]scale={tw}:{th}:force_original_aspect_ratio=decrease[fg];"
                    f"[blur][fg]overlay=(W-w)/2:(H-h)/2,setpts=PTS-STARTPTS"
                )
            if name_tag:
                # 用 drawtext 疊字：右上角，半透明黑底，白字
                # 過濾掉 drawtext 不喜歡的字元
                _tag = str(name_tag).replace(":", "").replace("'", "").replace("\\", "")
                _font_sz = int(max(24, min(tw, th) * 0.038))
                _pad = int(_font_sz * 0.5)
                _x = f"w-text_w-{_pad*2}-10"
                _y = "10+text_h*0.15"
                drawtext = (
                    f",drawtext=text='{_tag}':"
                    f"fontcolor=white:fontsize={_font_sz}:"
                    f"box=1:boxcolor=black@0.5:boxborderw={_pad}:"
                    f"x={_x}:y={_y}"
                )
                vf_complex = _base_chain + drawtext + "[outv]"
            else:
                vf_complex = _base_chain + "[outv]"
            cmd = [
                ffmpeg, "-y",
                "-ss", f"{start:.2f}",
                "-i", src_video,
                "-t", f"{actual_clip_dur:.2f}",
                "-filter_complex", vf_complex,
                "-map", "[outv]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            ]
            if audio_mode == "mute":
                cmd += ["-an"]
            else:
                # loudnorm: 每段正規化到 -14 LUFS（TikTok/YouTube 主流標準）
                # asetpts 修正時間戳，aresample 處理可能的取樣率不一致
                _lI = (loudnorm_params or {}).get("I", -14)
                _lTP = (loudnorm_params or {}).get("TP", -1.5)
                _lLRA = (loudnorm_params or {}).get("LRA", 11)
                cmd += [
                    "-map", "0:a?",
                    "-af",
                    f"aresample=async=1:first_pts=0,"
                    f"asetpts=PTS-STARTPTS,"
                    f"loudnorm=I={_lI}:TP={_lTP}:LRA={_lLRA}",
                    "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
                ]
            cmd += [
                "-r", "30",
                "-shortest",
                "-fflags", "+genpts",
                "-pix_fmt", "yuv420p",
                seg_path,
            ]
            r = subprocess.run(cmd, capture_output=True, timeout=120)
            if os.path.isfile(seg_path) and os.path.getsize(seg_path) > 0:
                # 驗證擷取出的片段沒有凍結（檢查實際長度）
                seg_dur = _probe_duration(ffmpeg, seg_path)
                if seg_dur and seg_dur < 0.5:
                    logging.warning(f"  clip {i}: too short ({seg_dur:.1f}s), skip")
                    os.remove(seg_path)
                else:
                    seg_files.append(seg_path)
                    logging.info(f"  clip {i}: {os.path.getsize(seg_path)/1024:.0f}KB "
                                 f"({seg_dur:.1f}s) from {os.path.basename(src_video)} "
                                 f"@{clip['time']:.1f}s")
            else:
                stderr_tail = r.stderr[-300:] if r.stderr else b''
                logging.warning(f"  clip {i} extraction failed: rc={r.returncode}, "
                                f"stderr={stderr_tail}")
            if progress_cb:
                progress_cb(i / len(clips))

        logging.info(f"compile_highlight: extracted {len(seg_files)}/{len(clips)} clips OK")
        if not seg_files:
            logging.warning("compile_highlight: no valid clips extracted")
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
            with open(list_path, "w", encoding="utf-8") as f:
                for sp in seg_files:
                    safe_path = sp.replace("\\", "/").replace("'", "'\\''")
                    f.write(f"file '{safe_path}'\n")
            cmd = [
                ffmpeg, "-y",
                "-f", "concat", "-safe", "0",
                "-i", list_path,
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-r", "30", "-pix_fmt", "yuv420p",
            ]
            if has_audio:
                cmd += ["-c:a", "aac", "-b:a", "128k"]
            else:
                cmd += ["-an"]
            cmd.append(output_path)
            r = subprocess.run(cmd, capture_output=True, timeout=300)
            logging.info(f"concat rc={r.returncode}")
            result = output_path

        if os.path.isfile(output_path):
            final_dur = _probe_duration(ffmpeg, output_path)
            logging.info(f"compile_highlight: output {os.path.getsize(output_path)/1024/1024:.1f}MB, "
                         f"duration={final_dur}s")
            return result
        else:
            logging.warning("compile_highlight: output file not created")
            return None
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _probe_duration(ffmpeg, filepath):
    """取得影片實際長度（秒）- 用 ffmpeg -i 解析（不需要 ffprobe）"""
    import subprocess
    # Method 1: ffmpeg -i → parse "Duration: HH:MM:SS.xx" from stderr
    try:
        r = subprocess.run(
            [ffmpeg, "-i", filepath],
            capture_output=True, text=True, timeout=10)
        m = re.search(r'Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)', r.stderr)
        if m:
            h, mi, s = float(m.group(1)), float(m.group(2)), float(m.group(3))
            return h * 3600 + mi * 60 + s
    except Exception:
        pass
    # Method 2: OpenCV fallback
    if HAS_CV2:
        try:
            cap = cv2.VideoCapture(filepath)
            if cap.isOpened():
                fps = cap.get(cv2.CAP_PROP_FPS) or 30
                total = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                cap.release()
                if fps > 0 and total > 0:
                    return total / fps
        except Exception:
            pass
    return None


def _xfade_concat(ffmpeg, seg_files, clip_dur, xfade_dur, output_path, temp_dir,
                   has_audio=True):
    """用 xfade filter 鏈式合成片段（使用實際長度計算 offset）"""
    import subprocess
    n = len(seg_files)
    if n == 1:
        shutil.copy2(seg_files[0], output_path)
        return output_path

    # 量測每個片段的實際長度
    durations = []
    valid_files = []
    for f in seg_files:
        d = _probe_duration(ffmpeg, f)
        if d is None:
            d = clip_dur  # 無法量測時用預設長度
        if d > 0.5:
            durations.append(d)
            valid_files.append(f)
        else:
            logging.warning(f"xfade: skip bad clip {os.path.basename(f)} (duration={d})")

    logging.info(f"xfade_concat: {len(valid_files)} valid clips, "
                 f"durations={[f'{d:.2f}' for d in durations]}, "
                 f"xfade_dur={xfade_dur}, has_audio={has_audio}")

    if len(valid_files) < 2:
        if valid_files:
            shutil.copy2(valid_files[0], output_path)
        return output_path

    seg_files = valid_files
    n = len(seg_files)

    # ── 嘗試 xfade ──
    xfade_ok = False
    try:
        inputs = []
        for f in seg_files:
            inputs.extend(["-i", f])

        # 用實際長度計算 xfade offset
        filters = []
        accumulated = durations[0]
        prev = "[0]"
        for i in range(1, n):
            curr = f"[{i}]"
            out = f"[v{i}]" if i < n - 1 else "[vout]"
            # offset 必須 > 0 且 < accumulated
            safe_xfade = min(xfade_dur, accumulated * 0.8, durations[i] * 0.8)
            offset = max(0.1, accumulated - safe_xfade)
            filters.append(
                f"{prev}{curr}xfade=transition=fade:duration={safe_xfade:.2f}"
                f":offset={offset:.2f}{out}"
            )
            accumulated = offset + durations[i]
            prev = out

        filter_complex = ";".join(filters)

        if has_audio:
            audio_inputs = "".join(f"[{i}:a]" for i in range(n))
            audio_filter = f"{audio_inputs}concat=n={n}:v=0:a=1[aout]"
            filter_complex += ";" + audio_filter

        cmd = [ffmpeg, "-y"] + inputs + [
            "-filter_complex", filter_complex,
            "-map", "[vout]",
        ]
        if has_audio:
            cmd += ["-map", "[aout]", "-c:a", "aac", "-b:a", "128k"]
        cmd += ["-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-shortest", output_path]

        result = subprocess.run(cmd, capture_output=True, timeout=300)
        stderr_tail = result.stderr[-800:] if result.stderr else b''
        logging.info(f"xfade rc={result.returncode}, stderr_tail={stderr_tail}")

        if result.returncode == 0 and os.path.isfile(output_path) and os.path.getsize(output_path) > 1024:
            xfade_ok = True
    except Exception as e:
        logging.warning(f"xfade exception: {e}")

    # ── xfade 失敗時 fallback 到 concat（重新編碼以確保相容）──
    if not xfade_ok:
        logging.warning("xfade failed, fallback to concat demuxer with re-encode")
        if os.path.isfile(output_path):
            os.remove(output_path)
        list_path = os.path.join(temp_dir, "list.txt")
        with open(list_path, "w", encoding="utf-8") as f:
            for sp in seg_files:
                # 使用 forward slash 且轉義單引號
                safe_path = sp.replace("\\", "/").replace("'", "'\\''")
                f.write(f"file '{safe_path}'\n")
        # 用 re-encode 而不是 -c copy，更可靠
        cmd2 = [ffmpeg, "-y", "-f", "concat", "-safe", "0", "-i", list_path,
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-r", "30", "-pix_fmt", "yuv420p"]
        if has_audio:
            cmd2 += ["-c:a", "aac", "-b:a", "128k", "-ar", "44100"]
        else:
            cmd2 += ["-an"]
        cmd2.append(output_path)
        r2 = subprocess.run(cmd2, capture_output=True, timeout=300)
        stderr2 = r2.stderr[-500:] if r2.stderr else b''
        logging.info(f"concat fallback rc={r2.returncode}, stderr_tail={stderr2}")

        # 最後手段：逐個合併
        if r2.returncode != 0 or not os.path.isfile(output_path) or os.path.getsize(output_path) < 1024:
            logging.warning("concat demuxer also failed, try sequential merge")
            if os.path.isfile(output_path):
                os.remove(output_path)
            # 先把前兩個合併，再逐一追加
            merged = seg_files[0]
            for i in range(1, len(seg_files)):
                tmp_out = os.path.join(temp_dir, f"merged_{i}.mp4")
                tmp_list = os.path.join(temp_dir, f"pair_{i}.txt")
                with open(tmp_list, "w", encoding="utf-8") as f:
                    f.write(f"file '{merged.replace(chr(92), '/').replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'\n")
                    f.write(f"file '{seg_files[i].replace(chr(92), '/').replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'\n")
                cmd3 = [ffmpeg, "-y", "-f", "concat", "-safe", "0",
                        "-i", tmp_list, "-c", "copy", tmp_out]
                r3 = subprocess.run(cmd3, capture_output=True, timeout=120)
                if r3.returncode == 0 and os.path.isfile(tmp_out) and os.path.getsize(tmp_out) > 1024:
                    merged = tmp_out
                else:
                    logging.warning(f"sequential merge step {i} failed")
            if merged != seg_files[0]:
                shutil.copy2(merged, output_path)

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
            # 補償 xfade 轉場重疊損失的時間
            import math
            if transition in ("crossfade", "fade") and transition_dur > 0 and clip_duration > transition_dur:
                needed_clips = math.ceil((total_duration - transition_dur) / (clip_duration - transition_dur))
                adjusted_total = needed_clips * clip_duration
            else:
                adjusted_total = total_duration
            clips = _select_highlight_clips(all_scores, clip_duration,
                                            adjusted_total, max_per_video,
                                            person=person,
                                            prefer_vertical=resolution.endswith("_v"))
            if not clips:
                q.put({"type": "error", "message": "找不到符合條件的片段"})
                return

            if transition in ("crossfade", "fade") and transition_dur > 0 and len(clips) > 1:
                actual_dur = len(clips) * clip_duration - (len(clips) - 1) * transition_dur
            else:
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
                _save_clip_history(person, clips)
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

# 任務歷史（保留最近完成/失敗的任務結果，供重連用）
auto_video_history = {}  # task_id -> {status, label, result_data, ts}
_AV_HISTORY_MAX = 50


def _av_save_history(task_id, status, label, data=None):
    """儲存任務最終狀態到歷史"""
    auto_video_history[task_id] = {
        "status": status,  # "running" | "preview" | "done" | "error"
        "label": label,
        "data": data or {},
        "ts": time.time(),
    }
    # 清理太舊的（超過 1 小時）
    cutoff = time.time() - 3600
    old = [k for k, v in auto_video_history.items() if v["ts"] < cutoff]
    for k in old:
        auto_video_history.pop(k, None)


@app.route("/api/auto-video/tasks")
def api_auto_video_tasks():
    """列出所有進行中和最近完成的任務"""
    result = []
    # 進行中的
    with auto_video_tasks_lock:
        for tid, task in auto_video_tasks.items():
            h = auto_video_history.get(tid, {})
            result.append({
                "task_id": tid,
                "status": h.get("status", "running"),
                "label": h.get("label", ""),
                "data": h.get("data", {}),
            })
    # 已完成但不在 active 中的
    with auto_video_tasks_lock:
        active_ids = set(auto_video_tasks.keys())
    for tid, h in auto_video_history.items():
        if tid not in active_ids and h["status"] in ("done", "error"):
            result.append({
                "task_id": tid,
                "status": h["status"],
                "label": h.get("label", ""),
                "data": h.get("data", {}),
            })
    return jsonify(result)


def _compute_smart_defaults(person):
    """根據人物已下載的資料推論最佳預設值。

    判斷邏輯：
    - 有 .negatives/ → 是團體成員 → fancam + closeup 策略
    - 照片 >= 30 → 資料充足 → balanced 策略
    - 照片 < 30 → 資料稀少 → closeup 策略（face_weight 高，精準）
    - 預設輸出 TikTok preset（垂直 1080p，30秒）
    """
    defaults = {
        "strategy": "balanced",
        "video_type": "fancam",
        "output_preset": "tiktok",
        "resolution": "1080p_v",
        "total_duration": 30,
        "clip_duration": 3,
        "max_videos": 5,
        "transition": "crossfade",
        "transition_dur": 0.5,
        "audio_mode": "original",
    }
    try:
        photo_dir = os.path.join(DOWNLOAD_ROOT, sanitize_name(person))
        if not os.path.isdir(photo_dir):
            return defaults
        import glob as _glob
        files = []
        for ext in ("*.jpg", "*.jpeg", "*.png", "*.webp"):
            files.extend(_glob.glob(os.path.join(photo_dir, ext)))
        n_photos = len(files)

        neg_dir = os.path.join(photo_dir, ".negatives")
        has_negatives = os.path.isdir(neg_dir) and any(
            _glob.glob(os.path.join(neg_dir, p)) for p in ("*.jpg", "*.jpeg", "*.png", "*.webp"))

        if has_negatives:
            # 團體成員：用 fancam + closeup（強調臉）
            defaults["strategy"] = "closeup"
            defaults["video_type"] = "fancam"
        elif n_photos >= 30:
            defaults["strategy"] = "balanced"
        else:
            defaults["strategy"] = "closeup"

        logging.info(f"smart_defaults({person}): photos={n_photos}, negatives={has_negatives} → {defaults['strategy']}/{defaults['video_type']}")
    except Exception as e:
        logging.warning(f"smart_defaults failed: {e}")
    return defaults


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

    # ── Smart Defaults：auto_mode=True 時，根據人物類型自動推薦參數 ──
    auto_mode = bool(data.get("auto_mode", False))
    smart_defaults = {}
    if auto_mode:
        smart_defaults = _compute_smart_defaults(person)
        # 使用者未明確指定的欄位才套用 smart default
        for k, v in smart_defaults.items():
            if k not in data or data.get(k) in (None, "", "default"):
                data[k] = v
        logging.info(f"auto_mode ON for {person}: {smart_defaults}")

    extra_keyword = (data.get("search_keyword") or "").strip()
    search_keyword = f"{person} {extra_keyword}".strip() if extra_keyword else person
    max_videos = int(data.get("max_videos", 5))
    clip_duration = float(data.get("clip_duration", 3))
    total_duration = float(data.get("total_duration", 30))
    strategy = data.get("strategy", "balanced")
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    resolution = data.get("resolution", "720p_v")  # 預設直式
    audio_mode = data.get("audio_mode", "original")
    platform = data.get("platform", "tiktok")
    video_type = data.get("video_type", "all")
    # 舞台 / 直拍類型：硬否決會狠狠砍幀，需要更多素材池才湊得滿目標長度
    if video_type in ("stage", "fancam"):
        _orig_mv = max_videos
        max_videos = int(round(max_videos * 1.5))
        logging.info(f"video_type={video_type} → max_videos {_orig_mv} → {max_videos} (×1.5)")
    # 根據目標長度動態擴大素材池：一部下載影片要過三道關
    #   抽臉有臉 (~70%) × 非 fan-edit (~50%) × 內部多段 (max 2)
    # 粗估每部下載僅能貢獻約 0.5-0.7 clip，保險起見拉到 needed × 3。
    try:
        import math as _math
        _needed = max(1, _math.ceil(float(total_duration) / max(float(data.get("clip_duration", 3)), 1.0)))
        _target_pool = _needed * 3
        if max_videos < _target_pool:
            _orig_mv2 = max_videos
            max_videos = _target_pool
            logging.info(f"target_duration={total_duration}s needs ~{_needed} clips → "
                         f"max_videos {_orig_mv2} → {max_videos} (≥ needed×3)")
    except Exception as _e:
        logging.warning(f"target-based max_videos bump skipped: {_e}")
    source = data.get("source", "online")  # online / local
    preview = data.get("preview", False)
    output_preset = (data.get("output_preset") or "").strip().lower()

    # 套用平台預設：覆寫 resolution 與 total_duration，loudnorm 由 preset 決定
    loudnorm_params = None
    if output_preset and output_preset in OUTPUT_PRESETS:
        _p = OUTPUT_PRESETS[output_preset]
        resolution = _p["resolution"]
        # 只有使用者沒改過 total_duration 才套用（30 是預設值）
        if float(data.get("total_duration", 30)) == 30:
            total_duration = float(_p["total_duration"])
        loudnorm_params = {
            "I": _p["loudnorm_I"],
            "TP": _p["loudnorm_TP"],
            "LRA": _p["loudnorm_LRA"],
        }
        logging.info(f"output_preset={output_preset} → res={resolution}, "
                     f"total={total_duration}s, loudnorm={loudnorm_params}")

    resolved_person = resolve_celebrity(person)

    task_id = str(uuid.uuid4())[:8]
    q = Queue()
    confirm_event = threading.Event() if preview else None
    confirm_data = [None]  # mutable container for thread-safe passing

    task_label = f"{person}" + (f" ({extra_keyword})" if extra_keyword else "")
    if preview:
        task_label += " [挑選模式]"

    with auto_video_tasks_lock:
        auto_video_tasks[task_id] = {
            "queue": q, "status": "running",
            "confirm_event": confirm_event,
            "confirm_data": confirm_data,
            "label": task_label,
        }
    _av_save_history(task_id, "running", task_label)

    def worker():
        try:
            _auto_video_pipeline(
                q, resolved_person, search_keyword, max_videos,
                clip_duration, total_duration, strategy, transition,
                transition_dur, resolution, audio_mode, platform,
                video_type=video_type, source=source,
                preview=preview, confirm_event=confirm_event,
                confirm_data=confirm_data,
                task_id=task_id, task_label=task_label,
                loudnorm_params=loudnorm_params,
            )
        except Exception as e:
            import traceback
            logging.error(f"auto-video error: {traceback.format_exc()}")
            q.put({"type": "error", "message": str(e)})
            _av_save_history(task_id, "error", task_label, {"message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id, "label": task_label})


def _auto_video_pipeline(q, person, search_keyword, max_videos,
                          clip_duration, total_duration, strategy,
                          transition, transition_dur, resolution,
                          audio_mode, platform, video_type="all",
                          source="online", preview=False,
                          confirm_event=None, confirm_data=None,
                          task_id=None, task_label=None,
                          loudnorm_params=None):
    """完整 pipeline：搜尋 → 下載 → 擷取 → 精華剪輯（或本地影片 → 擷取 → 精華剪輯）"""
    import subprocess
    import random as _random

    VIDEO_TYPE_KW = {
        "dance": "dance 舞蹈",
        "fancam": "fancam 直拍",
        "stage": "stage performance 舞台",
        "cute": "cute moments 可愛",
        "vlog": "vlog 日常",
        "mv": "MV official",
        "challenge": "challenge 挑戰",
    }
    VIDEO_TYPE_LABELS = {
        "all": "全部", "dance": "舞蹈", "fancam": "直拍", "stage": "舞台",
        "cute": "可愛", "vlog": "Vlog", "mv": "MV", "challenge": "挑戰",
    }

    # 各類型的標題/時長篩選規則
    # positive: 標題含任一關鍵字 → 加分（不一定要有）
    # negative: 標題含任一關鍵字 → 直接淘汰
    # min/max_duration: 時長範圍（秒），None = 不限制
    VIDEO_TYPE_FILTERS = {
        "fancam": {
            "positive": ["직캠", "fancam", "fan cam", "직찍", "focus", "4k", "1080p",
                         "stage", "무대", "cam"],
            "negative": ["綜藝", "variety", "interview", "訪談", "인터뷰",
                         "reaction", "리액션", "react",
                         "behind", "비하인드", "making", "메이킹",
                         "teaser", "예고", "trailer", "예고편",
                         "official mv", "m/v", "뮤직비디오", "music video",
                         "cover", "커버", "compilation", "compil",
                         "모음", "合集", "合輯", "best of", "best moment",
                         "vlog", "브이로그", "funny", "웃긴",
                         "ep.", "episode", "訪問", "專訪",
                         "綜藝", "예능", "show", "토크", "talk",
                         "announcement", "announce", "발표",
                         "뮤지컬", "musical", "연극", "musical theatre",
                         "musical theater", "curtain call", "커튼콜"],
            "min_duration": 45,
            "max_duration": 900,
        },
        "dance": {
            "positive": ["dance", "안무", "choreography", "practice", "연습",
                         "dance cover", "퍼포먼스", "performance",
                         "fancam", "직캠", "stage", "무대"],
            "negative": ["reaction", "리액션", "tutorial", "lesson", "강좌",
                         "interview", "訪談", "인터뷰", "variety", "綜藝", "예능", "ep.",
                         "teaser", "예고", "compilation", "모음", "合集", "合輯",
                         "edit", "edits", "edits)", "edited",
                         "eng sub", "engsub", "kor sub", "sub)", "subbed",
                         "moments", "best moment", "best of", "cute moment",
                         "behind", "비하인드", "bts", "making", "메이킹",
                         "talk", "토크", "commentary", "review",
                         "announcement", "발표", "vlog", "브이로그",
                         "funny", "웃긴", "meme", "밈",
                         "cover by", "커버 by"],
            "min_duration": 30,
            "max_duration": 900,
            "require_positive": True,  # 舞蹈類必須有白名單命中
        },
        "stage": {
            "positive": ["stage", "무대", "라이브", "live", "performance", "공연",
                         "music bank", "music core", "inkigayo", "show champion",
                         "mcountdown", "music show"],
            "negative": ["reaction", "리액션", "interview", "訪談", "variety",
                         "綜藝", "behind", "비하인드", "teaser", "예고",
                         "official mv", "compilation", "모음", "合集",
                         "뮤지컬", "musical", "연극", "musical theatre",
                         "musical theater", "curtain call", "커튼콜",
                         "musical ost", "뮤지컬ost"],
            "min_duration": 60,
            "max_duration": 900,
        },
        "cute": {
            "positive": ["cute", "귀여운", "funny", "웃긴", "moments", "순간",
                         "adorable", "사랑스러운"],
            "negative": ["reaction", "리액션", "interview", "訪談",
                         "teaser", "예고", "official mv"],
            "min_duration": 15,
            "max_duration": 900,
        },
        "mv": {
            "positive": ["mv", "m/v", "music video", "official", "뮤직비디오"],
            "negative": ["reaction", "리액션", "behind", "비하인드", "making",
                         "메이킹", "teaser", "예고", "cover", "커버",
                         "compilation", "모음", "fancam", "직캠"],
            "min_duration": 60,
            "max_duration": 600,
        },
        "challenge": {
            "positive": ["challenge", "챌린지", "dance challenge"],
            "negative": ["reaction", "리액션", "tutorial", "compilation"],
            "min_duration": 10,
            "max_duration": 180,
        },
    }

    def _filter_by_title_and_duration(results, video_type):
        """根據類型的黑/白名單和時長篩選搜尋結果。"""
        if not video_type or video_type == "all" or video_type not in VIDEO_TYPE_FILTERS:
            return results, 0, 0
        rule = VIDEO_TYPE_FILTERS[video_type]
        pos_kw = [k.lower() for k in rule.get("positive", [])]
        neg_kw = [k.lower() for k in rule.get("negative", [])]
        min_d = rule.get("min_duration")
        max_d = rule.get("max_duration")
        require_pos = rule.get("require_positive", False)

        kept = []
        dropped_title = 0
        dropped_duration = 0
        for r in results:
            title = (r.get("title") or "").lower()
            # 黑名單：命中任一就淘汰
            if any(k in title for k in neg_kw):
                dropped_title += 1
                continue
            # 時長（若有）
            dur = r.get("duration")
            if dur is not None and dur > 0:
                if min_d is not None and dur < min_d:
                    dropped_duration += 1
                    continue
                if max_d is not None and dur > max_d:
                    dropped_duration += 1
                    continue
            # 標記白名單命中數（給排序用）
            hits = sum(1 for k in pos_kw if k in title)
            # require_positive：白名單必須命中才保留（給 dance 這類需嚴格過濾的類型）
            # 例外：title 基本無意義（空字串 / 只有 hashtag）放行由人臉過濾處理
            import re as _re
            _title_no_tags = _re.sub(r"#\S+|@\S+", " ", title)
            _title_words = _re.findall(r"[a-zA-Z가-힣]{3,}", _title_no_tags)
            _meaningful_title = len(_title_words) >= 2
            if require_pos and hits == 0 and _meaningful_title:
                dropped_title += 1
                continue
            r["_filter_hits"] = hits
            kept.append(r)

        # 按白名單命中數排序（有命中的優先）
        kept.sort(key=lambda r: r.get("_filter_hits", 0), reverse=True)
        return kept, dropped_title, dropped_duration

    # 查詢變體：同一個類型準備多個查詢角度，每次隨機挑幾個跑
    # 目的是擴大候選池，避免重複抓到 YouTube 穩定 top-N 的那批影片
    def _build_query_variants(base_kw, vtype):
        p = base_kw.strip()
        _year = _random.choice(["2022", "2023", "2024", "2025", "2026"])
        _shows = ["music bank", "inkigayo", "mcountdown", "show champion",
                  "music core", "the show", "simply kpop"]
        if vtype == "fancam":
            variants = [
                f"{p} 직캠",
                f"{p} fancam 4K",
                f"{p} focus cam",
                f"{p} 직캠 무대",
                f"{p} fancam stage",
                f"{p} 직캠 {_year}",
                f"{p} {_random.choice(_shows)} 직캠",
                f"{p} 직찍",
                f"{p} live fancam",
                f"{p} 직캠 4K",
            ]
        elif vtype == "dance":
            variants = [
                f"{p} dance practice",
                f"{p} 안무 영상",
                f"{p} choreography",
                f"{p} dance cover",
                f"{p} 퍼포먼스",
                f"{p} dance practice 4K",
                f"{p} {_year} dance",
                f"{p} 안무 practice",
            ]
        elif vtype == "stage":
            variants = [
                f"{p} stage",
                f"{p} 무대",
                f"{p} live performance",
                f"{p} 공연",
                f"{p} {_random.choice(_shows)}",
                f"{p} stage {_year}",
                f"{p} live stage 4K",
                f"{p} 무대 {_year}",
            ]
        elif vtype == "cute":
            variants = [
                f"{p} cute moments",
                f"{p} 귀여운",
                f"{p} funny moments",
                f"{p} 웃긴",
                f"{p} adorable",
                f"{p} cute compilation",
                f"{p} 사랑스러운",
            ]
        elif vtype == "vlog":
            variants = [
                f"{p} vlog",
                f"{p} 브이로그",
                f"{p} daily vlog",
                f"{p} behind",
                f"{p} 일상",
            ]
        elif vtype == "mv":
            variants = [
                f"{p} official mv",
                f"{p} music video",
                f"{p} m/v",
                f"{p} 뮤직비디오",
                f"{p} {_year} mv",
            ]
        elif vtype == "challenge":
            variants = [
                f"{p} challenge",
                f"{p} 챌린지",
                f"{p} dance challenge",
                f"{p} tiktok challenge",
            ]
        else:
            # all / 未知類型
            variants = [p, f"{p} 4K", f"{p} {_year}", f"{p} best"]
        # 隨機挑 3-4 個變體
        n = min(4, len(variants))
        return _random.sample(variants, n)

    def _youtube_multi_search(base_kw, vtype, target_count, q_ch, shorts=False):
        """用多個查詢變體搜尋 YouTube，混用 ytsearch 和 ytsearchdate，合併去重。"""
        variants = _build_query_variants(base_kw, vtype)
        per_query = max(50, target_count // 2 + 20)  # 每個變體至少抓 50 部
        merged = {}  # id -> entry dict
        for i, vkw in enumerate(variants):
            # 交錯使用 relevance 和 date 排序
            prefix = "ytsearchdate" if (i % 2 == 1) else "ytsearch"
            if shorts:
                actual_q = f"{vkw} #shorts"
                ydl_opts = {
                    "quiet": True, "no_warnings": True, "extract_flat": True,
                    "match_filter": "duration < 120",
                }
            else:
                actual_q = vkw
                ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
            q_ch.put({"type": "progress", "phase": "search", "percent": 1 + i,
                      "message": f"🔍 變體 {i+1}/{len(variants)}: \"{vkw}\" ({prefix})"})
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(f"{prefix}{per_query}:{actual_q}", download=False)
                    entries = info.get("entries") or []
                for e in entries:
                    if not e:
                        continue
                    vid = e.get("id", "")
                    if not vid or vid in merged:
                        continue
                    merged[vid] = {
                        "url": e.get("url") or e.get("webpage_url")
                               or f"https://www.youtube.com/watch?v={vid}",
                        "title": e.get("title", ""),
                        "channel": e.get("channel") or e.get("uploader", ""),
                        "id": vid,
                        "duration": e.get("duration"),
                        "_query": vkw,
                    }
            except Exception as e:
                logging.warning(f"query variant failed ({vkw}): {e}")
                continue
        return list(merged.values())

    actual_keyword = search_keyword
    # 附加已知團名 (celeb_groups) 以縮小搜尋範圍 —— 對於 Ahyeon 這種常見名字
    # 直接搜 "Ahyeon" 會混入許多同名人物；加上 "babymonster" 能讓結果大幅聚焦
    try:
        _groups_map = _load_celeb_groups()
        _group_name = _groups_map.get((person or "").lower().strip(), "")
        if _group_name and _group_name.lower() not in actual_keyword.lower():
            actual_keyword = f"{actual_keyword} {_group_name}"
            logging.info(f"search query augmented with group '{_group_name}' → '{actual_keyword}'")
    except Exception as _e:
        logging.warning(f"group-augmentation skipped: {_e}")
    if video_type and video_type != "all" and video_type in VIDEO_TYPE_KW:
        actual_keyword = f"{actual_keyword} {VIDEO_TYPE_KW[video_type]}"

    person_folder = sanitize_name(person)

    # ═══ 本地模式：直接用本地已下載的影片 ═══
    if source == "local":
        dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
        exts = {".mp4", ".mkv", ".webm"}

        # 掃描本地影片：掃描整個人名目錄（含所有子資料夾）
        local_videos = []
        seen_paths = set()
        if os.path.isdir(dl_dir):
            for root, dirs, files in os.walk(dl_dir):
                for f in files:
                    fp = os.path.join(root, f)
                    if fp in seen_paths:
                        continue
                    if os.path.splitext(f)[1].lower() in exts \
                       and os.path.getsize(fp) > 10240 and not f.endswith(".part"):
                        local_videos.append(fp)
                        seen_paths.add(fp)

        if not local_videos:
            type_label = VIDEO_TYPE_LABELS.get(video_type, video_type)
            q.put({"type": "error",
                   "message": f"本地找不到 {person} 的影片"
                              f"（類型: {type_label}）\n"
                              f"請先用「線上搜尋」下載影片，或將影片放到 YouTube/downloads/{person_folder}/ 資料夾"})
            return

        type_label = VIDEO_TYPE_LABELS.get(video_type, video_type)
        total_local = len(local_videos)
        q.put({"type": "progress", "phase": "search", "percent": 5,
               "message": f"📂 本地影片庫：找到 {total_local} 個影片（{type_label}）"})

        # ── 智慧取樣：優先使用已有擷取結果的影片，再隨機補足到 max_videos ──
        _psuffix = f"_{sanitize_name(person)}.mp4"
        _existing_extract_bases = set(_scan_extract_files(sanitize_name(person), _psuffix).keys())

        # 將本地影片分為「已擷取過」和「未擷取」兩組
        cached_videos = []
        uncached_videos = []
        for fp in local_videos:
            base = os.path.splitext(os.path.basename(fp))[0]
            if base in _existing_extract_bases:
                cached_videos.append(fp)
            else:
                uncached_videos.append(fp)

        # 取樣邏輯：大部分用快取（免費），但保留 30% 名額給新影片以逐步擴充快取
        use_limit = max(max_videos, 5)  # 至少挑 5 部
        new_slots = max(int(use_limit * 0.3), 2)  # 至少 2 部新影片
        cached_slots = use_limit - new_slots

        selected_videos = []

        if not uncached_videos:
            # 全部都已擷取過，直接從快取挑
            selected_videos = _random.sample(cached_videos, min(use_limit, len(cached_videos)))
        elif not cached_videos:
            # 完全沒有快取，全部挑新的
            selected_videos = _random.sample(uncached_videos, min(use_limit, len(uncached_videos)))
        else:
            # 混合：快取 + 新影片
            pick_cached = min(cached_slots, len(cached_videos))
            pick_new = min(new_slots, len(uncached_videos))
            # 如果快取不夠填滿 cached_slots，多給新影片
            if pick_cached < cached_slots:
                pick_new = min(use_limit - pick_cached, len(uncached_videos))
            selected_videos = _random.sample(cached_videos, pick_cached)
            selected_videos.extend(_random.sample(uncached_videos, pick_new))

        # 打亂順序增加多樣性
        _random.shuffle(selected_videos)

        cached_count = sum(1 for fp in selected_videos
                          if os.path.splitext(os.path.basename(fp))[0] in _existing_extract_bases)
        new_count = len(selected_videos) - cached_count

        q.put({"type": "progress", "phase": "search", "percent": 8,
               "message": f"🎲 從 {total_local} 部中挑選 {len(selected_videos)} 部"
                          f"（{cached_count} 部有快取、{new_count} 部需掃描）"})

        # 包裝成 downloaded_files 格式，直接進入 Phase 3
        downloaded_files = []
        for fp in selected_videos:
            fname = os.path.basename(fp)
            rel = os.path.relpath(fp, YT_ROOT).replace("\\", "/")
            downloaded_files.append({
                "filename": fname,
                "rel_path": rel,
                "full_path": fp,
                "_is_local": True,  # 標記為本地，不要在最後刪除
            })

        # 跳到 Phase 2.5（照片） 然後 Phase 3（擷取）
        # 先設好 dl_dir 給後續用
        q.put({"type": "progress", "phase": "download", "percent": 30,
               "message": f"📂 使用 {len(downloaded_files)} 個本地影片"
                          f"（從 {total_local} 部中智慧取樣）"})

        # 後面的 pipeline 會用 downloaded_files
        # 跳過 Phase 1 和 Phase 2
    else:
        # ═══ 線上模式：搜尋 + 下載 ═══
        dl_dir = os.path.join(YT_DOWNLOADS, person_folder)
        # 線上下載時，如果有指定類型，存到子資料夾
        if video_type and video_type != "all":
            dl_dir = os.path.join(dl_dir, video_type)
        os.makedirs(dl_dir, exist_ok=True)
        # 掃描整個人名目錄（含所有子資料夾）的已下載 ID，用於去重
        existing_ids = set()
        _dl_person = os.path.join(YT_DOWNLOADS, person_folder)
        if os.path.isdir(_dl_person):
            for _root, _dirs, _files in os.walk(_dl_person):
                for fname in _files:
                    m = re.search(r'\[([A-Za-z0-9_-]+)\]\.', fname)
                    if m:
                        existing_ids.add(m.group(1))
                    else:
                        base = os.path.splitext(fname)[0]
                        if re.match(r'^[A-Za-z0-9_-]+$', base):
                            existing_ids.add(base)

        # ── Phase 1: 搜尋 (0-5%) ──
        # 搜尋數量 = 想要的 + 已有的，確保跳過舊影片後還有足夠新影片
        search_count = max_videos + len(existing_ids) + 10
        PLATFORM_LABELS = {
            "all": "全部平台",
            "tiktok": "TikTok", "youtube": "YouTube",
            "yt_shorts": "YouTube Shorts", "ig_reels": "Instagram Reels",
        }
        platform_label = PLATFORM_LABELS.get(platform, platform)
        q.put({"type": "progress", "phase": "search", "percent": 0,
               "message": f"🔍 搜尋 {platform_label}: {actual_keyword}（目標 {search_count} 部，已有 {len(existing_ids)} 部）..."})

        if platform == "all":
            # 跨平台合併：平均分配 search_count 到 3 個核心來源
            # （Instagram 需登入容易失敗，不納入預設合併）
            _per = max(1, search_count // 3)
            merged_results = []
            seen_ids = set()

            def _merge(batch, tag):
                added = 0
                for r in batch or []:
                    rid = r.get("id") or r.get("url")
                    if not rid or rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    r["_src_platform"] = tag
                    merged_results.append(r)
                    added += 1
                return added

            q.put({"type": "progress", "phase": "search", "percent": 1,
                   "message": f"🔍 跨平台搜尋 TikTok / YouTube / YouTube Shorts（每平台 ~{_per} 部）..."})
            try:
                tt_batch = _tiktok_search(actual_keyword, _per)
                q.put({"type": "progress", "phase": "search", "percent": 2,
                       "message": f"  TikTok：{_merge(tt_batch, 'tiktok')} 部"})
            except Exception as e:
                logging.warning(f"all-platform: TikTok search failed: {e}")
            try:
                yts_batch = _youtube_multi_search(search_keyword, video_type, _per, q, shorts=True)
                q.put({"type": "progress", "phase": "search", "percent": 3,
                       "message": f"  YouTube Shorts：{_merge(yts_batch, 'yt_shorts')} 部"})
            except Exception as e:
                logging.warning(f"all-platform: YT Shorts search failed: {e}")
            try:
                yt_batch = _youtube_multi_search(search_keyword, video_type, _per, q, shorts=False)
                q.put({"type": "progress", "phase": "search", "percent": 4,
                       "message": f"  YouTube：{_merge(yt_batch, 'youtube')} 部"})
            except Exception as e:
                logging.warning(f"all-platform: YouTube search failed: {e}")

            search_results = merged_results
            logging.info(f"all-platform merged {len(search_results)} unique videos "
                         f"({sum(1 for r in search_results if r.get('_src_platform')=='tiktok')} TT + "
                         f"{sum(1 for r in search_results if r.get('_src_platform')=='yt_shorts')} YTS + "
                         f"{sum(1 for r in search_results if r.get('_src_platform')=='youtube')} YT)")

        elif platform == "tiktok":
            if actual_keyword.startswith("@"):
                # yt-dlp 抓使用者頁面
                try:
                    tiktok_url = f"https://www.tiktok.com/{actual_keyword}"
                    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(tiktok_url, download=False)
                        entries = (info.get("entries") or [])[:search_count]
                    search_results = [{
                        "url": e.get("url") or e.get("webpage_url") or f"https://www.tiktok.com/video/{e.get('id','')}",
                        "title": e.get("title", ""),
                        "channel": e.get("channel") or e.get("uploader", ""),
                        "id": e.get("id", ""),
                        "duration": e.get("duration"),
                    } for e in entries if e]
                except Exception as e:
                    q.put({"type": "error", "message": f"TikTok 搜尋失敗: {e}"})
                    return
            else:
                # 搜尋更多，跳過已有的後確保還有足夠新影片
                search_results = _tiktok_search(actual_keyword, search_count)

        elif platform == "yt_shorts":
            # YouTube Shorts 多變體搜尋
            try:
                search_results = _youtube_multi_search(
                    search_keyword, video_type, search_count, q, shorts=True)
            except Exception as e:
                q.put({"type": "error", "message": f"YouTube Shorts 搜尋失敗: {e}"})
                return

        elif platform == "ig_reels":
            # Instagram Reels 搜尋（透過 yt-dlp 的 hashtag 或 user 搜尋）
            try:
                # 支援 @username 或 hashtag
                if actual_keyword.startswith("@"):
                    ig_url = f"https://www.instagram.com/{actual_keyword.lstrip('@')}/reels/"
                else:
                    # 用 hashtag 搜尋
                    tag = actual_keyword.replace(" ", "").lower()
                    ig_url = f"https://www.instagram.com/explore/tags/{tag}/"

                q.put({"type": "progress", "phase": "search", "percent": 2,
                       "message": f"📸 抓取 Instagram: {ig_url}"})
                ydl_opts = {
                    "quiet": True, "no_warnings": True, "extract_flat": True,
                    "playlistend": search_count,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(ig_url, download=False)
                    entries = (info.get("entries") or [])[:search_count]
                search_results = [{
                    "url": e.get("url") or e.get("webpage_url", ""),
                    "title": e.get("title") or e.get("description", "")[:80],
                    "channel": e.get("channel") or e.get("uploader", ""),
                    "id": e.get("id", ""),
                    "duration": e.get("duration"),
                } for e in entries if e]
            except Exception as e:
                q.put({"type": "error", "message": f"Instagram Reels 搜尋失敗: {e}\n"
                       f"提示：IG 可能需要登入。請在搜尋欄輸入 @用戶名 來抓取特定帳號的 Reels"})
                return

        else:
            # YouTube 多變體搜尋
            try:
                search_results = _youtube_multi_search(
                    search_keyword, video_type, search_count, q, shorts=False)
            except Exception as e:
                q.put({"type": "error", "message": f"YouTube 搜尋失敗: {e}"})
                return

        if not search_results:
            q.put({"type": "error", "message": f"搜尋「{actual_keyword}」沒有結果"})
            return

        # 多變體搜尋會自動合併去重，這裡顯示合併後的候選數
        if platform in ("youtube", "yt_shorts"):
            q.put({"type": "progress", "phase": "search", "percent": 2,
                   "message": f"🔎 合併多變體候選池：{len(search_results)} 部"})

        # 過濾已下載過的影片
        before_filter = len(search_results)
        search_results = [v for v in search_results if v.get("id", "") not in existing_ids]
        skipped = before_filter - len(search_results)
        if skipped:
            q.put({"type": "progress", "phase": "search", "percent": 3,
                   "message": f"⏭️ 跳過 {skipped} 個已下載過的影片"})

        if not search_results:
            q.put({"type": "error", "message": f"搜尋到的影片都已經下載過了，請換個關鍵字或類型"})
            return

        # 套用類型篩選（黑/白名單 + 時長）
        before_type_filter = len(search_results)
        search_results, dropped_title, dropped_duration = _filter_by_title_and_duration(
            search_results, video_type)
        type_dropped = before_type_filter - len(search_results)
        if type_dropped > 0:
            q.put({"type": "progress", "phase": "search", "percent": 4,
                   "message": f"🚫 類型篩選淘汰 {type_dropped} 個"
                              f"（標題不符 {dropped_title}、時長不符 {dropped_duration}）"})
        if not search_results:
            q.put({"type": "error",
                   "message": f"所有搜尋結果都被類型篩選淘汰了，請換個關鍵字或調整類型"})
            return

        # 依白名單命中分組：命中的優先，然後 shuffle，但先不截到 max_videos
        hit_results = [r for r in search_results if r.get("_filter_hits", 0) > 0]
        nohit_results = [r for r in search_results if r.get("_filter_hits", 0) == 0]
        _random.shuffle(hit_results)
        _random.shuffle(nohit_results)
        search_results = hit_results + nohit_results

        # ── 縮圖人臉預篩選（僅 YouTube / YouTube Shorts）──
        # 目的：下載前先用縮圖檢查是否含目標人物，避免抓到錯誤的人
        thumb_filter_applied = False
        if platform in ("youtube", "yt_shorts") and len(search_results) > max_videos:
            try:
                q.put({"type": "progress", "phase": "search", "percent": 4,
                       "message": f"🧑 建立 {person} 的臉部特徵（縮圖預檢用）..."})
                _ref_for_thumb = _build_reference_embeddings(person)
                _neg_for_thumb = _build_neg_embeddings(sanitize_name(person))
                if _ref_for_thumb is not None and len(_ref_for_thumb) >= 3:
                    _thumb_det, _thumb_rec = _build_face_models()
                    if _neg_for_thumb is not None:
                        q.put({"type": "progress", "phase": "search", "percent": 4,
                               "message": f"🛡️ 載入 {len(_neg_for_thumb)} 張反參考（團員拒絕）"})
                    # 只需要處理到湊滿 max_videos 的 passed 就可以停，但為了優先挑最佳候選，
                    # 先全部檢查再排序（候選池通常 < 200，不會太慢）
                    check_limit = min(len(search_results), max_videos * 5 + 20)
                    to_check = search_results[:check_limit]
                    untested = search_results[check_limit:]

                    _matched, _rejected, _no_face, _errors = [], [], [], []
                    # 序列處理（避免 OpenCV recognizer 在多執行緒的狀態衝突）
                    for i, v in enumerate(to_check):
                        vid = v.get("id", "")
                        if (i % 10) == 0:
                            q.put({"type": "progress", "phase": "search", "percent": 4,
                                   "message": f"🧑 縮圖預檢 {i}/{len(to_check)}："
                                              f"✅{len(_matched)} ❌{len(_rejected)} ⚠️{len(_no_face)+len(_errors)}"})
                        img = _fetch_youtube_thumbnail(vid)
                        if img is None:
                            _errors.append(v)
                            continue
                        status, score = _thumbnail_face_match(
                            img, _ref_for_thumb, _thumb_det, _thumb_rec,
                            threshold=0.35, neg_embeddings=_neg_for_thumb)
                        v["_thumb_score"] = score
                        v["_thumb_status"] = status
                        if status == "match":
                            _matched.append(v)
                        elif status == "reject":
                            _rejected.append(v)
                        elif status == "no_face":
                            _no_face.append(v)
                        else:
                            _errors.append(v)
                        # 提早中止：已經湊到 max_videos * 2 個 match 就夠了
                        if len(_matched) >= max_videos * 2:
                            untested = to_check[i+1:] + untested
                            break

                    # 優先順序：match（高分在前）> no_face > error > untested > reject
                    _matched.sort(key=lambda r: r.get("_thumb_score", 0), reverse=True)
                    search_results = _matched + _no_face + _errors + untested + _rejected
                    thumb_filter_applied = True
                    q.put({"type": "progress", "phase": "search", "percent": 4,
                           "message": f"🧑 縮圖預檢完成："
                                      f"✅ {len(_matched)} 通過｜"
                                      f"❌ {len(_rejected)} 淘汰｜"
                                      f"⚠️ {len(_no_face)+len(_errors)} 無法判斷"})
                else:
                    q.put({"type": "progress", "phase": "search", "percent": 4,
                           "message": f"⚠️ 參考臉部特徵不足，略過縮圖預檢"})
            except Exception as e:
                logging.warning(f"thumbnail pre-check failed: {e}")
                q.put({"type": "progress", "phase": "search", "percent": 4,
                       "message": f"⚠️ 縮圖預檢失敗，略過（{e}）"})

        # 截到 max_videos
        search_results = search_results[:max_videos]

        _msg_tail = "（已縮圖預檢）" if thumb_filter_applied else ""
        q.put({"type": "progress", "phase": "search", "percent": 5,
               "message": f"✅ 找到 {len(search_results)} 個新影片（跳過 {skipped} 個已有）{_msg_tail}"})

        # ── Phase 2: 並行下載 (5-35%) ──
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading as _threading

        PARALLEL_DL = len(search_results)  # 全部同時下載

        # 每個下載任務的進度追蹤
        _dl_progress = {}  # vi -> {pct, speed, eta}
        _dl_status = {}    # vi -> "downloading" | "done" | "failed" | "merging"
        _dl_lock = _threading.Lock()
        _done_count = [0]

        def _update_dl_display():
            """彙整所有下載中的進度到一條訊息"""
            with _dl_lock:
                active = []
                for vi in sorted(_dl_progress.keys()):
                    st = _dl_status.get(vi, "")
                    if st == "downloading":
                        info = _dl_progress[vi]
                        parts = [f"#{vi+1}:{info.get('pct',0)}%"]
                        if info.get("speed"):
                            parts.append(info["speed"])
                        active.append(" ".join(parts))
                    elif st == "merging":
                        active.append(f"#{vi+1}:合併中")
                done = _done_count[0]
                total = len(search_results)
                overall_pct = 5 + int(done / total * 25)

            if active:
                msg = f"📥 並行下載 ({done}/{total} 完成) | " + " | ".join(active)
            else:
                msg = f"📥 下載中 {done}/{total}..."
            q.put({"type": "progress", "phase": "download",
                   "percent": min(overall_pct, 30), "message": msg})

        def _download_one(vi, v):
            url = v.get("url", "")
            title = v.get("title", "") or f"video_{vi+1}"
            vid_id = v.get("id", "")

            with _dl_lock:
                _dl_status[vi] = "downloading"
                _dl_progress[vi] = {"pct": 0, "speed": ""}

            # 清除 .part 殘留
            if vid_id:
                for old_f in os.listdir(dl_dir):
                    if vid_id in old_f and old_f.endswith(".part"):
                        try:
                            os.remove(os.path.join(dl_dir, old_f))
                        except OSError:
                            pass

            out_tpl = os.path.join(dl_dir, "%(id)s.%(ext)s")
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "--no-playlist", "--windows-filenames", "--restrict-filenames",
                "--file-access-retries", "10",
                "-f", "best[height<=720]/best",
                "-o", out_tpl, "--newline", "--no-warnings",
                "--write-info-json",  # 寫 {id}.info.json 讓後續擷取文案用
            ]
            if FFMPEG_LOCATION:
                cmd += ["--ffmpeg-location", FFMPEG_LOCATION]
            cmd.append(url)

            logging.info(f"auto-video dl[{vi}]: {url}")

            _dl_ok = False
            for _attempt in range(2):
                try:
                    proc = subprocess.Popen(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        cwd=dl_dir,
                        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                    )
                    for raw_line in proc.stdout:
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        m = re.search(r'\[download\]\s+([\d.]+)%', line)
                        if m:
                            dl_pct = min(int(float(m.group(1))), 99)
                            speed_m = re.search(r'at\s+([\d.]+\s*\S+/s)', line)
                            with _dl_lock:
                                _dl_progress[vi] = {
                                    "pct": dl_pct,
                                    "speed": speed_m.group(1) if speed_m else "",
                                }
                            _update_dl_display()
                        elif "[Merger]" in line or "Merging" in line:
                            with _dl_lock:
                                _dl_status[vi] = "merging"
                            _update_dl_display()

                    proc.wait(timeout=420)
                    if proc.returncode == 0:
                        _dl_ok = True
                        break
                    logging.warning(f"auto-video dl[{vi}] attempt {_attempt+1} failed")
                except subprocess.TimeoutExpired:
                    logging.warning(f"auto-video dl[{vi}] timeout attempt {_attempt+1}")
                    try:
                        proc.kill()
                    except Exception:
                        pass

            if not _dl_ok:
                with _dl_lock:
                    _dl_status[vi] = "failed"
                    _done_count[0] += 1
                _update_dl_display()
                return None

            # 找到下載的檔案
            actual_file = None
            if vid_id:
                for f in sorted(os.listdir(dl_dir),
                                key=lambda x: (os.path.getmtime(os.path.join(dl_dir, x)) if os.path.exists(os.path.join(dl_dir, x)) else 0),
                                reverse=True):
                    fp_c = os.path.join(dl_dir, f)
                    if (os.path.isfile(fp_c) and vid_id in f
                            and not f.endswith(".part")
                            and os.path.getsize(fp_c) > 10240):
                        actual_file = f
                        break
            if not actual_file and vid_id:
                for f in os.listdir(dl_dir):
                    fp_c = os.path.join(dl_dir, f)
                    if (os.path.isfile(fp_c) and vid_id in f
                            and f.endswith(".part")
                            and os.path.getsize(fp_c) > 1024 * 1024):
                        new_name = f[:-5]
                        try:
                            os.rename(fp_c, os.path.join(dl_dir, new_name))
                            actual_file = new_name
                        except Exception:
                            actual_file = f
                        break

            with _dl_lock:
                _dl_status[vi] = "done"
                _done_count[0] += 1
            _update_dl_display()

            if actual_file:
                logging.info(f"auto-video dl[{vi}]: {actual_file} "
                             f"({os.path.getsize(os.path.join(dl_dir, actual_file))/1024/1024:.1f}MB)")
                return {
                    "filename": actual_file,
                    "rel_path": os.path.relpath(os.path.join(dl_dir, actual_file), YT_ROOT).replace("\\", "/"),
                    "full_path": os.path.join(dl_dir, actual_file),
                }
            return None

        downloaded_files = []
        with ThreadPoolExecutor(max_workers=PARALLEL_DL) as executor:
            futures = {executor.submit(_download_one, vi, v): vi
                       for vi, v in enumerate(search_results)}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    downloaded_files.append(result)

        q.put({"type": "progress", "phase": "download", "percent": 30,
               "message": f"📥 下載完成：{len(downloaded_files)}/{len(search_results)} 個影片"})

        if not downloaded_files:
            q.put({"type": "error", "message": "所有影片都下載失敗"})
            return

        q.put({"type": "progress", "phase": "download", "percent": 30,
               "message": f"📥 下載完成：{len(downloaded_files)} 個影片"})

    # ── Phase 2.5: 自動下載照片（若不足）(30-38%) ──
    photo_dir = os.path.join(DOWNLOAD_ROOT, person)
    existing_photos = 0
    if os.path.isdir(photo_dir):
        img_exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
        existing_photos = sum(1 for f in os.listdir(photo_dir)
                              if os.path.splitext(f)[1].lower() in img_exts)

    MIN_PHOTOS_FOR_FACE = 10
    if existing_photos < MIN_PHOTOS_FOR_FACE:
        need = max(50, MIN_PHOTOS_FOR_FACE)
        q.put({"type": "progress", "phase": "photos", "percent": 30,
               "message": f"📸 {person} 照片不足（現有 {existing_photos} 張），從 Pinterest 下載 {need} 張..."})
        try:
            scraper = PinterestImageScraper()
            search_kw = f"{person} photo"
            def _pin_cb(msg):
                q.put({"type": "progress", "phase": "photos", "percent": 32,
                       "message": f"📸 {msg}"})
            urls = scraper.search(search_kw, need, "", callback=_pin_cb)
            if not urls:
                # Pinterest 失敗，fallback 到 Bing
                q.put({"type": "progress", "phase": "photos", "percent": 32,
                       "message": f"📸 Pinterest 無結果，改用 Bing 搜尋..."})
                bing_scraper = BingImageScraper()
                urls = bing_scraper.search(search_kw, need, "", callback=_pin_cb)

            if urls:
                q.put({"type": "progress", "phase": "photos", "percent": 34,
                       "message": f"📸 找到 {len(urls)} 個連結，下載中..."})
                img_dl = ImageDownloader(db, person, "pinterest")
                img_dl.download_all(
                    urls, dedup_url=True, dedup_md5=True, dedup_phash=True,
                    progress_cb=lambda cur, tot, stats: q.put({
                        "type": "progress", "phase": "photos",
                        "percent": 34 + int(cur / max(tot, 1) * 4),
                        "message": f"📸 下載照片 {cur}/{tot}（成功 {stats['downloaded']}）"
                    }),
                )
                q.put({"type": "progress", "phase": "photos", "percent": 38,
                       "message": f"📸 照片下載完成（新增 {img_dl.stats['downloaded']} 張）"})
                # 清除 embedding 快取，讓新照片生效
                cache_path = os.path.join(photo_dir, ".face_embeddings.npy")
                if os.path.isfile(cache_path):
                    os.remove(cache_path)
            else:
                q.put({"type": "progress", "phase": "photos", "percent": 38,
                       "message": f"⚠️ 無法從網路下載照片，嘗試用現有 {existing_photos} 張"})
        except Exception as e:
            logging.warning(f"auto-video photo download failed: {e}")
            q.put({"type": "progress", "phase": "photos", "percent": 38,
                   "message": f"⚠️ 照片下載失敗: {e}，嘗試用現有照片"})
    else:
        q.put({"type": "progress", "phase": "photos", "percent": 38,
               "message": f"📸 已有 {existing_photos} 張照片，跳過下載"})

    # ── Phase 3: 人臉擷取 (38-70%) ──
    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)

    # 總是建立人臉特徵（Phase 4 精華評分也需要用來辨認目標人物）
    q.put({"type": "progress", "phase": "extract", "percent": 38,
           "message": f"🧑 建立 {person} 的臉部特徵..."})

    def emb_cb(pct):
        p = 38 + int(pct * 4)
        q.put({"type": "progress", "phase": "extract", "percent": min(p, 42),
               "message": f"建立臉部特徵 {int(pct*100)}%"})

    ref = _build_reference_embeddings(person, emb_cb)
    if ref is None or len(ref) < 3:
        q.put({"type": "error",
               "message": f"{person} 的照片中找不到足夠的臉部特徵（至少需要 3 張有臉的照片）"})
        return

    # 載入反參考 embeddings（若 .negatives/ 存在）— 用來拒絕其他團員
    neg_ref = _build_neg_embeddings(sanitize_name(person))
    if neg_ref is not None:
        q.put({"type": "progress", "phase": "extract", "percent": 42,
               "message": f"🛡️ 使用 {len(ref)} 張正參考 + {len(neg_ref)} 張反參考（團員拒絕啟用）"})
    else:
        q.put({"type": "progress", "phase": "extract", "percent": 42,
               "message": f"✅ 使用 {len(ref)} 張臉部特徵"})

    # 檢查已有的擷取結果（*_person.mp4），可跳過已處理的影片（遞迴掃描含子資料夾）
    _person_suffix = f"_{sanitize_name(person)}.mp4"
    existing_extracts = _scan_extract_files(sanitize_name(person), _person_suffix)

    # 判斷哪些影片需要重新擷取
    need_extract = []
    already_extracted = []
    for dlf in downloaded_files:
        vname = dlf["filename"]
        base = os.path.splitext(vname)[0]
        if base in existing_extracts:
            already_extracted.append(existing_extracts[base])
        else:
            need_extract.append(dlf)

    if already_extracted and not need_extract:
        # 全部都已經擷取過，直接跳到 Phase 4
        q.put({"type": "progress", "phase": "extract", "percent": 70,
               "message": f"🎯 已有 {len(already_extracted)} 個擷取結果，跳過重新掃描"})
        extracted_files = already_extracted
    else:
        if already_extracted:
            q.put({"type": "progress", "phase": "extract", "percent": 42,
                   "message": f"📂 已有 {len(already_extracted)} 個擷取結果，"
                              f"還需掃描 {len(need_extract)} 個新影片"})

        extracted_files = list(already_extracted)  # 先加入已有的

        scan_list = need_extract if need_extract else downloaded_files
        for vi, dlf in enumerate(scan_list):
            video_path = dlf["full_path"]
            vname = dlf["filename"]
            pct_base = 42 + int(vi / max(len(scan_list), 1) * 23)

            q.put({"type": "progress", "phase": "extract", "percent": pct_base,
                   "message": f"🔍 掃描 {vi+1}/{len(scan_list)}: {vname[:40]}..."})

            def scan_cb(pct, _vi=vi):
                p = 42 + int((_vi + pct) / max(len(scan_list), 1) * 23)
                q.put({"type": "progress", "phase": "extract", "percent": min(p, 65),
                       "message": f"掃描 {vname[:30]}... {int(pct*100)}%"})

            # Per-video 快取：若影片未改且 ref/neg 特徵沒變就直接用快取
            _ref_sig = _embeddings_sig(ref)
            _neg_sig = _embeddings_sig(neg_ref)
            _cached = _load_video_cache(video_path)
            timestamps = None
            if (_cached and _cached.get("ref_sig") == _ref_sig
                    and _cached.get("neg_sig") == _neg_sig
                    and "timestamps" in _cached):
                timestamps = _cached["timestamps"]
                q.put({"type": "progress", "phase": "extract", "percent": pct_base + 2,
                       "message": f"⚡ {vname[:40]} 快取命中（{len(timestamps)} 個時間點）"})
            else:
                try:
                    timestamps = _scan_video_for_person(video_path, ref, scan_cb,
                                                          neg_embeddings=neg_ref)
                    _save_video_cache(video_path, {
                        "ref_sig": _ref_sig, "neg_sig": _neg_sig,
                        "timestamps": timestamps,
                    })
                except Exception as _se:
                    logging.warning(f"scan failed for {vname}: {_se}")
                    q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                           "message": f"⚠️ {vname[:40]} 掃描失敗，略過（{_se}）"})
                    continue
            if not timestamps:
                q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                       "message": f"⏭️ {vname[:40]} 中未偵測到 {person}"})
                continue

            segments = _merge_segments(timestamps)

            # 取得來源影片時長，修剪過於接近結尾的片段（避免 TikTok 片尾）
            _src_dur = None
            try:
                _cap = cv2.VideoCapture(video_path)
                if _cap.isOpened():
                    _fps = _cap.get(cv2.CAP_PROP_FPS) or 30
                    _fc = _cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    _src_dur = _fc / _fps if _fps > 0 else None
                    _cap.release()
            except Exception:
                pass
            if _src_dur and _src_dur > 10:
                cutoff = _src_dur - 5.0  # 最後 5 秒視為片尾區域
                trimmed = []
                for s, e in segments:
                    if s >= cutoff:
                        continue  # 整段都在片尾區
                    e = min(e, cutoff)
                    if e - s >= 1.0:
                        trimmed.append((s, e))
                segments = trimmed
                if not segments:
                    q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                           "message": f"⏭️ {vname[:40]}: 片段都在片尾區，跳過"})
                    continue

            total_dur = sum(e - s for s, e in segments)

            base = os.path.splitext(os.path.basename(vname))[0]
            out_name_ext = f"{base}_{sanitize_name(person)}.mp4"
            vtype = _infer_video_type_from_path(video_path, sanitize_name(person))
            if vtype:
                _typed_extract_dir = os.path.join(extract_dir, vtype)
            else:
                _typed_extract_dir = extract_dir
            os.makedirs(_typed_extract_dir, exist_ok=True)
            out_path = os.path.join(_typed_extract_dir, out_name_ext)
            result = _extract_segments(video_path, segments, out_path)

            if result and os.path.isfile(result):
                extracted_files.append(result)
                q.put({"type": "progress", "phase": "extract",
                       "percent": 42 + int((vi + 1) / max(len(scan_list), 1) * 23),
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
    _score_ref_sig = _embeddings_sig(ref)
    _score_neg_sig = _embeddings_sig(neg_ref)
    _cache_hits = 0
    for vi, vpath in enumerate(extracted_files):
        vname = os.path.basename(vpath)
        def score_cb(pct, _vi=vi):
            p = 70 + int((_vi + pct) / len(extracted_files) * 15)
            q.put({"type": "progress", "phase": "highlight", "percent": min(p, 85),
                   "message": f"分析 {vname[:30]}... {int(pct*100)}%"})

        # Per-video 快取：若影片、ref/neg、strategy、scoring 版本都沒變才用快取
        # _SCORE_VERSION: 每次改 scoring 邏輯就遞增，讓舊快取失效
        _SCORE_VERSION = 5
        scores = None
        _cached = _load_video_cache(vpath)
        if (_cached and _cached.get("score_ref_sig") == _score_ref_sig
                and _cached.get("score_neg_sig") == _score_neg_sig
                and _cached.get("strategy") == strategy
                and _cached.get("score_version") == _SCORE_VERSION
                and "scores" in _cached):
            scores = _cached["scores"]
            _cache_hits += 1
        else:
            try:
                scores = _score_video_highlights(vpath, strategy, score_cb,
                                                    ref_embeddings=ref, neg_embeddings=neg_ref)
                if scores:
                    _save_video_cache(vpath, {
                        "score_ref_sig": _score_ref_sig,
                        "score_neg_sig": _score_neg_sig,
                        "strategy": strategy,
                        "score_version": _SCORE_VERSION,
                        "scores": scores,
                    })
            except Exception as _se:
                logging.warning(f"score failed for {vname}: {_se}")
                q.put({"type": "progress", "phase": "highlight",
                       "percent": 70 + int((vi + 1) / max(len(extracted_files), 1) * 15),
                       "message": f"⚠️ {vname[:40]} 評分失敗，略過"})
                scores = None
        if scores:
            all_scores.append((vpath, scores))

    if _cache_hits > 0:
        q.put({"type": "progress", "phase": "highlight", "percent": 85,
               "message": f"⚡ 評分快取命中 {_cache_hits}/{len(extracted_files)} 部"})

    if not all_scores:
        q.put({"type": "error", "message": "所有擷取影片都無法分析"})
        return

    # 補償 xfade 轉場重疊損失的時間
    # 用 round() 而非 ceil()：target 45s / 6s clip / 0.5s fade 時
    #   ceil((45-0.5)/(6-0.5)) = 9 clips → 50s（超標 5s，末尾常是低質 clip）
    #   round(8.09) = 8 clips → 44.5s（貼近目標）
    import math
    if transition in ("crossfade", "fade") and transition_dur > 0 and clip_duration > transition_dur:
        _exact_clips = (total_duration - transition_dur) / (clip_duration - transition_dur)
        needed_clips = max(1, round(_exact_clips))
        adjusted_total = needed_clips * clip_duration
    else:
        adjusted_total = total_duration
    # 選多一些候選片段供預覽模式替換用
    _extra_factor = 3 if preview else 2.5
    # 限制單一影片的片段數，確保來源多樣性
    _needed_clips = max(1, int(adjusted_total / clip_duration))
    _src_count = len(all_scores)
    if _src_count <= 1:
        max_per_video = _needed_clips  # 只有一個來源時不限制
    elif _src_count >= _needed_clips:
        # 來源數夠：每部影片只取 1 段，避免「同一支影片被拆成兩段拼」造成重複感
        max_per_video = 1
    elif _src_count * 2 >= _needed_clips:
        max_per_video = 2
    else:
        # 來源嚴重不足：放寬到 3 段才能填滿目標長度
        max_per_video = max(2, math.ceil(_needed_clips / max(_src_count, 1)))
    _want_vertical = resolution.endswith("_v")
    # 嚴格限制每部影片片段數（主選 + 替補都用同一個上限）
    all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                              adjusted_total * _extra_factor,
                                              max_per_video,
                                              person=sanitize_name(person),
                                              prefer_vertical=_want_vertical)
    # 兩階段選擇：第一輪 max_per_video=1 結果不足時，放寬到 2 重跑補齊
    # 目的：source 多樣性優先；若 source 實際可用數 < needed，再允許同 source 多段
    _needed_for_target = needed_clips if (transition in ("crossfade", "fade") and transition_dur > 0
                                           and clip_duration > transition_dur) else math.ceil(adjusted_total / clip_duration)
    if len(all_candidates) < _needed_for_target and max_per_video == 1:
        logging.info(f"first pass got only {len(all_candidates)} candidates for {_needed_for_target} "
                     f"needed, retrying with max_per_video=2")
        all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                                  adjusted_total * _extra_factor,
                                                  2,
                                                  person=sanitize_name(person),
                                                  prefer_vertical=_want_vertical)
    if len(all_candidates) < _needed_for_target and max_per_video <= 2:
        logging.info(f"second pass got only {len(all_candidates)} candidates for {_needed_for_target} "
                     f"needed, retrying with max_per_video=3")
        all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                                  adjusted_total * _extra_factor,
                                                  3,
                                                  person=sanitize_name(person),
                                                  prefer_vertical=_want_vertical)
    # 分成 selected 和 alternates（用 needed_clips 保持與上面計算一致）
    needed = needed_clips if (transition in ("crossfade", "fade") and transition_dur > 0
                              and clip_duration > transition_dur) else math.ceil(adjusted_total / clip_duration)
    clips = all_candidates[:needed]
    alternates = all_candidates[needed:]

    if not clips:
        q.put({"type": "error", "message": "找不到符合條件的片段"})
        return

    # ── 預覽模式：暫停讓使用者挑選 ──
    if preview and confirm_event:
        def _clip_to_info(c, idx):
            vpath = c["video"]
            rel = os.path.relpath(vpath, YT_ROOT).replace("\\", "/")
            return {
                "idx": idx,
                "video_url": f"/yt-files/{rel}",
                "time": float(round(c["time"], 1)),
                "score": float(round(c["score"], 2)),
                "duration": float(round(c.get("src_duration", 0), 1)),
                "filename": os.path.basename(vpath),
                "is_vertical": bool(c.get("is_vertical", False)),
            }

        clips_info = [_clip_to_info(c, i) for i, c in enumerate(clips)]
        n_clips = len(clips)
        alts_info = [_clip_to_info(c, n_clips + i) for i, c in enumerate(alternates)]

        # ── Confidence-based auto-confirm ──
        # 若所有主選片段的分數都夠高、來源夠分散，就不等使用者，直接合成
        CONFIDENCE_THRESHOLD = 0.75  # 平均分數門檻
        MIN_SCORE_THRESHOLD = 0.55   # 任一片段最低分數門檻
        try:
            scores_list = [c.get("score", 0) for c in clips]
            avg_score = sum(scores_list) / max(len(scores_list), 1)
            min_score = min(scores_list) if scores_list else 0
            n_sources = len(set(c["video"] for c in clips))
            source_diversity_ok = (n_sources >= max(2, len(clips) // 2))
            auto_confirm = (
                avg_score >= CONFIDENCE_THRESHOLD
                and min_score >= MIN_SCORE_THRESHOLD
                and source_diversity_ok
                and len(clips) > 0
            )
        except Exception:
            auto_confirm = False

        if auto_confirm:
            q.put({"type": "progress", "phase": "highlight", "percent": 86,
                   "message": f"✨ 信心分數足夠（avg={avg_score:.2f}），自動確認 {len(clips)} 片段"})
            logging.info(f"Auto-confirm: avg={avg_score:.2f}, min={min_score:.2f}, "
                         f"sources={n_sources}, clips={len(clips)}")
            # 跳過等待，直接往下走
        else:
            q.put({
                "type": "clips_ready",
                "clips": clips_info,
                "alternates": alts_info,
                "clip_duration": clip_duration,
                "person": sanitize_name(person),
            })

            # 等待使用者確認（最多 10 分鐘）
            logging.info(f"Preview mode: waiting for user confirmation ({len(clips)} clips, {len(alternates)} alts)")
            confirmed = confirm_event.wait(timeout=600)
            if not confirmed or confirm_data[0] is None:
                q.put({"type": "error", "message": "預覽逾時或已取消"})
                return

            # 使用者確認的片段索引
            final_indices = confirm_data[0]  # list of indices into all_candidates
            clips = [all_candidates[i] for i in final_indices if i < len(all_candidates)]
            if not clips:
                q.put({"type": "error", "message": "沒有選取任何片段"})
                return

        q.put({"type": "progress", "phase": "highlight", "percent": 87,
               "message": f"✂️ 確認 {len(clips)} 個片段，開始合成..."})

    if transition in ("crossfade", "fade") and transition_dur > 0 and len(clips) > 1:
        actual_dur = len(clips) * clip_duration - (len(clips) - 1) * transition_dur
    else:
        actual_dur = len(clips) * clip_duration

    if not preview:
        q.put({"type": "progress", "phase": "highlight", "percent": 87,
               "message": f"✂️ 選出 {len(clips)} 個片段（{actual_dur:.0f}秒），合成中..."})

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _vtype_label = video_type if video_type and video_type != "all" else ""
    out_name = f"auto_{sanitize_name(person)}_{ts}.mp4"
    if _vtype_label:
        output_dir = os.path.join(YT_ROOT, "output", sanitize_name(person), _vtype_label)
    else:
        output_dir = os.path.join(YT_ROOT, "output", sanitize_name(person))
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, out_name)

    def compile_cb(pct):
        p = 87 + int(pct * 12)
        q.put({"type": "progress", "phase": "highlight", "percent": min(p, 99),
               "message": f"合成影片 {int(pct*100)}%"})

    # 合成：若失敗，嘗試用替補片段替換 bad clip 後重試
    result = None
    _compile_attempts = 0
    _max_attempts = 3
    _clips_now = list(clips)
    _alt_queue = list(alternates) if isinstance(alternates, list) else []
    while _compile_attempts < _max_attempts and _clips_now:
        try:
            result = _compile_highlight(
                _clips_now, clip_duration, out_path, transition,
                transition_dur, resolution, compile_cb,
                audio_mode=audio_mode,
                loudnorm_params=loudnorm_params,
                name_tag=person,
            )
        except Exception as _ce:
            logging.warning(f"compile attempt {_compile_attempts+1} failed: {_ce}")
            result = None
        if result and os.path.isfile(result):
            break
        _compile_attempts += 1
        # 移除最後一個（最可能是問題片段），補一個替補
        if _alt_queue:
            dropped = _clips_now.pop()
            _clips_now.append(_alt_queue.pop(0))
            q.put({"type": "progress", "phase": "highlight", "percent": 90,
                   "message": f"⚠️ 合成失敗，替換片段後重試 ({_compile_attempts}/{_max_attempts})"})
        else:
            # 無替補：嘗試用較少片段合成
            if len(_clips_now) > 1:
                _clips_now = _clips_now[:-1]
                q.put({"type": "progress", "phase": "highlight", "percent": 90,
                       "message": f"⚠️ 合成失敗，改用 {len(_clips_now)} 片段重試"})
            else:
                break
    if result and os.path.isfile(result):
        clips = _clips_now  # 記錄實際使用的片段
        # 記錄已使用的片段，下次優先選新片段
        _save_clip_history(sanitize_name(person), clips)
        # 產生封面圖
        try:
            cover_path = _pick_cover_frame(clips, result)
        except Exception as _cfe:
            logging.warning(f"cover selection failed: {_cfe}")
            cover_path = None
        rel_path = os.path.relpath(result, YT_ROOT).replace("\\", "/")
        cover_rel = os.path.relpath(cover_path, YT_ROOT).replace("\\", "/") if cover_path else None
        fsize = os.path.getsize(result)
        _cap = _synthesize_caption_from_clips(person, clips)
        done_data = {
            "type": "done",
            "filename": out_name,
            "rel_path": rel_path,
            "file_url": f"/yt-files/{rel_path}",
            "file_size_mb": round(fsize / 1024 / 1024, 1),
            "clips_count": len(clips),
            "duration": round(actual_dur, 1),
            "downloaded": len(downloaded_files),
            "extracted": len(extracted_files),
            "caption_text": _cap["text"],
            "hashtags": _cap["hashtags"],
            "caption_sources": _cap.get("sources", 0),
            "cover_url": f"/yt-files/{cover_rel}" if cover_rel else None,
        }
        q.put(done_data)
        if task_id:
            _av_save_history(task_id, "done", task_label or person, done_data)
    else:
        q.put({"type": "error", "message": "FFmpeg 合成失敗"})
        if task_id:
            _av_save_history(task_id, "error", task_label or person, {"message": "FFmpeg 合成失敗"})

    # ── 不刪除任何影片：保留原始下載和擷取結果 ──
    # 原始影片保留在 downloads/ 供去重判斷（避免下次重複下載同一部）
    # 擷取結果保留在 extracts/ 供快取使用


@app.route("/api/auto-video/progress/<task_id>")
def api_auto_video_progress(task_id):
    """一鍵影片生成 SSE 進度"""
    def generate():
        # 先檢查是否已完成（重連情境）
        h = auto_video_history.get(task_id)
        if h and h["status"] in ("done", "error"):
            evt = dict(h.get("data", {}))
            evt["type"] = h["status"]
            if h["status"] == "error" and "message" not in evt:
                evt["message"] = "任務失敗"
            yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
            return

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
                # clips_ready 不結束 SSE，worker 還在等確認
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        # 任務結束後才清理 active task
        with auto_video_tasks_lock:
            auto_video_tasks.pop(task_id, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/auto-video/confirm/<task_id>", methods=["POST"])
def api_auto_video_confirm(task_id):
    """預覽模式：使用者確認片段後觸發合成"""
    with auto_video_tasks_lock:
        task = auto_video_tasks.get(task_id)
    if not task:
        return jsonify({"error": "任務不存在或已逾時"}), 404

    confirm_event = task.get("confirm_event")
    confirm_data = task.get("confirm_data")
    if not confirm_event or not confirm_data:
        return jsonify({"error": "此任務不是預覽模式"}), 400

    data = request.get_json(force=True)
    indices = data.get("indices", [])  # 最終選取的片段索引
    confirm_data[0] = indices
    confirm_event.set()
    return jsonify({"ok": True})


@app.route("/api/auto-video/blacklist", methods=["POST"])
def api_auto_video_blacklist():
    """將片段加入黑名單（下次生成時不再出現）"""
    data = request.get_json(force=True)
    person = (data.get("person") or "").strip()
    video = (data.get("video") or "").strip()
    time_sec = data.get("time", 0)
    if not person or not video:
        return jsonify({"error": "需要 person 和 video"}), 400
    _add_to_clip_blacklist(sanitize_name(person), video, float(time_sec))
    return jsonify({"ok": True})


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
<thead><tr><th>名稱</th><th>數量</th><th>最後下載</th><th style="width:50px"></th></tr></thead>
<tbody id="celeb-list"></tbody>
</table>
<div class="total-bar" id="total-bar"></div>
</div>

<!-- 影片 → 照片擷取 -->
<div class="card">
<h3>🎥 從影片擷取照片</h3>
<p style="font-size:.82em;color:var(--txt2);margin-bottom:10px">
  上傳影片，自動每隔幾秒擷取一張畫面存為照片
</p>
<div class="row">
  <label>人物名稱</label>
  <input type="text" id="vp-person" placeholder="例: ahyeon, karina">
</div>
<div class="row">
  <label>擷取間隔</label>
  <select id="vp-interval">
    <option value="0.5">每 0.5 秒</option>
    <option value="1">每 1 秒</option>
    <option value="2" selected>每 2 秒</option>
    <option value="3">每 3 秒</option>
    <option value="5">每 5 秒</option>
  </select>
</div>
<div class="row">
  <label>選擇影片</label>
  <input type="file" id="vp-file" accept="video/*" style="font-size:.88em">
</div>
<div class="row" style="margin-top:10px">
  <button class="btn btn-pri" onclick="vpStart()">📤 上傳並擷取</button>
</div>
<div id="vp-progress" style="display:none;margin-top:10px">
  <div class="progress-wrap"><div class="progress-bar" id="vp-bar" style="width:0%"></div>
  <span class="progress-text" id="vp-pct">0%</span></div>
  <div id="vp-msg" style="font-size:.85em;color:var(--txt2);margin-top:6px"></div>
</div>
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
    <option value="dance">舞蹈</option>
    <option value="music_show">音樂節目</option>
    <option value="mv">MV</option>
    <option value="cute">可愛日常</option>
    <option value="vlog">Vlog</option>
    <option value="challenge">挑戰</option>
  </select>
  <label style="margin-left:16px">搜尋數量</label>
  <select id="yt-max-results">
    <option value="5">5</option>
    <option value="10" selected>10</option>
    <option value="20">20</option>
    <option value="30">30</option>
    <option value="50">50</option>
    <option value="100">100</option>
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
<div id="yt-search-progress" style="display:none;margin-top:6px">
  <div class="progress-wrap" style="height:18px">
    <div class="progress-bar" id="yt-search-bar" style="width:0%"></div>
    <span class="progress-text" id="yt-search-pct" style="font-size:.75em">0%</span>
  </div>
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
  <button onclick="ytBatchAction('extract')">⬇ 批量下載+擷取</button>
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
      <option value="6">6 秒</option>
      <option value="8">8 秒</option>
      <option value="10">10 秒</option>
      <option value="15">15 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">總長度</label>
    <select id="hl-total-dur">
      <option value="15">15 秒</option>
      <option value="30" selected>30 秒</option>
      <option value="45">45 秒</option>
      <option value="60">1 分鐘</option>
      <option value="90">1.5 分鐘</option>
      <option value="120">2 分鐘</option>
      <option value="180">3 分鐘</option>
      <option value="300">5 分鐘</option>
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
  輸入人物名稱，自動完成：搜尋 → 下載 → 人臉辨識擷取 → 精華剪輯
</p>
<div class="row">
  <label>人物名稱</label>
  <select id="av-group" onchange="avGroupChanged()" style="flex:0 0 140px;margin-right:6px">
    <option value="">-- 選團體 --</option>
  </select>
  <select id="av-person" style="flex:1"
          onkeydown="if(event.key==='Enter')autoVideoStart()">
    <option value="">-- 選成員 --</option>
  </select>
</div>
<div class="row">
  <label>額外關鍵字</label>
  <input type="text" id="av-keyword" placeholder="(選填) 額外關鍵字，例如：ice cream、drip、forever">
</div>
<div class="row" style="background:rgba(100,150,255,0.08);padding:8px 10px;border-radius:6px;margin-bottom:8px;align-items:center;gap:12px;flex-wrap:wrap">
  <label style="font-size:.88em;font-weight:600;display:flex;align-items:center;gap:6px;cursor:pointer;margin:0">
    <input type="checkbox" id="av-auto-mode" onchange="avAutoModeChanged()">
    ✨ 智能模式（自動選參數）
  </label>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">輸出平台</label>
    <select id="av-output-preset">
      <option value="">自訂</option>
      <option value="tiktok">TikTok (1080x1920, 30s)</option>
      <option value="yt_shorts">YT Shorts (1080x1920, 45s)</option>
      <option value="ig_reels">IG Reels (1080x1920, 30s)</option>
      <option value="youtube">YouTube 橫式 (1920x1080, 60s)</option>
    </select>
  </div>
  <span style="font-size:.78em;color:var(--txt2)">勾選後參數由系統決定</span>
</div>
<div class="row" id="av-detail-options" style="flex-wrap:wrap;gap:8px;align-items:end">
  <div>
    <label style="font-size:.8em;color:var(--txt2)">來源</label>
    <select id="av-source" onchange="avSourceChanged()">
      <option value="online" selected>線上搜尋</option>
      <option value="local">本地影片庫</option>
    </select>
  </div>
  <div id="av-platform-wrap">
    <label style="font-size:.8em;color:var(--txt2)">平台</label>
    <select id="av-platform">
      <option value="all">全部（跨平台合併）</option>
      <option value="tiktok" selected>TikTok</option>
      <option value="youtube">YouTube</option>
      <option value="yt_shorts">YouTube Shorts</option>
      <option value="ig_reels">Instagram Reels</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">影片類型</label>
    <select id="av-video-type">
      <option value="all" selected>全部</option>
      <option value="dance">舞蹈</option>
      <option value="fancam">直拍</option>
      <option value="stage">舞台</option>
      <option value="cute">可愛</option>
      <option value="vlog">Vlog</option>
      <option value="mv">MV</option>
      <option value="challenge">挑戰</option>
    </select>
  </div>
  <div id="av-max-videos-wrap">
    <label style="font-size:.8em;color:var(--txt2)">下載數量</label>
    <select id="av-max-videos">
      <option value="5">5 部</option>
      <option value="10" selected>10 部</option>
      <option value="20">20 部</option>
      <option value="30">30 部</option>
      <option value="50">50 部</option>
      <option value="100">100 部</option>
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
      <option value="6">6 秒</option>
      <option value="8">8 秒</option>
      <option value="10">10 秒</option>
      <option value="15">15 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">總長度</label>
    <select id="av-total-dur">
      <option value="15">15 秒</option>
      <option value="30" selected>30 秒</option>
      <option value="45">45 秒</option>
      <option value="60">1 分鐘</option>
      <option value="90">1.5 分鐘</option>
      <option value="120">2 分鐘</option>
      <option value="180">3 分鐘</option>
      <option value="300">5 分鐘</option>
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
<div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap">
  <button class="btn btn-pri" id="btn-auto-video" onclick="autoVideoStart(false)"
          style="font-size:1em;padding:10px 32px">🚀 一鍵生成影片</button>
  <button class="btn btn-sec" onclick="autoVideoStart(true)"
          style="font-size:1em;padding:10px 24px">🎞️ 先挑選再合成</button>
</div>
</div>

<div id="av-tasks-container"></div>

</div><!-- /tab-autovid -->

<!-- 圖庫彈窗 -->
<div class="modal" id="gallery-modal">
<div class="modal-box">
<div class="modal-head"><h3 id="gallery-title">圖庫</h3><span id="gallery-count" style="font-size:.82em;color:var(--txt2);margin-left:8px"></span><button class="modal-close" onclick="closeModal('gallery-modal')">&times;</button></div>
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

<!-- 片段瀏覽器 -->
<div class="modal" id="clip-browser-modal">
<div class="modal-box" style="max-width:900px">
<div class="modal-head">
  <h3 id="cb-title">🎞️ 挑選片段</h3>
  <span id="cb-count" style="font-size:.82em;color:var(--txt2);margin-left:8px"></span>
  <button class="modal-close" onclick="closeModal('clip-browser-modal')">&times;</button>
</div>
<div class="modal-body">
  <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center">
    <select id="cb-type-filter" onchange="cbFilterClips()" style="font-size:.88em">
      <option value="all">全部類型</option>
    </select>
    <span id="cb-selected-count" style="font-size:.85em;color:var(--pri);font-weight:600"></span>
    <span style="flex:1"></span>
    <button class="btn" onclick="cbSelectAll()" style="font-size:.82em;padding:4px 10px">全選</button>
    <button class="btn" onclick="cbDeselectAll()" style="font-size:.82em;padding:4px 10px">全不選</button>
  </div>
  <div id="cb-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;max-height:55vh;overflow-y:auto"></div>
  <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
    <div>
      <label style="font-size:.8em;color:var(--txt2)">每段秒數</label>
      <select id="cb-clip-dur" style="font-size:.88em">
        <option value="2">2 秒</option>
        <option value="3" selected>3 秒</option>
        <option value="4">4 秒</option>
        <option value="5">5 秒</option>
        <option value="8">8 秒</option>
        <option value="10">10 秒</option>
      </select>
    </div>
    <div>
      <label style="font-size:.8em;color:var(--txt2)">過場</label>
      <select id="cb-transition" style="font-size:.88em">
        <option value="crossfade" selected>淡入淡出</option>
        <option value="cut">直切</option>
      </select>
    </div>
    <div>
      <label style="font-size:.8em;color:var(--txt2)">解析度</label>
      <select id="cb-resolution" style="font-size:.88em">
        <option value="720" selected>720p</option>
        <option value="1080">1080p</option>
      </select>
    </div>
    <div>
      <label style="font-size:.8em;color:var(--txt2)">音訊</label>
      <select id="cb-audio" style="font-size:.88em">
        <option value="original" selected>保留原音</option>
        <option value="mute">靜音</option>
      </select>
    </div>
    <span style="flex:1"></span>
    <button class="btn btn-pri" onclick="cbCompile()" style="font-size:.95em;padding:8px 24px">
      🎬 合成選取的片段
    </button>
  </div>
</div>
</div>
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
      const esc = c.name.replace(/'/g,"\\'");
      tr.innerHTML = '<td class="cel-name" onclick="openGallery(\'' + esc + '\')">'
        + c.name + '</td><td>' + c.count + '</td><td>' + c.last + '</td>'
        + '<td><button onclick="deletePhotoFolder(\'' + esc + '\',this)" '
        + 'style="background:none;border:none;cursor:pointer;font-size:1em;opacity:.5;padding:2px 6px" '
        + 'title="刪除整個圖庫" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=.5">🗑️</button></td>';
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

let _galleryCurrentName = '';
async function openGallery(name) {
  _galleryCurrentName = name;
  document.getElementById('gallery-title').textContent = name + ' — 圖庫';
  const grid = document.getElementById('gallery-grid');
  grid.innerHTML = '<p style="color:var(--txt2)">載入中...</p>';
  document.getElementById('gallery-modal').classList.add('show');
  try {
    const r = await fetch('/api/images/' + encodeURIComponent(name));
    const imgs = await r.json();
    grid.innerHTML = '';
    document.getElementById('gallery-count').textContent = imgs.length + ' 張';
    if (!imgs.length) { grid.innerHTML = '<p style="color:var(--txt2)">尚無照片</p>'; return; }
    imgs.forEach(img => {
      const div = document.createElement('div');
      div.style.position = 'relative';
      div.innerHTML = '<img src="' + img.url + '" loading="lazy" onclick="showLight(this.src)">'
        + '<button onclick="deletePhoto(\'' + name.replace(/'/g,"\\'") + '\',\'' + img.filename.replace(/'/g,"\\'") + '\',this)" '
        + 'style="position:absolute;top:4px;right:4px;background:rgba(0,0,0,.55);color:#fff;border:none;'
        + 'border-radius:50%;width:24px;height:24px;cursor:pointer;font-size:.8em;display:flex;align-items:center;'
        + 'justify-content:center;opacity:0;transition:opacity .2s" '
        + 'onmouseover="this.style.opacity=1" title="刪除">&times;</button>'
        + '<div class="gallery-info">' + img.filename + ' (' + img.size_kb + 'KB)</div>';
      div.onmouseover = function(){ this.querySelector('button').style.opacity=1; };
      div.onmouseout = function(){ this.querySelector('button').style.opacity=0; };
      grid.appendChild(div);
    });
  } catch(e) { grid.innerHTML = '<p style="color:var(--err)">載入失敗</p>'; }
}

function showLight(src) {
  document.getElementById('lightbox-img').src = src;
  document.getElementById('lightbox').classList.add('show');
}

async function deletePhoto(celebrity, filename, btn){
  const div = btn.closest('div');
  try{
    btn.textContent = '...';
    const r = await fetch('/api/photos/delete',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity, filename})
    });
    const d = await r.json();
    if(d.ok){
      div.style.transition = 'opacity .3s, transform .3s';
      div.style.opacity = '0';
      div.style.transform = 'scale(.8)';
      setTimeout(()=>div.remove(), 300);
      document.getElementById('gallery-count').textContent = d.remaining + ' 張';
      loadCelebs();
    }else{
      alert('刪除失敗: '+(d.error||''));
    }
  }catch(e){ alert('連線失敗: '+e); }
}

async function deletePhotoFolder(name, btn){
  if(!confirm('確定要刪除「'+name+'」的所有照片嗎？此操作無法復原。')) return;
  const tr = btn.closest('tr');
  try{
    btn.textContent = '...';
    btn.disabled = true;
    const r = await fetch('/api/photos/delete-folder',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({celebrity: name})
    });
    const d = await r.json();
    if(d.ok){
      tr.style.transition = 'opacity .3s';
      tr.style.opacity = '0';
      setTimeout(()=>{ tr.remove(); loadCelebs(); }, 300);
    }else{
      alert('刪除失敗: '+(d.error||''));
      btn.textContent = '🗑️';
      btn.disabled = false;
    }
  }catch(e){
    alert('連線失敗: '+e);
    btn.textContent = '🗑️';
    btn.disabled = false;
  }
}

function showBatch() { document.getElementById('batch-modal').classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }

// 初始化
loadCelebs();
fetch('/api/stats').then(r=>r.json()).then(d=>{
  if(!d.has_imagehash) document.getElementById('chk-phash').disabled=true;
});

// 恢復進行中的任務
(async function _avRestoreTasks(){
  try{
    const r = await fetch('/api/auto-video/tasks');
    const tasks = await r.json();
    tasks.forEach(t=>{
      if(!t.label) return;
      const tc = _avCreateTaskCard(t.label);
      document.getElementById('av-tasks-container').prepend(tc.card);
      if(t.status === 'done' && t.data && t.data.file_url){
        // 已完成 — 直接顯示結果
        tc.bar.style.width = '100%';
        tc.pct.textContent = '100%';
        tc.title.textContent = '🎉 完成！ — '+t.label;
        tc.title.style.color = 'var(--ok)';
        const d = t.data;
        tc.msg.innerHTML = '✅ <a href="'+d.file_url+'" target="_blank" style="color:var(--pri);font-weight:600">'
          +d.filename+'</a> ('+d.file_size_mb+'MB)';
        tc.result.style.display = '';
        tc.result.innerHTML =
          '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:.85em;margin-bottom:8px">'
          +'<span>✂️ <b>'+d.clips_count+'</b> 個片段</span>'
          +'<span>⏱️ <b>'+d.duration+'</b> 秒</span>'
          +'<span>📦 <b>'+d.file_size_mb+'</b> MB</span></div>'
          +'<video src="'+d.file_url+'" controls style="max-width:100%;max-height:400px;border-radius:12px"></video>'
          + _avRenderCaption(d)
          +'<div style="display:flex;gap:8px;margin-top:8px">'
          +'<a href="'+d.file_url+'" download class="btn btn-pri" style="font-size:.85em">⬇️ 下載影片</a>'
          +'<button class="btn" style="font-size:.85em;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5" '
          +'onclick="_avDeleteOutput(this,\''+d.rel_path+'\')">🗑️ 刪除影片</button>'
          +'<button class="btn btn-sec" style="font-size:.85em" '
          +'onclick="document.getElementById(\'tab-autovid\').scrollIntoView({behavior:\'smooth\'})">⬆️ 回到設定</button></div>';
      }else if(t.status === 'error'){
        tc.title.textContent = '❌ 失敗 — '+t.label;
        tc.title.style.color = 'var(--err)';
        tc.msg.textContent = '❌ '+(t.data.message||'');
        tc.msg.style.color = 'var(--err)';
      }else{
        // 進行中 — 重新連接 SSE
        tc.msg.textContent = '🔄 重新連線中...';
        _avListenSSE(t.task_id, tc, t.label);
      }
    });
  }catch(e){}
})();

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
  const inp = document.getElementById('yt-query');
  if(p === 'tiktok'){
    inp.placeholder = '輸入關鍵字搜尋、貼上 TikTok 網址、或 @用戶名';
  } else {
    inp.placeholder = '輸入關鍵字或貼上 YouTube / TikTok 網址';
  }
}

let _ytSearchES = null;
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
  const progWrap = document.getElementById('yt-search-progress');
  const progBar = document.getElementById('yt-search-bar');
  const progPct = document.getElementById('yt-search-pct');
  btn.disabled = true;
  status.textContent = '搜尋中...';
  progWrap.style.display = '';
  progBar.style.width = '0%';
  progPct.textContent = '0%';
  document.getElementById('yt-results-card').style.display = 'none';

  try{
    const r = await fetch('/api/yt/search-sse',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({query, max_results:maxResults, video_type:videoType, platform})
    });
    const d = await r.json();
    if(d.error){ status.textContent = '錯誤: '+d.error; progWrap.style.display='none'; btn.disabled=false; return; }
    // 開始 SSE 監聽
    if(_ytSearchES) _ytSearchES.close();
    _ytSearchES = new EventSource('/api/yt/search-progress/'+d.task_id);
    _ytSearchES.onmessage = (e)=>{
      const ev = JSON.parse(e.data);
      switch(ev.type){
        case 'progress':
          const pct = Math.round(ev.percent||0);
          progBar.style.width = pct+'%';
          progPct.textContent = pct+'%';
          status.textContent = ev.message||'搜尋中...';
          break;
        case 'done':
          _ytSearchES.close(); _ytSearchES=null;
          btn.disabled=false;
          progBar.style.width = '100%';
          progPct.textContent = '100%';
          const results = ev.results||[];
          let msg = '找到 '+results.length+' 個結果';
          if(ev.hint) msg += ' — '+ev.hint;
          status.textContent = msg;
          setTimeout(()=>{ progWrap.style.display='none'; }, 1500);
          renderYtResults(results);
          break;
        case 'error':
          _ytSearchES.close(); _ytSearchES=null;
          btn.disabled=false;
          progWrap.style.display='none';
          status.textContent = '錯誤: '+(ev.message||'未知錯誤');
          status.style.color = 'var(--err)';
          break;
        case 'heartbeat': break;
      }
    };
    _ytSearchES.onerror = ()=>{
      _ytSearchES.close(); _ytSearchES=null;
      btn.disabled=false;
      progWrap.style.display='none';
      status.textContent = '搜尋連線中斷';
    };
  }catch(e){
    status.textContent = '搜尋失敗: '+e;
    progWrap.style.display='none';
    btn.disabled=false;
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
    if(v.downloaded) div.style.opacity = '0.7';
    const durStr = _fmtDuration(v.duration);
    const viewStr = _fmtViews(v.view_count);
    const safeTitle = (v.title||'').replace(/</g,'&lt;');
    // 狀態標籤
    let badges = '';
    if(v.extracted) badges += '<span style="font-size:.7em;background:#10b981;color:#fff;padding:1px 6px;border-radius:8px;margin-left:6px">已擷取</span>';
    else if(v.downloaded) badges += '<span style="font-size:.7em;background:#6366f1;color:#fff;padding:1px 6px;border-radius:8px;margin-left:6px">已下載</span>';
    div.innerHTML = '<input type="checkbox" class="yt-item-cb" data-cb-idx="'+idx+'" onchange="ytUpdateBatch()">'
      +'<img src="'+(v.thumbnail||'')+'" alt="" onerror="this.style.display=\'none\'">'
      +'<div class="yt-item-info">'
      +'<div class="yt-item-title">'+safeTitle+badges+'</div>'
      +'<div class="yt-item-meta">'+(v.channel||'')+' · '+(durStr?durStr+' · ':'')+viewStr+' 次觀看</div>'
      +'<div class="yt-item-actions">'
      +'<button class="btn btn-pri" style="font-size:.8em;padding:4px 12px" data-yt-extract-idx="'+idx+'">'+(v.extracted?'🎯 重新擷取':v.downloaded?'🎯 擷取':'⬇ 下載+擷取')+'</button>'
      +'<a href="'+(v.url||'')+'" target="_blank" style="font-size:.8em;color:var(--pri);text-decoration:none;padding:4px 8px">▶ 開啟</a>'
      +'</div></div>';
    container.appendChild(div);
  });
  // Event delegation — all downloads now include extraction
  container.onclick = (e)=>{
    if(e.target.classList.contains('yt-item-cb')) return;
    const btn = e.target.closest('[data-yt-extract-idx]');
    if(btn){
      const idx = parseInt(btn.dataset.ytExtractIdx);
      const v = _ytResults[idx];
      if(!v) return;
      const quality = document.getElementById('yt-quality').value;
      const keyword = document.getElementById('yt-query').value.trim();
      const videoType = document.getElementById('yt-video-type').value;
      if(!keyword){ alert('搜尋關鍵字將作為擷取人物名稱，請確認搜尋欄有輸入'); return; }
      ytDownloadAndExtract(v.url, v.title, quality, keyword, videoType);
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
  mode = 'extract';  // 一律下載+擷取
  const checked = document.querySelectorAll('.yt-item-cb:checked');
  if(!checked.length) return;
  const keyword = document.getElementById('yt-query').value.trim();
  if(!keyword){
    alert('搜尋關鍵字將作為擷取人物名稱，請確認搜尋欄有輸入');
    return;
  }
  const quality = document.getElementById('yt-quality').value;
  const videoType = document.getElementById('yt-video-type').value;
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

  _ytBatchNext(quality, keyword, videoType);
}

function _ytBatchNext(quality, keyword, videoType){
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
    _ytBatchDownloadAndExtract(v, quality, keyword, videoType);
  } else {
    _ytBatchDownloadOne(v, quality, keyword, videoType);
  }
}

async function _ytBatchDownloadOne(v, quality, keyword, videoType){
  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url:v.url, title:v.title, quality, keyword, video_type:videoType})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ '+v.title+': '+d.error;
      _ytBatchDone++;
      _ytBatchUpdateBar();
      _ytBatchNext(quality, keyword, videoType);
      return;
    }
    _ytBatchListenDL(d.task_id, quality, keyword, videoType, false);
  }catch(e){
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword, videoType);
  }
}

function _ytBatchListenDL(taskId, quality, keyword, videoType, chainExtract){
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
          _ytBatchNext(quality, keyword, videoType);
        }
        break;
      case 'error':
        ytES.close(); ytES=null;
        document.getElementById('yt-prog-msg').textContent = '⚠️ 下載失敗：'+d.message;
        _ytBatchDone++;
        _ytBatchUpdateBar();
        _ytBatchNext(quality, keyword, videoType);
        break;
      case 'heartbeat': break;
    }
  };
  ytES.onerror = ()=>{
    ytES.close(); ytES=null;
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword, videoType);
  };
}

async function _ytBatchDownloadAndExtract(v, quality, keyword, videoType){
  try{
    const r = await fetch('/api/yt/download',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url:v.url, title:v.title, quality, keyword, video_type:videoType})
    });
    const d = await r.json();
    if(d.error){
      document.getElementById('yt-prog-msg').textContent = '⚠️ '+v.title+': '+d.error;
      _ytBatchDone++;
      _ytBatchUpdateBar();
      _ytBatchNext(quality, keyword, videoType);
      return;
    }
    _ytBatchListenDL(d.task_id, quality, keyword, videoType, true);
  }catch(e){
    _ytBatchDone++;
    _ytBatchUpdateBar();
    _ytBatchNext(quality, keyword, videoType);
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

async function ytDownload(url, title, quality, keyword, videoType){
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
      body:JSON.stringify({url, title, quality, keyword, video_type:videoType})
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
async function ytDownloadAndExtract(url, title, quality, keyword, videoType){
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
      body:JSON.stringify({url, title, quality, keyword, video_type:videoType})
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

function avSourceChanged(){
  const isLocal = document.getElementById('av-source').value === 'local';
  document.getElementById('av-platform-wrap').style.display = isLocal ? 'none' : '';
  document.getElementById('av-max-videos-wrap').style.display = isLocal ? 'none' : '';
  document.getElementById('av-keyword').parentElement.style.display = isLocal ? 'none' : '';
}

function _avRenderCaption(d){
  const txt = (d && d.caption_text) || '';
  if(!txt) return '';
  const safe = txt.replace(/"/g,'&quot;').replace(/</g,'&lt;');
  return '<div style="margin-top:10px;padding:10px 12px;background:#f3f4f6;border-radius:8px;border:1px solid #e5e7eb">'
    +'<div style="font-size:.76em;color:var(--txt2);margin-bottom:4px">📝 文案（點擊複製）</div>'
    +'<div style="display:flex;align-items:center;gap:8px">'
    +'<code id="cap-'+(d.filename||'x')+'" style="flex:1;font-size:.95em;color:#111;word-break:break-all">'+safe+'</code>'
    +'<button class="btn" style="font-size:.78em;padding:4px 10px" '
    +'onclick="navigator.clipboard.writeText(this.previousElementSibling.textContent);this.textContent=\'✓ 已複製\';setTimeout(()=>this.textContent=\'📋 複製\',1500)">📋 複製</button>'
    +'</div></div>';
}

function avAutoModeChanged(){
  const on = document.getElementById('av-auto-mode').checked;
  const det = document.getElementById('av-detail-options');
  if(det) det.style.opacity = on ? '0.45' : '1';
  if(det) det.style.pointerEvents = on ? 'none' : '';
}

const AV_GROUPS = {
  'Fromis_9':    ['HaYoung'],
  'IVE':         ['WonYoung', 'AnYuJin', 'Leeseo', 'Liz'],
  'Babymonster': ['Ahyeon'],
  'IU':          ['IU'],
  'QWER':        ['chodan'],
  'Illit':       ['WonHee'],
  'H2H':         ['Ian', 'jiwoo'],
  'aespa':       ['karina', 'winter'],
  'ITZY':        ['Yuna'],
};

function avGroupChanged(){
  const g = document.getElementById('av-group').value;
  const sel = document.getElementById('av-person');
  sel.innerHTML = '<option value="">-- 選成員 --</option>';
  if(!g || !AV_GROUPS[g]) return;
  for(const m of AV_GROUPS[g]){
    const opt = document.createElement('option');
    opt.value = m; opt.textContent = m;
    sel.appendChild(opt);
  }
  if(AV_GROUPS[g].length === 1){ sel.value = AV_GROUPS[g][0]; }
}

(function avInitGroupOptions(){
  const sel = document.getElementById('av-group');
  if(!sel) return;
  for(const g of Object.keys(AV_GROUPS)){
    const opt = document.createElement('option');
    opt.value = g; opt.textContent = g;
    sel.appendChild(opt);
  }
})();

async function autoVideoStart(preview){
  const person = document.getElementById('av-person').value.trim();
  if(!person) return alert('請輸入人物名稱');

  const source = document.getElementById('av-source').value;
  const keyword = document.getElementById('av-keyword').value.trim();
  const autoMode = document.getElementById('av-auto-mode').checked;
  const outputPreset = document.getElementById('av-output-preset').value;
  const opts = {
    person: person,
    source: source,
    search_keyword: keyword,
    platform: document.getElementById('av-platform').value,
    max_videos: parseInt(document.getElementById('av-max-videos').value),
    strategy: document.getElementById('av-strategy').value,
    clip_duration: parseFloat(document.getElementById('av-clip-dur').value),
    total_duration: parseFloat(document.getElementById('av-total-dur').value),
    resolution: _avGetResolution(),
    transition: document.getElementById('av-transition').value,
    transition_dur: 0.5,
    audio_mode: document.getElementById('av-audio').value,
    video_type: document.getElementById('av-video-type').value,
    preview: !!preview,
    auto_mode: autoMode,
    output_preset: outputPreset,
  };

  // 建立任務卡片
  const taskLabel = person + (keyword ? ' ('+keyword+')' : '') + (preview ? ' [挑選模式]' : '');
  const taskCard = _avCreateTaskCard(taskLabel);
  document.getElementById('av-tasks-container').prepend(taskCard.card);
  taskCard.card.scrollIntoView({behavior:'smooth', block:'center'});

  try{
    const r = await fetch('/api/auto-video',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify(opts)
    });
    const d = await r.json();
    if(d.error){
      taskCard.msg.textContent = '❌ '+d.error;
      taskCard.msg.style.color = 'var(--err)';
      taskCard.title.textContent = '❌ '+taskLabel+' — 失敗';
      return;
    }
    _avListenSSE(d.task_id, taskCard, taskLabel);
  }catch(e){
    taskCard.msg.textContent = '連線失敗: '+e;
    taskCard.msg.style.color = 'var(--err)';
  }
}

let _avTaskCounter = 0;
const _avPhaseLabels = {
  search: '🔍 搜尋中',
  download: '📥 下載影片中',
  photos: '📸 下載照片中',
  extract: '🎯 擷取中',
  highlight: '🎬 剪輯中',
};

function _avCreateTaskCard(label){
  _avTaskCounter++;
  const id = 'av-task-'+_avTaskCounter;
  const card = document.createElement('div');
  card.className = 'card';
  card.id = id;
  card.style.cssText = 'margin-bottom:12px;position:relative';
  card.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
    +'<h3 class="av-task-title" style="margin:0">⏳ '+label+'</h3>'
    +'<button class="av-task-close" onclick="this.closest(\'.card\').style.transition=\'opacity .3s,transform .3s\';this.closest(\'.card\').style.opacity=\'0\';this.closest(\'.card\').style.transform=\'translateX(30px)\';setTimeout(()=>this.closest(\'.card\').remove(),300)" '
    +'style="background:none;border:none;font-size:1.3em;cursor:pointer;color:var(--txt2);padding:0 4px;line-height:1" title="關閉">&times;</button></div>'
    +'<div class="progress-wrap"><div class="progress-bar av-task-bar" style="width:0%"></div>'
    +'<span class="progress-text av-task-pct">0%</span></div>'
    +'<div class="av-task-msg" style="font-size:.88em;color:var(--txt2);margin-top:8px">啟動中...</div>'
    +'<div class="av-task-log" style="max-height:150px;overflow-y:auto;font-family:Consolas,monospace;font-size:.75em;line-height:1.7;padding:6px;background:#F9FAFB;border-radius:8px;margin-top:8px"></div>'
    +'<div class="av-task-result" style="display:none;margin-top:10px"></div>';
  return {
    card: card,
    title: card.querySelector('.av-task-title'),
    bar: card.querySelector('.av-task-bar'),
    pct: card.querySelector('.av-task-pct'),
    msg: card.querySelector('.av-task-msg'),
    log: card.querySelector('.av-task-log'),
    result: card.querySelector('.av-task-result'),
  };
}

function _avListenSSE(taskId, tc, taskLabel){
  let lastPhase = '';
  const es = new EventSource('/api/auto-video/progress/'+taskId);
  es.onmessage = (e)=>{
    const d = JSON.parse(e.data);
    switch(d.type){
      case 'progress':
        const pct = Math.round(d.percent||0);
        tc.bar.style.width = pct+'%';
        tc.pct.textContent = pct+'%';
        tc.msg.textContent = d.message||'';
        if(d.phase && d.phase !== lastPhase){
          lastPhase = d.phase;
          tc.title.textContent = (_avPhaseLabels[d.phase]||'⏳ 處理中')+'... — '+taskLabel;
        }
        if(d.message){
          const line = document.createElement('div');
          const ts = new Date().toTimeString().slice(0,8);
          line.style.color = d.message.startsWith('⚠️') ? 'var(--warn)' :
                             d.message.startsWith('❌') ? 'var(--err)' :
                             d.message.startsWith('✅') ? 'var(--ok)' : 'var(--txt2)';
          line.textContent = '['+ts+'] '+d.message;
          tc.log.appendChild(line);
          tc.log.scrollTop = tc.log.scrollHeight;
        }
        break;

      case 'clips_ready':
        tc.bar.style.width = '85%';
        tc.pct.textContent = '85%';
        tc.title.textContent = '🎞️ 挑選片段 — '+taskLabel;
        tc.title.style.color = 'var(--pri)';
        tc.msg.textContent = '選出 '+d.clips.length+' 個片段，按 ✕ 換掉不要的，確認後合成';
        // 儲存資料
        tc._taskId = taskId;
        tc._person = d.person || '';
        tc._clips = d.clips;
        tc._alts = d.alternates || [];
        tc._clipDur = d.clip_duration || 3;
        _avRenderClipPreview(tc);
        break;

      case 'done':
        es.close();
        tc.bar.style.width = '100%';
        tc.pct.textContent = '100%';
        tc.title.textContent = '🎉 完成！ — '+taskLabel;
        tc.title.style.color = 'var(--ok)';
        tc.msg.innerHTML =
          '✅ <a href="'+d.file_url+'" target="_blank" style="color:var(--pri);font-weight:600">'+d.filename+'</a>'
          +' ('+d.file_size_mb+'MB)';
        tc.result.style.display = '';
        tc.result.innerHTML =
          '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:.85em;margin-bottom:8px">'
          +'<span>📥 下載 <b>'+d.downloaded+'</b> 部</span>'
          +'<span>🎯 擷取 <b>'+d.extracted+'</b> 部</span>'
          +'<span>✂️ <b>'+d.clips_count+'</b> 個片段</span>'
          +'<span>⏱️ <b>'+d.duration+'</b> 秒</span>'
          +'<span>📦 <b>'+d.file_size_mb+'</b> MB</span></div>'
          +'<video src="'+d.file_url+'" controls style="max-width:100%;max-height:400px;border-radius:12px"></video>'
          + _avRenderCaption(d)
          +'<div style="display:flex;gap:8px;margin-top:8px">'
          +'<a href="'+d.file_url+'" download class="btn btn-pri" style="font-size:.85em">⬇️ 下載影片</a>'
          +'<button class="btn" style="font-size:.85em;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5" '
          +'onclick="_avDeleteOutput(this,\''+d.rel_path+'\')">🗑️ 刪除影片</button>'
          +'<button class="btn btn-sec" style="font-size:.85em" '
          +'onclick="document.getElementById(\'tab-autovid\').scrollIntoView({behavior:\'smooth\'})">⬆️ 回到設定</button></div>';
        const dline = document.createElement('div');
        dline.style.color = 'var(--ok)';
        dline.style.fontWeight = '600';
        dline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ✅ 完成！'+d.filename;
        tc.log.appendChild(dline);
        break;
      case 'error':
        es.close();
        tc.title.textContent = '❌ 失敗 — '+taskLabel;
        tc.title.style.color = 'var(--err)';
        tc.msg.textContent = '❌ '+d.message;
        tc.msg.style.color = 'var(--err)';
        const eline = document.createElement('div');
        eline.style.color = 'var(--err)';
        eline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ❌ '+d.message;
        tc.log.appendChild(eline);
        break;
      case 'heartbeat': break;
    }
  };
  es.onerror = ()=>{
    es.close();
    tc.title.textContent = '⚠️ 連線中斷 — '+taskLabel;
  };
}

function _avRenderClipPreview(tc){
  tc.result.style.display = '';
  const grid = document.createElement('div');
  grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:8px;margin-bottom:12px';

  tc._clips.forEach((c, i)=>{
    const cell = document.createElement('div');
    const isV = c.is_vertical;
    cell.style.cssText = 'border:2px solid '+(isV?'var(--ok)':'var(--warn)')
      +';border-radius:8px;overflow:hidden;position:relative;background:#000';
    cell.dataset.clipIdx = c.idx;

    const vid = document.createElement('video');
    vid.src = c.video_url + '#t=' + c.time;
    vid.preload = 'metadata';
    vid.muted = true;
    vid.style.cssText = 'width:100%;aspect-ratio:9/16;object-fit:cover;display:block';
    vid.onmouseover = function(){ this.currentTime=c.time; this.play(); };
    vid.onmouseout = function(){ this.pause(); };
    cell.appendChild(vid);

    // 方向標籤
    const oTag = document.createElement('span');
    oTag.style.cssText = 'position:absolute;top:4px;left:4px;background:'+(isV?'rgba(16,185,129,.85)':'rgba(245,158,11,.85)')
      +';color:#fff;font-size:.65em;padding:1px 6px;border-radius:4px;font-weight:600';
    oTag.textContent = isV ? '📱 直' : '🖥️ 橫';
    cell.appendChild(oTag);

    // ✕ 按鈕（點擊顯示操作選單）
    const xBtn = document.createElement('button');
    xBtn.textContent = '✕';
    xBtn.style.cssText = 'position:absolute;top:4px;right:4px;background:rgba(239,68,68,.85);color:#fff;'
      +'border:none;border-radius:50%;width:26px;height:26px;cursor:pointer;font-size:.9em;font-weight:700';
    xBtn.title = '操作選單';
    xBtn.onclick = function(e){
      e.stopPropagation();
      _avShowClipMenu(tc, i, cell);
    };
    cell.appendChild(xBtn);

    // 時間標籤
    const tLabel = document.createElement('span');
    tLabel.style.cssText = 'position:absolute;bottom:4px;left:4px;background:rgba(0,0,0,.6);color:#fff;'
      +'font-size:.7em;padding:1px 6px;border-radius:4px';
    tLabel.textContent = c.time.toFixed(1)+'s / '+c.duration+'s';
    cell.appendChild(tLabel);

    // 來源名
    const fname = document.createElement('div');
    fname.style.cssText = 'padding:3px 5px;font-size:.68em;color:var(--txt2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;background:var(--card)';
    fname.textContent = c.filename;
    cell.appendChild(fname);

    grid.appendChild(cell);
  });

  const btnRow = document.createElement('div');
  btnRow.style.cssText = 'display:flex;gap:10px;align-items:center;margin-top:8px';
  btnRow.innerHTML =
    '<span style="font-size:.85em;color:var(--txt2)">✕ 換段 / 瀏覽備選 / 移除（備選：'+tc._alts.length+' 段，已選：'+tc._clips.length+' 段）</span>'
    +'<span style="flex:1"></span>'
    +'<button class="btn btn-pri" style="font-size:.95em;padding:8px 28px" '
    +'onclick="_avConfirmClips(this)">✅ 確認合成</button>';
  btnRow.querySelector('button')._tc = tc;

  tc.result.innerHTML = '';
  tc.result.appendChild(grid);
  tc.result.appendChild(btnRow);
}

function _avShowClipMenu(tc, clipIndex, cell){
  // 移除舊選單
  document.querySelectorAll('.av-clip-menu').forEach(m=>m.remove());

  const menu = document.createElement('div');
  menu.className = 'av-clip-menu';
  menu.style.cssText = 'position:absolute;top:32px;right:4px;background:#fff;border-radius:8px;'
    +'box-shadow:0 4px 16px rgba(0,0,0,.25);z-index:100;min-width:140px;overflow:hidden;font-size:.82em';

  const items = [
    {icon:'🔄', label:'換一段', action:()=>_avSwapClip(tc,clipIndex)},
    {icon:'👀', label:'瀏覽備選', action:()=>_avBrowseAlts(tc,clipIndex,cell)},
    {icon:'🗑️', label:'直接移除', action:()=>_avRemoveClip(tc,clipIndex), color:'#dc2626'},
  ];
  items.forEach(it=>{
    const btn = document.createElement('button');
    btn.style.cssText = 'display:flex;align-items:center;gap:6px;width:100%;padding:8px 12px;border:none;background:none;'
      +'cursor:pointer;font-size:1em;text-align:left;white-space:nowrap';
    if(it.color) btn.style.color = it.color;
    btn.onmouseover = function(){ this.style.background='#f3f4f6'; };
    btn.onmouseout = function(){ this.style.background='none'; };
    btn.innerHTML = it.icon+' '+it.label;
    btn.onclick = function(e){ e.stopPropagation(); menu.remove(); it.action(); };
    menu.appendChild(btn);
  });

  // 顯示備選數量
  const info = document.createElement('div');
  info.style.cssText = 'padding:4px 12px 6px;font-size:.85em;color:#9ca3af;border-top:1px solid #e5e7eb';
  info.textContent = '備選：'+tc._alts.length+' 段';
  menu.appendChild(info);

  cell.appendChild(menu);
  // 點任意處關閉
  setTimeout(()=>{
    const close = (e)=>{ if(!menu.contains(e.target)){ menu.remove(); document.removeEventListener('click',close); } };
    document.addEventListener('click', close);
  }, 10);
}

function _avBlacklist(tc, clip){
  // 加入黑名單（下次不再出現），靜默呼叫不阻塞
  if(!tc._person) return;
  fetch('/api/auto-video/blacklist',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({person:tc._person, video:clip.filename, time:clip.time})
  }).catch(()=>{});
}

function _avSwapClip(tc, clipIndex){
  if(!tc._alts.length){
    alert('沒有更多備選片段了');
    return;
  }
  const removed = tc._clips[clipIndex];
  _avBlacklist(tc, removed); // 加入黑名單
  const replacement = tc._alts.shift();
  tc._clips[clipIndex] = replacement;
  // 被換掉的不放回備選（已黑名單）
  _avRenderClipPreview(tc);
}

function _avRemoveClip(tc, clipIndex){
  if(tc._clips.length <= 1){
    alert('至少需要保留 1 個片段');
    return;
  }
  const removed = tc._clips.splice(clipIndex, 1)[0];
  _avBlacklist(tc, removed); // 加入黑名單
  // 不放回備選
  _avRenderClipPreview(tc);
}

function _avBrowseAlts(tc, clipIndex, cell){
  if(!tc._alts.length){
    alert('沒有備選片段了');
    return;
  }
  // 彈出視窗讓使用者瀏覽所有備選，點擊選中替換
  document.querySelectorAll('.av-browse-overlay').forEach(m=>m.remove());

  const overlay = document.createElement('div');
  overlay.className = 'av-browse-overlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.6);'
    +'z-index:200;display:flex;align-items:center;justify-content:center';

  const panel = document.createElement('div');
  panel.style.cssText = 'background:#fff;border-radius:12px;padding:16px;max-width:90vw;max-height:80vh;'
    +'overflow:auto;min-width:320px';

  const header = document.createElement('div');
  header.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-bottom:12px';
  header.innerHTML = '<h3 style="margin:0;font-size:1em">選擇備選片段（共 '+tc._alts.length+' 段）</h3>';
  const closeBtn = document.createElement('button');
  closeBtn.textContent = '✕';
  closeBtn.style.cssText = 'background:none;border:none;font-size:1.3em;cursor:pointer;color:#999';
  closeBtn.onclick = ()=> overlay.remove();
  header.appendChild(closeBtn);
  panel.appendChild(header);

  const grid = document.createElement('div');
  grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px';

  tc._alts.forEach((alt, ai)=>{
    const altCell = document.createElement('div');
    const isV = alt.is_vertical;
    altCell.style.cssText = 'border:2px solid '+(isV?'var(--ok)':'var(--warn)')
      +';border-radius:8px;overflow:hidden;cursor:pointer;position:relative;background:#000';
    altCell.onmouseover = function(){ this.style.boxShadow='0 0 0 3px var(--pri)'; };
    altCell.onmouseout = function(){ this.style.boxShadow='none'; };

    const vid = document.createElement('video');
    vid.src = alt.video_url + '#t=' + alt.time;
    vid.preload = 'metadata';
    vid.muted = true;
    vid.style.cssText = 'width:100%;aspect-ratio:9/16;object-fit:cover;display:block';
    vid.onmouseover = function(){ this.currentTime=alt.time; this.play(); };
    vid.onmouseout = function(){ this.pause(); };
    altCell.appendChild(vid);

    const oTag = document.createElement('span');
    oTag.style.cssText = 'position:absolute;top:3px;left:3px;background:'+(isV?'rgba(16,185,129,.85)':'rgba(245,158,11,.85)')
      +';color:#fff;font-size:.6em;padding:1px 5px;border-radius:4px;font-weight:600';
    oTag.textContent = isV ? '直' : '橫';
    altCell.appendChild(oTag);

    const tLbl = document.createElement('span');
    tLbl.style.cssText = 'position:absolute;bottom:3px;left:3px;background:rgba(0,0,0,.6);color:#fff;'
      +'font-size:.65em;padding:1px 5px;border-radius:4px';
    tLbl.textContent = alt.time.toFixed(1)+'s';
    altCell.appendChild(tLbl);

    const fLbl = document.createElement('div');
    fLbl.style.cssText = 'padding:2px 4px;font-size:.62em;color:var(--txt2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;background:var(--card)';
    fLbl.textContent = alt.filename;
    altCell.appendChild(fLbl);

    altCell.onclick = function(){
      // 把原本的片段放回備選，用選中的替換
      const removed = tc._clips[clipIndex];
      tc._clips[clipIndex] = alt;
      tc._alts.splice(ai, 1);
      tc._alts.push(removed);
      overlay.remove();
      _avRenderClipPreview(tc);
    };
    grid.appendChild(altCell);
  });

  panel.appendChild(grid);
  overlay.appendChild(panel);
  document.body.appendChild(overlay);
  overlay.onclick = function(e){ if(e.target===overlay) overlay.remove(); };
}

async function _avConfirmClips(btn){
  const tc = btn._tc;
  if(!tc || !tc._taskId) return;
  const indices = tc._clips.map(c=>c.idx);
  btn.disabled = true;
  btn.textContent = '合成中...';

  try{
    const r = await fetch('/api/auto-video/confirm/'+tc._taskId,{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({indices: indices})
    });
    const d = await r.json();
    if(d.error){
      alert('確認失敗: '+d.error);
      btn.disabled = false;
      btn.textContent = '✅ 確認合成';
    }
    // SSE 會繼續收到 progress → done
    tc.result.innerHTML = '<div style="color:var(--txt2);font-size:.88em">⏳ 正在合成...</div>';
  }catch(e){
    alert('連線失敗: '+e);
    btn.disabled = false;
    btn.textContent = '✅ 確認合成';
  }
}

async function _avDeleteOutput(btn, relPath){
  if(!confirm('確定要刪除這部影片嗎？此操作無法復原。')) return;
  btn.disabled = true;
  btn.textContent = '刪除中...';
  try{
    const r = await fetch('/api/yt/delete-output',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rel_path: relPath})
    });
    const d = await r.json();
    if(d.ok){
      const card = btn.closest('.card');
      const video = card.querySelector('video');
      if(video) video.remove();
      const resultDiv = card.querySelector('.av-task-result');
      resultDiv.innerHTML = '<div style="color:var(--txt2);font-size:.9em">🗑️ 影片已刪除</div>';
      const title = card.querySelector('.av-task-title');
      if(title) title.textContent = title.textContent.replace('🎉 完成！','🗑️ 已刪除 —');
    }else{
      alert('刪除失敗: '+(d.error||'未知錯誤'));
      btn.disabled = false;
      btn.textContent = '🗑️ 刪除影片';
    }
  }catch(e){
    alert('連線失敗: '+e);
    btn.disabled = false;
    btn.textContent = '🗑️ 刪除影片';
  }
}

// ── 影片 → 照片擷取 ──────────────────────────────────
async function vpStart(){
  const person = document.getElementById('vp-person').value.trim();
  if(!person) return alert('請輸入人物名稱');
  const fileInput = document.getElementById('vp-file');
  if(!fileInput.files.length) return alert('請選擇影片檔案');

  const interval = document.getElementById('vp-interval').value;
  const fd = new FormData();
  fd.append('video', fileInput.files[0]);
  fd.append('celebrity', person);
  fd.append('interval', interval);

  document.getElementById('vp-progress').style.display = '';
  document.getElementById('vp-bar').style.width = '5%';
  document.getElementById('vp-pct').textContent = '上傳中...';
  document.getElementById('vp-msg').textContent = '正在上傳影片...';

  try{
    const r = await fetch('/api/photos/from-video',{method:'POST', body:fd});
    const d = await r.json();
    if(d.error){
      document.getElementById('vp-msg').textContent = '❌ '+d.error;
      document.getElementById('vp-msg').style.color = 'var(--err)';
      return;
    }
    // 監聽 SSE 進度
    const es = new EventSource('/api/photos/from-video/progress/'+d.task_id);
    es.onmessage = (e)=>{
      const ev = JSON.parse(e.data);
      if(ev.type==='progress'){
        const pct = Math.round(ev.percent||0);
        document.getElementById('vp-bar').style.width = pct+'%';
        document.getElementById('vp-pct').textContent = pct+'%';
        document.getElementById('vp-msg').textContent = ev.message||'';
      }else if(ev.type==='done'){
        es.close();
        document.getElementById('vp-bar').style.width = '100%';
        document.getElementById('vp-pct').textContent = '100%';
        document.getElementById('vp-msg').textContent = '✅ '+ev.message;
        document.getElementById('vp-msg').style.color = 'var(--ok)';
        loadCelebs();
      }else if(ev.type==='error'){
        es.close();
        document.getElementById('vp-msg').textContent = '❌ '+ev.message;
        document.getElementById('vp-msg').style.color = 'var(--err)';
      }
    };
    es.onerror = ()=>{ es.close(); };
  }catch(e){
    document.getElementById('vp-msg').textContent = '連線失敗: '+e;
    document.getElementById('vp-msg').style.color = 'var(--err)';
  }
}

// ── 片段瀏覽器 ──────────────────────────────────────
let _cbClips = [];
let _cbPerson = '';

async function openClipBrowser(){
  const person = document.getElementById('av-person').value.trim();
  if(!person) return alert('請先輸入人物名稱');
  _cbPerson = person;

  document.getElementById('cb-title').textContent = '🎞️ '+person+' — 挑選片段';
  document.getElementById('cb-grid').innerHTML = '<p style="color:var(--txt2)">載入中...</p>';
  document.getElementById('clip-browser-modal').classList.add('show');

  try{
    const r = await fetch('/api/yt/clips/'+encodeURIComponent(person));
    _cbClips = await r.json();

    if(!_cbClips.length){
      document.getElementById('cb-grid').innerHTML =
        '<p style="color:var(--txt2)">尚無擷取片段。請先使用一鍵生成或影片下載 → 擷取。</p>';
      document.getElementById('cb-count').textContent = '0 個片段';
      return;
    }

    // 填充類型篩選
    const types = [...new Set(_cbClips.map(c=>c.type))].sort();
    const sel = document.getElementById('cb-type-filter');
    sel.innerHTML = '<option value="all">全部類型 ('+_cbClips.length+')</option>';
    types.forEach(t=>{
      const n = _cbClips.filter(c=>c.type===t).length;
      sel.innerHTML += '<option value="'+t+'">'+t+' ('+n+')</option>';
    });

    // 預設全選
    _cbClips.forEach(c=>c._selected=true);
    cbRenderGrid(_cbClips);
    cbUpdateCount();
  }catch(e){
    document.getElementById('cb-grid').innerHTML = '<p style="color:var(--err)">載入失敗</p>';
  }
}

function cbRenderGrid(clips){
  const grid = document.getElementById('cb-grid');
  grid.innerHTML = '';
  document.getElementById('cb-count').textContent = clips.length + ' 個片段';

  clips.forEach((c, i)=>{
    const div = document.createElement('div');
    div.style.cssText = 'border:2px solid '+(c._selected?'var(--pri)':'var(--border)')
      +';border-radius:10px;overflow:hidden;cursor:pointer;transition:border-color .2s;background:var(--card)';
    div.dataset.idx = i;
    div.onclick = function(e){
      if(e.target.tagName==='VIDEO') return;
      c._selected = !c._selected;
      this.style.borderColor = c._selected ? 'var(--pri)' : 'var(--border)';
      this.querySelector('.cb-check').textContent = c._selected ? '✅' : '⬜';
      cbUpdateCount();
    };
    div.innerHTML =
      '<div style="position:relative;background:#000">'
      +'<video src="'+c.url+'" preload="metadata" muted style="width:100%;aspect-ratio:9/16;object-fit:cover"'
      +' onmouseover="this.play()" onmouseout="this.pause();this.currentTime=0"></video>'
      +'<span class="cb-check" style="position:absolute;top:4px;right:4px;font-size:1.1em">'
      +(c._selected?'✅':'⬜')+'</span>'
      +'<span style="position:absolute;bottom:4px;right:4px;background:rgba(0,0,0,.6);color:#fff;'
      +'font-size:.72em;padding:1px 6px;border-radius:4px">'+c.duration+'s</span></div>'
      +'<div style="padding:4px 6px;font-size:.72em;color:var(--txt2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
      +c.filename+'</div>';
    grid.appendChild(div);
  });
}

function cbFilterClips(){
  const type = document.getElementById('cb-type-filter').value;
  const filtered = type==='all' ? _cbClips : _cbClips.filter(c=>c.type===type);
  cbRenderGrid(filtered);
}

function cbSelectAll(){
  _cbClips.forEach(c=>c._selected=true);
  cbFilterClips();
  cbUpdateCount();
}
function cbDeselectAll(){
  _cbClips.forEach(c=>c._selected=false);
  cbFilterClips();
  cbUpdateCount();
}
function cbUpdateCount(){
  const n = _cbClips.filter(c=>c._selected).length;
  document.getElementById('cb-selected-count').textContent = '已選 '+n+' / '+_cbClips.length+' 個';
}

async function cbCompile(){
  const selected = _cbClips.filter(c=>c._selected);
  if(!selected.length) return alert('請至少選擇一個片段');

  const orient = document.getElementById('av-orientation').value;
  const res = document.getElementById('cb-resolution').value;
  const resolution = orient==='vertical' ? res+'p_v' : res+'p';

  const opts = {
    person: _cbPerson,
    clips: selected.map(c=>({rel_path:c.rel_path, start:0, duration:c.duration})),
    clip_duration: parseFloat(document.getElementById('cb-clip-dur').value),
    resolution: resolution,
    transition: document.getElementById('cb-transition').value,
    transition_dur: 0.5,
    audio_mode: document.getElementById('cb-audio').value,
  };

  closeModal('clip-browser-modal');

  const taskLabel = _cbPerson+' (手動 '+selected.length+' 段)';
  const taskCard = _avCreateTaskCard(taskLabel);
  document.getElementById('av-tasks-container').prepend(taskCard.card);
  taskCard.card.scrollIntoView({behavior:'smooth', block:'center'});

  try{
    const r = await fetch('/api/yt/compile-selected',{
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify(opts)
    });
    const d = await r.json();
    if(d.error){
      taskCard.msg.textContent = '❌ '+d.error;
      taskCard.msg.style.color = 'var(--err)';
      return;
    }
    _avListenSSE(d.task_id, taskCard, taskLabel);
  }catch(e){
    taskCard.msg.textContent = '連線失敗: '+e;
    taskCard.msg.style.color = 'var(--err)';
  }
}
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════
#  啟動
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    # 設定 file logging（方便遠端除錯）
    _log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web_app.log")
    _fh = logging.FileHandler(_log_path, encoding="utf-8", mode="a")
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                        datefmt="%Y-%m-%d %H:%M:%S"))
    logging.root.addHandler(_fh)
    logging.root.setLevel(logging.INFO)

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
