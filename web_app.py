# -*- coding: utf-8 -*-
"""
明星照片下載器 Web 版 v1.0
透過瀏覽器操作，區網內任何裝置皆可使用
"""

import os, sys, re, json, time, hashlib, sqlite3, threading, logging, uuid
import html as html_mod
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse
from queue import Queue, Empty

import requests as http_requests
from PIL import Image
from flask import Flask, request, Response, jsonify, send_from_directory

try:
    import imagehash
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False

# ── 設定（可透過環境變數覆寫） ─────────────────────────────
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", r"D:\CelebrityPhotos")
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "history.db")
PHASH_THRESHOLD = 8
MAX_FILE_SIZE = 20 * 1024 * 1024
PORT = int(os.environ.get("PORT", "5000"))

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
#  圖片下載器
# ══════════════════════════════════════════════════════════
def sanitize_name(name):
    return "".join(c for c in name if c not in r'<>:"/\|?*').strip(". ") or "unknown"


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
        return jsonify({"error": "請輸入明星名稱"}), 400

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
        return jsonify({"error": "請輸入至少一位明星"}), 400

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


def _download_worker(task_id, q, celebrity, opts):
    try:
        keyword = celebrity
        if opts.get("append_photo", True):
            keyword += " photo"

        sf = SIZE_FILTERS.get(opts.get("size", "any"), "")
        if opts.get("face_only"):
            sf += "+filterui:face-face"

        max_num = int(opts.get("count", 100))

        q.put({"type": "log", "msg": f"搜尋: {keyword} (數量: {max_num})", "tag": "info"})

        scraper = BingImageScraper()
        urls = scraper.search(
            keyword, max_num, sf,
            callback=lambda msg: q.put({"type": "log", "msg": msg, "tag": "info"}),
        )
        if not urls:
            q.put({"type": "log", "msg": "未找到任何圖片", "tag": "error"})
            q.put({"type": "done", "stats": {}})
            return

        q.put({"type": "log", "msg": f"找到 {len(urls)} 個連結，開始下載...", "tag": "info"})

        dl = ImageDownloader(db, celebrity, "bing")
        with tasks_lock:
            tasks[task_id]["downloader"] = dl

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
            _download_worker(task_id + f"_{i}", q, name, opts)
            if i < len(names) - 1:
                time.sleep(1)
        q.put({"type": "done", "stats": {"batch": True, "total": len(names)}})
    except Exception as e:
        q.put({"type": "error", "msg": str(e)})


# ══════════════════════════════════════════════════════════
#  HTML 前端
# ══════════════════════════════════════════════════════════
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>明星照片下載器</title>
<style>
:root{--pri:#4F46E5;--pri-h:#4338CA;--ok:#10B981;--warn:#F59E0B;--err:#EF4444;
--bg:#F3F4F6;--card:#FFF;--txt:#1F2937;--txt2:#6B7280;--border:#E5E7EB}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Microsoft JhengHei',sans-serif;
background:var(--bg);color:var(--txt);padding:16px}
.container{max-width:920px;margin:0 auto}
header{text-align:center;padding:20px 0 12px;font-size:1.6em;font-weight:700;color:var(--pri)}
header small{display:block;font-size:.45em;color:var(--txt2);font-weight:400;margin-top:4px}
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
</style>
</head>
<body>
<div class="container">
<header>明星照片下載器<small>Web 版 — 區網內任何裝置皆可使用</small></header>

<!-- 搜尋設定 -->
<div class="card">
<h3>搜尋設定</h3>
<div class="row">
  <label>明星名稱</label>
  <input type="text" id="inp-name" placeholder="例: Taylor Swift, IU, 周杰倫">
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

<!-- 已下載明星 -->
<div class="card">
<h3>已下載明星</h3>
<table>
<thead><tr><th>名稱</th><th>數量</th><th>最後下載</th></tr></thead>
<tbody id="celeb-list"></tbody>
</table>
<div class="total-bar" id="total-bar"></div>
</div>
</div>

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
<p style="margin-bottom:10px;font-size:.88em;color:var(--txt2)">每行輸入一位明星名稱：</p>
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
let currentTask = null;
let eventSource = null;

async function startDownload() {
  const name = document.getElementById('inp-name').value.trim();
  if (!name) return alert('請輸入明星名稱');
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
  if (!names) return alert('請輸入至少一位明星');
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
  return {
    celebrity,
    count: parseInt(document.getElementById('inp-count').value) || 100,
    size: document.getElementById('sel-size').value,
    append_photo: document.getElementById('chk-photo').checked,
    face_only: document.getElementById('chk-face').checked,
    dedup_url: document.getElementById('chk-url').checked,
    dedup_md5: document.getElementById('chk-md5').checked,
    dedup_phash: document.getElementById('chk-phash').checked,
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
      case 'done':
        eventSource.close(); eventSource = null;
        addLog('━━━ 完成 ━━━', 'info');
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
      '共 ' + data.length + ' 位明星，' + total + ' 張照片';
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
    print("  明星照片下載器 Web 版")
    print("=" * 50)
    print(f"  本機存取: http://localhost:{PORT}")
    print(f"  區網存取: http://{local_ip}:{PORT}")
    print(f"  下載路徑: {DOWNLOAD_ROOT}")
    print(f"  感知雜湊: {'已啟用' if HAS_IMAGEHASH else '未安裝 (pip install imagehash)'}")
    print("=" * 50)
    print("  按 Ctrl+C 停止伺服器")
    print()

    try:
        from waitress import serve
        serve(app, host="0.0.0.0", port=PORT, threads=8)
    except ImportError:
        app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
