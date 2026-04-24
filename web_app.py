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
# APP_DATA_DIR：所有「runtime 資料」的基底路徑。
# 預設等同 APP_DIR；若 source code 跟資料分開（例如 source 在 Project/，資料在 CelebrityPhotoDownloader/），
# 就用環境變數 APP_DATA_DIR 覆寫即可。
APP_DATA_DIR = os.environ.get("APP_DATA_DIR", APP_DIR)
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", os.path.join(APP_DATA_DIR, "Photos"))
VIDEO_ROOT = os.environ.get("VIDEO_ROOT", os.path.join(APP_DATA_DIR, "Videos"))
YT_ROOT = os.environ.get("YT_ROOT", os.path.join(APP_DATA_DIR, "YouTube"))
YT_DOWNLOADS = os.path.join(YT_ROOT, "downloads")
YT_EXTRACTS = os.path.join(YT_ROOT, "extracts")
DATA_DIR = os.environ.get("APP_DATA_SUBDIR", os.path.join(APP_DATA_DIR, "data"))
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
    "jiwoo": "nmixx",  # 同名於 H2H；裸名 fallback 預設 NMIXX
    "jiwoo_h2h":   "h2h",
    "jiwoo_nmixx": "nmixx",
    "haewon": "nmixx",
    "sullyoon": "nmixx",
    "bae": "nmixx",
    "kyujin": "nmixx",
    "lily": "nmixx",
    "karina": "aespa",
    "winter": "aespa",
    "giselle": "aespa",
    "ningning": "aespa",
    "hayoung": "fromis_9",   # 同名於 Apink；裸名 fallback 預設 Fromis_9
    "hayoung_fromis9": "fromis_9",
    "hayoung_apink":   "apink",
    "iu": "",  # solo
    # Apink（2011出道）
    "chorong": "apink",
    "bomi":    "apink",
    "eunji":   "apink",
    "naeun":   "apink",
    "namjoo":  "apink",
    # BLACKPINK
    "jisoo": "blackpink",
    "jennie": "blackpink",
    "rose": "blackpink",
    "rosé": "blackpink",
    "lisa": "blackpink",
    # TWICE
    "nayeon": "twice",
    "jeongyeon": "twice",
    "momo": "twice",
    "sana": "twice",
    "jihyo": "twice",
    "mina": "twice",
    "dahyun": "twice",
    "chaeyoung": "twice",  # 同名於 Fromis_9；此為裸名 fallback，以 TWICE 為預設 hashtag
    "chaeyoung_twice":    "twice",
    "chaeyoung_fromis9":  "fromis_9",
    "tzuyu": "twice",
    # NewJeans
    "minji": "newjeans",
    "hanni": "newjeans",
    "danielle": "newjeans",
    "haerin": "newjeans",
    "hyein": "newjeans",
    # (G)I-DLE
    "miyeon": "gidle",
    "minnie": "gidle",
    "soyeon": "gidle",
    "yuqi": "gidle",
    "shuhua": "gidle",
    # Red Velvet
    "irene": "redvelvet",
    "seulgi": "redvelvet",
    "wendy": "redvelvet",
    "joy": "redvelvet",
    "yeri": "redvelvet",
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


# ── 衝突名字處理（同名於多團，例：Chaeyoung ∈ {TWICE, Fromis_9}） ──
# 與前端 AV_GROUPS 同步，用來偵測衝突名 + 在 person key 裡編碼團名。
# Folder key 範例：`Chaeyoung_TWICE`、`Chaeyoung_Fromis9`；
# sanitize_name 轉小寫後成為獨立資料夾（chaeyoung_twice、chaeyoung_fromis9）。
_AV_GROUPS_PY = {
    'Fromis_9':    ['Saerom', 'Hayoung', 'Gyuri', 'Jiwon', 'Jisun', 'Seoyeon', 'Chaeyoung', 'Nagyung', 'Jiheon'],
    'IVE':         ['Yujin', 'Gaeul', 'Rei', 'Wonyoung', 'Liz', 'Leeseo'],
    'Babymonster': ['Ruka', 'Pharita', 'Asa', 'Ahyeon', 'Rami', 'Rora', 'Chiquita'],
    'IU':          ['IU'],
    'QWER':        ['Chodan', 'Magenta', 'Hina', 'Siyeon'],
    'Illit':       ['Yunah', 'Minju', 'Moka', 'Wonhee', 'Iroha'],
    'H2H':         ['Carmen', 'Jiwoo', 'Yuha', 'Stella', 'Juun', 'Ana', 'Ian', 'Yeon'],
    'aespa':       ['Karina', 'Winter', 'Giselle', 'Ningning'],
    'ITZY':        ['Yeji', 'Lia', 'Ryujin', 'Chaeryeong', 'Yuna'],
    'LE_SSERAFIM': ['Sakura', 'Chaewon', 'Yunjin', 'Kazuha', 'Eunchae'],
    'BLACKPINK':   ['Jisoo', 'Jennie', 'Rosé', 'Lisa'],
    'TWICE':       ['Nayeon', 'Jeongyeon', 'Momo', 'Sana', 'Jihyo', 'Mina', 'Dahyun', 'Chaeyoung', 'Tzuyu'],
    'NewJeans':    ['Minji', 'Hanni', 'Danielle', 'Haerin', 'Hyein'],
    '(G)I-DLE':    ['Miyeon', 'Minnie', 'Soyeon', 'Yuqi', 'Shuhua'],
    'Red Velvet':  ['Irene', 'Seulgi', 'Wendy', 'Joy', 'Yeri'],
    'NMIXX':       ['Lily', 'Haewon', 'Sullyoon', 'Bae', 'Jiwoo', 'Kyujin'],
    'Apink':       ['Chorong', 'Bomi', 'Eunji', 'Naeun', 'Namjoo', 'Hayoung'],
}

def _group_slug(g):
    """去掉所有非字母數字字元，例：'(G)I-DLE' → 'GIDLE'、'Fromis_9' → 'Fromis9'、'Red Velvet' → 'RedVelvet'。"""
    import re as _re
    return _re.sub(r'[^A-Za-z0-9]', '', g or '')

_AV_NAME_GROUPS = {}
for _g, _ms in _AV_GROUPS_PY.items():
    for _m in _ms:
        _AV_NAME_GROUPS.setdefault(_m, []).append(_g)

# ── Groups/members canonical JSON（2026-04-23 新增）──
# data/groups.json 是前後端共用的 single source of truth。
# 若檔案存在且解析成功，會 **覆蓋** 上方 hardcoded 的 `_DEFAULT_CELEB_GROUPS`、
# `_AV_GROUPS_PY`、`_AV_NAME_GROUPS`；hardcoded 保留為 fallback。
_GROUPS_CONFIG_PATH = os.path.join(APP_DIR, "data", "groups.json")
_AV_KO_NAMES_PY = {}  # 韓文名（新增欄位，以前只在前端 hardcoded）
_GROUPS_CONFIG_RAW = None  # 給 /api/groups-data + template 注入使用

def _load_groups_config():
    """讀 data/groups.json → 回傳 parsed dict；失敗回 None。"""
    if not os.path.isfile(_GROUPS_CONFIG_PATH):
        return None
    try:
        with open(_GROUPS_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "groups" not in data:
            logging.warning("[groups.json] 格式錯誤：缺少 groups 欄位")
            return None
        return data
    except Exception as e:
        logging.warning(f"[groups.json] 載入失敗: {e}")
        return None


def _apply_groups_config(data):
    """把 JSON 的內容套用到模組 level 常數。"""
    global _DEFAULT_CELEB_GROUPS, _AV_GROUPS_PY, _AV_NAME_GROUPS, _AV_KO_NAMES_PY
    # 1) 重建 _AV_GROUPS_PY：group_name → [members]
    new_groups = {}
    for g_name, g_info in (data.get("groups") or {}).items():
        if not isinstance(g_info, dict): continue
        members = g_info.get("members") or []
        if isinstance(members, list) and members:
            new_groups[g_name] = list(members)
    if new_groups:
        _AV_GROUPS_PY = new_groups

    # 2) 重建 _AV_NAME_GROUPS
    new_name_groups = {}
    for g, ms in _AV_GROUPS_PY.items():
        for m in ms:
            new_name_groups.setdefault(m, []).append(g)
    _AV_NAME_GROUPS = new_name_groups

    # 3) 重建 _DEFAULT_CELEB_GROUPS：名字(lower) → group_slug
    #    先從 groups 的 members auto-populate（衝突由後面的 overrides 決定 fallback）
    new_default = {}
    for g_name, g_info in (data.get("groups") or {}).items():
        slug = (g_info or {}).get("slug", "") or ""
        for m in (g_info or {}).get("members", []) or []:
            key = m.lower()
            # 不覆蓋已存在的（第一次設定的為準；衝突名由 overrides 決定）
            new_default.setdefault(key, slug)
    # overrides 強制寫入（處理衝突名 fallback、別名、多拼法）
    for k, v in (data.get("name_group_overrides") or {}).items():
        new_default[k.lower().strip()] = (v or "").strip()
    if new_default:
        _DEFAULT_CELEB_GROUPS = new_default

    # 4) ko_names
    kn = data.get("ko_names") or {}
    if isinstance(kn, dict):
        _AV_KO_NAMES_PY = dict(kn)

# 載入設定（若存在則覆寫預設）
_GROUPS_CONFIG_RAW = _load_groups_config()
if _GROUPS_CONFIG_RAW:
    _apply_groups_config(_GROUPS_CONFIG_RAW)
    logging.info(
        f"[groups.json] 已載入 canonical groups: {len(_AV_GROUPS_PY)} 團、"
        f"{sum(len(v) for v in _AV_GROUPS_PY.values())} 位成員、"
        f"{len(_AV_KO_NAMES_PY)} 筆韓文名"
    )

def _split_person_key(key):
    """把 `Chaeyoung_TWICE` 拆成 (display='Chaeyoung', group='TWICE')。

    規則：
      - key 必須含底線；最後一段 slug 必須對應某個已知團名
      - 前半段 name 必須在 >=2 個團裡都出現（真的衝突才拆）
      - 否則整串當 person name 返回：(key, '')
    """
    if not key or '_' not in key:
        return key, ''
    head, _, tail = key.rpartition('_')
    if len(_AV_NAME_GROUPS.get(head, [])) < 2:
        return key, ''
    for _g in _AV_GROUPS_PY:
        if _group_slug(_g) == tail:
            return head, _g
    return key, ''

def _display_name(person_key):
    """取顯示名（若 key 為衝突名後綴形式則脫掉後綴，否則原樣）。"""
    return _split_person_key(person_key)[0]


def _generate_caption(person):
    """根據格式 `{name} #{name} #{group}` 生成文案與 hashtags。

    回傳 dict: {"caption": str, "hashtags": [str, ...], "text": str}
    """
    name_raw = (person or "").strip()
    if not name_raw:
        return {"caption": "", "hashtags": [], "text": ""}
    # 衝突名字的 key（例：Chaeyoung_TWICE）要拆成顯示名 + 團名
    display, group_from_key = _split_person_key(name_raw)
    groups = _load_celeb_groups()
    # 先以完整 key 查（允許 celeb_groups.json 為 chaeyoung_twice 設定特定 group）
    group = groups.get(name_raw.lower(), "")
    if not group:
        group = groups.get(display.lower(), "")
    if not group and group_from_key:
        group = group_from_key
    tags = [display]
    if group:
        tags.append(group)
    text = display + " " + " ".join(f"#{t}" for t in tags)
    return {
        "caption": display,
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
        # 並發下載的共享狀態保護：phash_cache / _next / stats
        self._write_lock = threading.Lock()

    def stop(self):
        self._stop.set()

    def download_all(self, urls, dedup_url=True, dedup_md5=True,
                     dedup_phash=True, progress_cb=None, log_cb=None,
                     max_workers=8):
        """並行下載（2026-04-23 升級）。

        預設 8 個 worker thread。HTTP GET / PIL verify / 寫檔都在鎖外，
        只有 `_phash_cache` 查重、filename reserve、stats 更新進短 critical section。
        序列版的單次 `_one` 原本 ~1-2 秒（HTTP），平行後 120 張從 ~150s 壓到 ~25s。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        total = len(urls)
        if not total:
            return self.stats

        done = [0]
        done_lock = threading.Lock()

        def _task(url):
            if self._stop.is_set():
                return {"status": "stop", "msg": "使用者中止"}
            try:
                return self._one(url, dedup_url, dedup_md5, dedup_phash)
            except Exception as e:
                with self._write_lock:
                    self.stats["failed"] += 1
                return {"status": "error", "msg": f"thread error: {e}"}

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="dl") as ex:
            futures = [ex.submit(_task, u) for u in urls]
            try:
                for fut in as_completed(futures):
                    if self._stop.is_set():
                        for f in futures:
                            f.cancel()
                        if log_cb: log_cb("stop", "使用者中止", {})
                        break
                    try:
                        r = fut.result()
                    except Exception as e:
                        r = {"status": "error", "msg": f"unknown: {e}"}
                    with done_lock:
                        done[0] += 1
                        d = done[0]
                    if progress_cb: progress_cb(d, total, self.stats)
                    if log_cb: log_cb(r["status"], r.get("msg", ""), r)
            except KeyboardInterrupt:
                self._stop.set()
                raise
        return self.stats

    def _one(self, url, du, dm, dp):
        """單張下載。Thread-safe：狀態寫入都經 self._write_lock，HTTP/寫檔在鎖外。"""
        if du and self.db.url_exists(url):
            with self._write_lock:
                self.stats["skip_url"] += 1
            return {"status": "skip_url", "msg": "URL 已存在，跳過"}
        try:
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.content
        except Exception as e:
            with self._write_lock:
                self.stats["failed"] += 1
            return {"status": "error", "msg": f"下載失敗: {e}"}
        if len(data) > MAX_FILE_SIZE or len(data) < 1000:
            with self._write_lock:
                self.stats["failed"] += 1
            return {"status": "error", "msg": "檔案大小異常"}
        try:
            img = Image.open(BytesIO(data)); img.verify()
            img = Image.open(BytesIO(data))
            w, h = img.size
        except Exception:
            with self._write_lock:
                self.stats["failed"] += 1
            return {"status": "error", "msg": "非有效圖片"}
        md5 = hashlib.md5(data).hexdigest()
        if dm and self.db.md5_exists(md5):
            with self._write_lock:
                self.stats["skip_md5"] += 1
            return {"status": "skip_md5", "msg": "MD5 重複，跳過"}
        phash_str = None
        phash_val = None
        if HAS_IMAGEHASH and dp:
            try:
                phash_val = imagehash.phash(img); phash_str = str(phash_val)
            except Exception:
                pass
        ext = self._ext(url, data)
        # ── 臨界區 A：phash dedup 比對 + filename reserve（短鎖，但要 atomic） ──
        with self._write_lock:
            if phash_val is not None:
                for _ex in self._phash_cache:
                    if abs(phash_val - _ex) < PHASH_THRESHOLD:
                        self.stats["skip_phash"] += 1
                        return {"status": "skip_phash", "msg": "相似圖片已存在"}
            fn = f"{self._next:05d}{ext}"
            fp = os.path.join(self.cel_dir, fn)
            while os.path.exists(fp):
                self._next += 1
                fn = f"{self._next:05d}{ext}"; fp = os.path.join(self.cel_dir, fn)
            # reserve：bump _next 在鎖內以保證唯一性
            self._next += 1
        # 寫檔（鎖外，fp 已 reserved）
        try:
            with open(fp, "wb") as f:
                f.write(data)
        except Exception as e:
            with self._write_lock:
                self.stats["failed"] += 1
            return {"status": "error", "msg": f"寫檔失敗: {e}"}
        # DB insert（DatabaseManager 自帶 lock）
        self.db.add(self.celebrity, url, fn, md5, phash_str, len(data), w, h, self.source)
        # ── 臨界區 B：phash cache append + stats（短鎖） ──
        with self._write_lock:
            if phash_str and HAS_IMAGEHASH:
                try: self._phash_cache.append(imagehash.hex_to_hash(phash_str))
                except Exception: pass
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
    # 把 data/groups.json 的 canonical 資料注入到前端 HTML。
    # 前端 AV_GROUPS / AV_KO_NAMES 從 Python side 拿，確保 single source of truth。
    html = HTML_PAGE
    try:
        html = html.replace(
            "__AV_GROUPS_JSON__",
            json.dumps(_AV_GROUPS_PY, ensure_ascii=False),
        )
        html = html.replace(
            "__AV_KO_NAMES_JSON__",
            json.dumps(_AV_KO_NAMES_PY, ensure_ascii=False),
        )
    except Exception as _e:
        logging.warning(f"[index] groups.json 注入失敗: {_e}")
    return html


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


@app.route("/api/groups-data")
def api_groups_data():
    """回傳 canonical groups/ko_names/name_overrides（從 data/groups.json 或 hardcoded 預設）。

    給外部工具 / 測試 / 前端 debug 用。主要前端是在 HTML 載入時由 template substitution 注入。
    """
    if _GROUPS_CONFIG_RAW:
        # 直接回傳 JSON 原始檔（含註解欄位 _comment 等，保持完整性）
        return jsonify(_GROUPS_CONFIG_RAW)
    # fallback：從 hardcoded 常數組出來
    return jsonify({
        "version": 1,
        "_source": "hardcoded_fallback",
        "groups": {
            g: {"slug": _group_slug(g).lower(), "members": list(ms)}
            for g, ms in _AV_GROUPS_PY.items()
        },
        "ko_names": dict(_AV_KO_NAMES_PY),
        "name_group_overrides": {},
    })


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


@app.route("/api/photos-health")
def api_photos_health():
    """每個明星資料夾的健康狀態，給 Dashboard 用。

    每個 member 傳回:
      folder, display, group, photo_count,
      face_cache_exists, face_cache_built_at,
      face_kept, face_rejected, (embeddings_raw_rows)
      status: 'ok' | 'low_photos' | 'low_faces' | 'high_reject' | 'no_cache'
    """
    out = []
    groups_map = _load_celeb_groups()
    if not os.path.isdir(DOWNLOAD_ROOT):
        return jsonify({"members": [], "download_root": DOWNLOAD_ROOT, "total": 0})

    IMG_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
    for folder in sorted(os.listdir(DOWNLOAD_ROOT)):
        full = os.path.join(DOWNLOAD_ROOT, folder)
        if not os.path.isdir(full): continue
        if folder.startswith("."): continue
        # 忽略 _unsorted 等內部資料夾
        if folder.startswith("_"): continue

        try:
            names = os.listdir(full)
        except OSError:
            continue
        photos = [n for n in names
                  if os.path.splitext(n)[1].lower() in IMG_EXTS
                  and not n.startswith(".")]
        photo_count = len(photos)

        # resolve display + group
        _disp, _grp_from_key = _split_person_key(folder)
        _grp = groups_map.get(folder.lower(), "") or groups_map.get(_disp.lower(), "") or _grp_from_key

        # embedding cache 狀態
        cache_path = os.path.join(full, ".face_embeddings.npy")
        meta_path = os.path.join(full, ".face_embeddings.meta.json")
        face_cache_exists = os.path.isfile(cache_path)
        face_built_at = None
        face_raw = face_kept = face_rejected = None
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    m = json.load(f)
                face_built_at = m.get("built_at")
                face_raw = m.get("embeddings_raw_rows")
                face_kept = m.get("embeddings_kept_rows") or m.get("embeddings_rows")
                face_rejected = m.get("embeddings_rejected_rows")
            except Exception:
                pass

        # status
        if photo_count < 30:
            status = "low_photos"
        elif not face_cache_exists:
            status = "no_cache"
        elif face_kept is not None and face_kept < 10:
            status = "low_faces"
        elif face_rejected and face_raw and face_raw > 0 and (face_rejected / face_raw) > 0.2:
            status = "high_reject"
        else:
            status = "ok"

        out.append({
            "folder": folder,
            "display": _disp,
            "group": _grp,
            "photo_count": photo_count,
            "face_cache_exists": face_cache_exists,
            "face_cache_built_at": face_built_at,
            "face_raw_rows": face_raw,
            "face_kept_rows": face_kept,
            "face_rejected_rows": face_rejected,
            "status": status,
        })

    return jsonify({
        "members": out,
        "download_root": DOWNLOAD_ROOT,
        "total": len(out),
        "generated_at": time.time(),
    })


@app.route("/api/photos-cluster-audit/<path:folder>")
def api_photos_cluster_audit(folder):
    """Tier 1 #2：用 HDBSCAN 偵測某成員資料夾是否混了別人。

    Query params:
      force=1        → 強制 rebuild embeddings（確保 files.json 最新）
      min_cluster=5  → HDBSCAN min_cluster_size，預設 5

    成功回：
      {ok:True, person, n_photos, n_clusters, dominant_fraction, is_mixed,
       clusters:[{label,size,fraction,cohesion,example_files:[...]}]}
    失敗回：
      {ok:False, reason}
    """
    from flask import request
    force = request.args.get("force", "0") in ("1", "true", "yes")
    try:
        min_cs = int(request.args.get("min_cluster", 5))
        min_cs = max(3, min(15, min_cs))
    except Exception:
        min_cs = 5

    safe_folder = sanitize_name(folder)
    try:
        result = _audit_folder_clusters(safe_folder, min_cluster_size=min_cs, force_rebuild=force)
    except Exception as e:
        logging.exception(f"[cluster-audit] {safe_folder} failed")
        return jsonify({"ok": False, "reason": f"{type(e).__name__}: {e}"}), 500
    return jsonify(result)


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

        # 衝突名字：celebrity 可能是 'Chaeyoung_TWICE' 這類 key
        # folder 用完整 key（保持隔離），搜尋詞用 "Group Display"
        _disp, _grp = _split_person_key(celebrity)
        if _disp != celebrity:
            _search_base = f"{_grp} {_disp}".strip() if _grp else _disp
            q.put({"type": "log",
                   "msg": f"衝突名字解析: key='{celebrity}' → 搜尋詞='{_search_base}'（資料夾: {sanitize_name(celebrity)}）",
                   "tag": "info"})
        else:
            # 無衝突裸名 → 若知道所屬團，仍自動加團名前綴以提升搜尋精準度
            # （例：Mina, Yuna, Sana 這類短/常見名，加團名大幅降低 false positive）
            _groups_map = _load_celeb_groups()
            _auto_grp = _groups_map.get(celebrity.lower(), "")
            if _auto_grp:
                _search_base = f"{_auto_grp} {celebrity}"
                q.put({"type": "log",
                       "msg": f"搜尋詞自動加團名: '{celebrity}' → '{_search_base}'",
                       "tag": "info"})
            else:
                _search_base = celebrity

        # 產生關鍵字列表，過濾掉已用過的
        _override_kw = (opts.get("search_keyword") or "").strip()
        if _override_kw:
            # 呼叫端明確指定搜尋詞（例如 "ITZY Yuna"）：生成以該詞為基礎的變體
            base = _override_kw
            if auto_keyword:
                keywords = generate_keywords(base, append_photo)
            else:
                keywords = [base + (" photo" if append_photo else "")]
            q.put({"type": "log",
                    "msg": f"覆寫搜尋詞: {base} → {len(keywords)} 組變體",
                    "tag": "info"})
        elif auto_keyword:
            all_keywords = generate_keywords(_search_base, append_photo)
            used = set(db.get_used_keywords(celebrity))
            keywords = [kw for kw in all_keywords if kw not in used]
            if not keywords:
                q.put({"type": "log", "msg": "所有關鍵字變體都已用過，重新從頭開始", "tag": "info"})
                keywords = all_keywords
            q.put({"type": "log",
                    "msg": f"可用關鍵字: {len(keywords)} 組（已用過: {len(used)} 組）",
                    "tag": "info"})
        else:
            kw = _search_base
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

        # Wave 4 S10: YouTube 搜尋階段就過濾掉明顯不適合的結果
        # 只在 YouTube 關鍵字搜尋情境啟用（URL / TikTok / playlist 不過濾）
        _apply_s10 = (platform == "youtube") and (not is_url)
        _s10_bad_title = re.compile(
            r'(?i)\b(reaction|reacts?\s+to|react\s+video|compilation|'
            r'\d+\s*hour[s]?\b|try\s+not\s+to|analysis|tutorial|'
            r'interview|press\s+con|commentary|lyric[s]?\s+video|'
            r'slowed(\s|\+)?reverb|nightcore|10\s*h\b|1\s*h\s*loop)'
        )
        _s10_rej = {"short": 0, "long": 0, "title": 0, "nodur": 0}

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
            _title = e.get("title", "") or ""
            _dur = e.get("duration")
            if _apply_s10:
                if _dur is None:
                    # extract_flat 的結果可能沒有 duration；保留不過濾
                    _s10_rej["nodur"] += 1
                else:
                    try:
                        _dv = float(_dur)
                    except Exception:
                        _dv = 0.0
                    if _dv < 10:
                        _s10_rej["short"] += 1
                        continue
                    if _dv > 900:
                        _s10_rej["long"] += 1
                        continue
                if _title and _s10_bad_title.search(_title):
                    _s10_rej["title"] += 1
                    continue
            results.append({
                "id": vid_id,
                "title": _title,
                "url": vid_url,
                "duration": _dur,
                "channel": e.get("channel") or e.get("uploader") or e.get("creator", ""),
                "view_count": e.get("view_count"),
                "thumbnail": e.get("thumbnail") or (
                    f"https://i.ytimg.com/vi/{vid_id}/hqdefault.jpg" if platform == "youtube" else ""),
                "platform": platform,
            })
        if _apply_s10 and sum(_s10_rej.values()) > 0:
            logging.info(f"S10 search filter: short={_s10_rej['short']} "
                         f"long={_s10_rej['long']} title={_s10_rej['title']} "
                         f"(kept {len(results)}/{len(entries[:max_results])})")

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
    dl = (request.args.get("dl") or "").strip()
    if dl:
        dl = os.path.basename(dl)  # 防路徑注入
        return send_from_directory(YT_ROOT, filename, as_attachment=True, download_name=dl)
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
UPLOAD_TMP = os.environ.get("UPLOAD_TMP", os.path.join(APP_DATA_DIR, "tmp_uploads"))
os.makedirs(UPLOAD_TMP, exist_ok=True)

# BGM（背景音樂）上傳目錄
BGM_DIR = os.environ.get("BGM_DIR", os.path.join(APP_DATA_DIR, "bgm"))
os.makedirs(BGM_DIR, exist_ok=True)
_BGM_AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus"}


def _resolve_bgm_path(bgm_input):
    """
    把前端傳來的 bgm 輸入解析成實際檔案路徑。支援：
      - 完整檔名（BGM_DIR 下）："my_song.mp3"
      - 絕對路徑（需存在）
      - None / 空字串 / "none" / "off" → None
    安全限制：非絕對路徑必須落在 BGM_DIR 或 UPLOAD_TMP 下。
    """
    if not bgm_input:
        return None
    s = str(bgm_input).strip()
    if not s or s.lower() in ("none", "off", "disabled"):
        return None
    # 絕對路徑 → 只接受副檔名為音訊
    if os.path.isabs(s):
        if os.path.isfile(s) and os.path.splitext(s)[1].lower() in _BGM_AUDIO_EXTS:
            return s
        return None
    # 相對路徑 → 先試 BGM_DIR，再試 UPLOAD_TMP
    for base in (BGM_DIR, UPLOAD_TMP):
        p = os.path.normpath(os.path.join(base, s))
        # 防 path traversal
        if not p.startswith(os.path.normpath(base)):
            continue
        if os.path.isfile(p) and os.path.splitext(p)[1].lower() in _BGM_AUDIO_EXTS:
            return p
    return None


@app.route("/api/bgm/list", methods=["GET"])
def api_bgm_list():
    """列出 BGM_DIR 內所有可用的背景音樂檔。"""
    try:
        files = []
        for f in sorted(os.listdir(BGM_DIR)):
            fp = os.path.join(BGM_DIR, f)
            if os.path.isfile(fp) and os.path.splitext(f)[1].lower() in _BGM_AUDIO_EXTS:
                _sz = os.path.getsize(fp)
                files.append({
                    "name": f,
                    "size_mb": round(_sz / 1024 / 1024, 2),
                    "size_kb": round(_sz / 1024),
                })
        return jsonify({"ok": True, "files": files, "dir": BGM_DIR})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/bgm/upload", methods=["POST"])
def api_bgm_upload():
    """上傳 BGM 檔案到 BGM_DIR。回傳檔名供後續 API 使用。
    支援欄位名：bgm / file（前者為舊版，後者為 UI 預設）。"""
    f = request.files.get("bgm") or request.files.get("file")
    if f is None:
        return jsonify({"error": "未上傳 BGM 檔案"}), 400
    name = (f.filename or "").strip()
    if not name:
        return jsonify({"error": "檔名不可空"}), 400
    ext = os.path.splitext(name)[1].lower()
    if ext not in _BGM_AUDIO_EXTS:
        return jsonify({"error": f"不支援的格式 {ext}（支援 {', '.join(_BGM_AUDIO_EXTS)}）"}), 400
    # 簡易 sanitize：只保留英數/中文/底線/點/減號
    import re as _re
    safe_name = _re.sub(r'[^\w\-.\u4e00-\u9fff]', '_', name)
    if not safe_name.lower().endswith(ext):
        safe_name = safe_name + ext
    dest = os.path.join(BGM_DIR, safe_name)
    # 避免覆蓋：若已存在就加時間戳
    if os.path.isfile(dest):
        stem, _ext = os.path.splitext(safe_name)
        safe_name = f"{stem}_{int(time.time())}{_ext}"
        dest = os.path.join(BGM_DIR, safe_name)
    try:
        f.save(dest)
    except Exception as e:
        return jsonify({"error": f"儲存失敗: {e}"}), 500
    size_mb = os.path.getsize(dest) / 1024 / 1024
    return jsonify({"ok": True, "name": safe_name, "size_mb": round(size_mb, 2)})


@app.route("/api/bgm/delete", methods=["POST"])
def api_bgm_delete():
    """刪除 BGM_DIR 內指定檔案。"""
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name or "/" in name or "\\" in name or ".." in name:
        return jsonify({"error": "無效的檔名"}), 400
    fp = os.path.join(BGM_DIR, name)
    if not os.path.isfile(fp):
        return jsonify({"error": "檔案不存在"}), 404
    try:
        os.remove(fp)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
    bgm_path = _resolve_bgm_path(data.get("bgm_path") or data.get("bgm"))
    try:
        bgm_volume = float(data.get("bgm_volume", 1.0))
    except Exception:
        bgm_volume = 1.0
    try:
        bgm_start = float(data.get("bgm_start", 0.0))
    except Exception:
        bgm_start = 0.0
    # Wave 5/6 進階選項
    enhance = bool(data.get("enhance", False))
    stabilize = bool(data.get("stabilize", False))
    interpolate_60fps = bool(data.get("interpolate_60fps", False))
    upscale_target = (data.get("upscale_target") or "").strip() or None
    two_pass_loudnorm_opt = bool(data.get("two_pass_loudnorm", True))

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
                bgm_path=bgm_path, bgm_volume=bgm_volume, bgm_start=bgm_start,
                enhance=enhance, stabilize=stabilize,
                interpolate_60fps=interpolate_60fps,
                upscale_target=upscale_target,
                two_pass_loudnorm=two_pass_loudnorm_opt,
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
MODELS_DIR = os.environ.get("MODELS_DIR", os.path.join(APP_DATA_DIR, "models"))
YUNET_MODEL = os.path.join(MODELS_DIR, "yunet.onnx")
SFACE_MODEL = os.path.join(MODELS_DIR, "sface.onnx")
ARCFACE_MODEL = os.path.join(MODELS_DIR, "arcface_r50.onnx")
HAS_FACE_MODELS = os.path.isfile(YUNET_MODEL) and os.path.isfile(SFACE_MODEL)
# ── 2026-04-23 Tier 1 #1：有 arcface_r50.onnx 就優先用 (512-D, 2022 模型) ──
# 仍保留 SFace (128-D, 2021) 做後備與 alignCrop 對齊
HAS_ARCFACE = os.path.isfile(ARCFACE_MODEL)
_FACE_MODEL_ID = "arcface_r50" if HAS_ARCFACE else "sface"
_FACE_EMB_DIM = 512 if HAS_ARCFACE else 128

try:
    import cv2
    import numpy as np
    HAS_CV2 = True
    # ── 並行加速 #1：讓 OpenCV / PyTorch 盡量吃滿多核 ──
    # 205 有 10 核，預設只會用 1~4 核；顯式設為 10 可讓 cv2 內部的
    # filter / resize / Laplacian / DNN CPU backend 全部使用多執行緒。
    try:
        cv2.setNumThreads(10)
    except Exception:
        pass
    try:
        import torch as _torch_for_threads
        _torch_for_threads.set_num_threads(10)
        try:
            _torch_for_threads.set_num_interop_threads(4)
        except Exception:
            pass
    except Exception:
        pass
except ImportError:
    HAS_CV2 = False

EXTRACT_CONFIG = {
    "sample_interval": 1.0,       # 每幾秒取一幀
    # cosine similarity 門檻，依 face model 自動調整：
    #   SFace (128-D): 0.40 — 官方建議 0.363；本專案實測 SFace 對 apink 內團員的 cross-person
    #                          分數仍到 0.42~0.50，需要 0.40 以上才能拒絕
    #   ArcFace R50 (512-D): 0.32 — 2026-04-23 sanity test 顯示 ArcFace 的 within/cross 分離
    #                               比 SFace 乾淨，0.32 落在 cross-p95 ~ within-p5 中間
    "similarity_threshold": 0.32 if HAS_ARCFACE else 0.40,
    "similarity_threshold_sface":   0.40,
    "similarity_threshold_arcface": 0.32,
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


# ──────────────────────────────────────────────────────────
#  ArcFace R50 adapter (onnxruntime) + FIQA
#  Tier 1 #1 & Tier 2 #4 — 2026-04-23
# ──────────────────────────────────────────────────────────
_ARCFACE_SESSION = None
_ARCFACE_LOCK = threading.Lock()


def _get_arcface_session():
    """Lazy-init a global onnxruntime InferenceSession for ArcFace R50."""
    global _ARCFACE_SESSION
    if _ARCFACE_SESSION is not None:
        return _ARCFACE_SESSION
    with _ARCFACE_LOCK:
        if _ARCFACE_SESSION is not None:
            return _ARCFACE_SESSION
        try:
            import onnxruntime as _ort
            sess_opts = _ort.SessionOptions()
            # 配合 setNumThreads(10)：讓 ORT 也吃滿 CPU
            try:
                sess_opts.intra_op_num_threads = 10
                sess_opts.inter_op_num_threads = 2
            except Exception:
                pass
            _ARCFACE_SESSION = _ort.InferenceSession(
                ARCFACE_MODEL, sess_options=sess_opts,
                providers=["CPUExecutionProvider"])
            logging.info(f"[arcface] session initialized: {ARCFACE_MODEL}")
        except Exception as e:
            logging.warning(f"[arcface] failed to init session: {e}")
            raise
    return _ARCFACE_SESSION


class _FaceRecognizerArcFace:
    """Drop-in replacement for cv2.FaceRecognizerSF — exposes alignCrop()
    (delegated to SFace, whose ArcFace-template alignment matches InsightFace)
    and feature() (returns 512-D ArcFace R50 embedding).

    Kept API-compatible with SFace:
      - alignCrop(img_bgr, face_row) -> 112x112 aligned BGR image
      - feature(aligned_bgr)         -> np.ndarray shape (1, 512), float32
    """

    def __init__(self, sface_fallback):
        self._sface = sface_fallback
        self._sess = _get_arcface_session()
        self._input_name = self._sess.get_inputs()[0].name

    def alignCrop(self, img_bgr, face_row):
        # SFace 的 alignCrop 用 ArcFace 5-landmark template，輸出 112x112 BGR，
        # 這跟 InsightFace buffalo_l 訓練時用的對齊方式一致，可直接餵 ArcFace。
        return self._sface.alignCrop(img_bgr, face_row)

    def feature(self, aligned_bgr):
        # ArcFace 訓練時使用 RGB + (x-127.5)/127.5
        try:
            rgb = cv2.cvtColor(aligned_bgr, cv2.COLOR_BGR2RGB)
            x = (rgb.astype(np.float32) - 127.5) / 127.5
            x = np.transpose(x, (2, 0, 1))[None, :, :, :]
            emb = self._sess.run(None, {self._input_name: x})[0]
            # shape (1, 512) — SFace 原本就是 (1, 128)，保持形狀一致
            return emb.astype(np.float32)
        except Exception as e:
            logging.warning(f"[arcface] feature() failed, falling back to SFace: {e}")
            return self._sface.feature(aligned_bgr)


def _pass_fiqa(aligned_bgr, min_lap_var=30.0, min_brightness=40.0, max_brightness=230.0):
    """Heuristic Face Image Quality Assessment (Tier 2 #4).

    在把一張臉加進 reference library 之前，用輕量規則擋掉模糊/太暗/過曝的臉，
    避免高噪聲照片污染 embedding 平均像。判斷寬鬆，有疑慮就放行（False positive
    比 false negative 好 — 我們寧可多收幾張，也不要把正常照片誤殺）。

    aligned_bgr: 112x112 BGR（alignCrop 輸出）
    Returns True = pass, False = reject.
    """
    try:
        if aligned_bgr is None or aligned_bgr.size == 0:
            return False
        gray = cv2.cvtColor(aligned_bgr, cv2.COLOR_BGR2GRAY)
        # Laplacian variance ~ 模糊偵測。越低越糊。
        lap = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        if lap < min_lap_var:
            return False
        mean_b = float(gray.mean())
        if mean_b < min_brightness or mean_b > max_brightness:
            return False
        return True
    except Exception:
        # 保守處理：錯誤時放行
        return True


def _build_face_models(width=320, height=320):
    """建立人臉偵測器和辨識器。
    Tier 1 #1 (2026-04-23)：若 arcface_r50.onnx 存在，回傳 ArcFace R50 adapter（512-D）；
    否則 fallback 回 SFace (128-D)。
    """
    detector = cv2.FaceDetectorYN.create(YUNET_MODEL, "", (width, height), 0.7, 0.3, 5000)
    sface = cv2.FaceRecognizerSF.create(SFACE_MODEL, "")
    if HAS_ARCFACE:
        try:
            recognizer = _FaceRecognizerArcFace(sface_fallback=sface)
        except Exception as e:
            logging.warning(f"[face-models] ArcFace init failed, using SFace: {e}")
            recognizer = sface
    else:
        recognizer = sface
    return detector, recognizer


def _cv_imread_unicode(fpath):
    """Windows-safe 讀圖：cv2.imread 無法讀取含非 ASCII 字元的路徑（如「rosé」），
    改用 numpy.fromfile + cv2.imdecode 繞過。
    讀取失敗回傳 None，與原 cv2.imread 相容。
    """
    try:
        arr = np.fromfile(fpath, dtype=np.uint8)
        if arr.size == 0:
            return None
        return cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return None


def _cv_imwrite_unicode(fpath, img, params=None):
    """Windows-safe 寫圖：cv2.imwrite 同樣無法處理含非 ASCII 字元的路徑。
    改用 cv2.imencode + numpy.tofile 繞過。成功回傳 True。"""
    try:
        ext = os.path.splitext(fpath)[1] or ".jpg"
        ok, buf = cv2.imencode(ext, img, params or [])
        if not ok:
            return False
        buf.tofile(fpath)
        return True
    except Exception:
        return False


def _filter_outlier_embeddings(mat, min_keep_ratio=0.5, inlier_threshold=0.30):
    """用 2-pass centroid refinement 過濾 outlier embeddings。

    用途：下載到的照片有可能混入別人（路人、合照、其他成員、同名但不同人）。
    這些 outlier 會污染 reference embedding，降低 face matching 準確度。

    演算法：
      Pass 1（粗濾）：拿全部 emb 算 centroid，丟掉 bottom 20% 的離群點
      Pass 2（細濾）：用 pass-1 inliers 再算 centroid（更乾淨），硬門檻 0.30 cosine
      保底：至少保留 min_keep_ratio 的 emb（避免極端情況把好人也丟掉）

    參數：
      mat: (N, D) row-normalized embeddings
      min_keep_ratio: 最少保留比例（防止過度過濾）
      inlier_threshold: 最終 cosine similarity 下限（SFace 經驗：同人 > 0.363）

    回傳: (filtered_mat, kept_indices, rejected_indices)
    """
    N = mat.shape[0]
    if N <= 5:
        return mat, list(range(N)), []

    min_n = max(3, int(N * min_keep_ratio))

    # Pass 1: 粗濾（percentile based）
    c1 = mat.mean(axis=0); c1 = c1 / (np.linalg.norm(c1) + 1e-9)
    sims1 = mat @ c1
    if N >= 20:
        p20 = float(np.percentile(sims1, 20))
        # 丟掉 < p20 或 < 0.20（清楚的極端 outlier）裡比較寬鬆的那個
        thr1 = min(p20, 0.20)
    else:
        thr1 = 0.15  # 資料少，標準放寬
    mask1 = sims1 >= thr1
    if int(mask1.sum()) < min_n:
        order = np.argsort(-sims1)
        mask1 = np.zeros(N, dtype=bool)
        mask1[order[:min_n]] = True

    # Pass 2: 細濾（refined centroid + 硬門檻）
    c2 = mat[mask1].mean(axis=0); c2 = c2 / (np.linalg.norm(c2) + 1e-9)
    sims2 = mat @ c2
    mask2 = sims2 >= inlier_threshold
    if int(mask2.sum()) < min_n:
        order = np.argsort(-sims2)
        mask2 = np.zeros(N, dtype=bool)
        mask2[order[:min_n]] = True

    kept = list(np.where(mask2)[0])
    rejected = list(np.where(~mask2)[0])
    return mat[mask2], kept, rejected


def _photo_dir_fingerprint(photo_dir):
    """回傳 (count, max_mtime) 用來判斷 cache 是否過期。
    只看主要圖片副檔名，忽略 hidden 檔（.face_embeddings.* 等）。"""
    import glob as _glob
    exts = ["*.jpg", "*.jpeg", "*.png", "*.webp"]
    files = []
    for ext in exts:
        files.extend(_glob.glob(os.path.join(photo_dir, ext)))
    if not files:
        return 0, 0.0
    mt = 0.0
    for fp in files:
        try:
            m = os.path.getmtime(fp)
            if m > mt: mt = m
        except OSError:
            pass
    return len(files), mt


def _build_reference_embeddings(person_name, progress_cb=None):
    """從已下載照片建立人臉特徵向量。

    Cache 失效規則（2026-04-23 新增）：
      - .face_embeddings.npy 旁邊存 .face_embeddings.meta.json
      - meta 記錄 build 時的 (photo_count, max_mtime, max_ref_photos)
      - 若任何欄位跟現況不符 → 強制 rebuild（新增/刪除/替換照片皆會觸發）
      - 缺 meta（舊 cache）→ 也 rebuild，確保一次遷移到新機制
    """
    photo_dir = os.path.join(DOWNLOAD_ROOT, person_name)
    if not os.path.isdir(photo_dir):
        return None

    cache_path = os.path.join(photo_dir, ".face_embeddings.npy")
    meta_path  = os.path.join(photo_dir, ".face_embeddings.meta.json")
    max_photos = EXTRACT_CONFIG["max_ref_photos"]

    cur_count, cur_mtime = _photo_dir_fingerprint(photo_dir)

    # ── 檢查 cache + meta 一致性 ──
    #   model_id 檢查：SFace ↔ ArcFace 切換時會強制 rebuild，避免 128-D/512-D 混用
    if os.path.isfile(cache_path) and os.path.isfile(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            cached_model = meta.get("model_id", "sface")  # 舊 cache 沒這欄 → 當 sface
            if (meta.get("count") == cur_count
                    and abs(float(meta.get("max_mtime", 0)) - cur_mtime) < 1e-3
                    and meta.get("max_ref_photos") == max_photos
                    and cached_model == _FACE_MODEL_ID):
                return np.load(cache_path)
            else:
                logging.info(
                    f"[emb-cache] {person_name} stale "
                    f"(count {meta.get('count')}→{cur_count}, "
                    f"mtime {meta.get('max_mtime')}→{cur_mtime}, "
                    f"cap {meta.get('max_ref_photos')}→{max_photos}, "
                    f"model {cached_model}→{_FACE_MODEL_ID}), rebuilding"
                )
        except Exception as e:
            logging.warning(f"[emb-cache] {person_name} meta read failed: {e}, rebuilding")

    import glob as _glob
    import random as _random
    exts = ["*.jpg", "*.jpeg", "*.png", "*.webp"]
    files = []
    for ext in exts:
        files.extend(_glob.glob(os.path.join(photo_dir, ext)))
    if not files:
        return None

    if len(files) > max_photos:
        _random.shuffle(files)
        files = files[:max_photos]

    detector, recognizer = _build_face_models()
    embeddings = []
    emb_files = []  # 與 embeddings 平行的檔名，供 outlier filter 記錄
    fiqa_rejected = 0  # Tier 2 #4：被 FIQA 淘汰的張數

    for i, fpath in enumerate(files):
        try:
            img = _cv_imread_unicode(fpath)
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
            # Tier 2 #4：參考照 pipeline 加 FIQA 濾網，把模糊/過暗/過曝的臉擋掉
            if not _pass_fiqa(aligned):
                fiqa_rejected += 1
                continue
            emb = recognizer.feature(aligned)
            embeddings.append(emb.flatten())
            emb_files.append(fpath)
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
    raw_rows = int(mat.shape[0])

    # ── 自我一致性過濾：丟掉 outlier embeddings（污染照片） ──
    mat_filtered, keep_idx, reject_idx = _filter_outlier_embeddings(mat)
    rejected_files = [os.path.basename(emb_files[i]) for i in reject_idx]
    if reject_idx:
        logging.info(
            f"[emb-filter] {person_name}: kept {len(keep_idx)}/{raw_rows}, "
            f"rejected {len(reject_idx)} outlier photos"
        )
    mat = mat_filtered

    # 保底：過濾後 < 3 筆就回傳 None（trigger 失敗路徑）
    if mat.shape[0] < 3:
        logging.warning(
            f"[emb-filter] {person_name}: after filter only {mat.shape[0]} embs left, abort"
        )
        return None

    np.save(cache_path, mat)

    # 寫 paired filename list（Tier 1 #2 HDBSCAN audit 用）：
    # embeddings 第 i 行對應 kept_files[i]。沒有 meta_path 旁的 .npy 重建時會再刷新。
    kept_files = [os.path.basename(emb_files[i]) for i in keep_idx]
    files_path = os.path.join(photo_dir, ".face_embeddings.files.json")
    try:
        with open(files_path, "w", encoding="utf-8") as f:
            json.dump({
                "model_id": _FACE_MODEL_ID,
                "built_at": time.time(),
                "files": kept_files,
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"[emb-cache] {person_name} files.json write failed: {e}")

    # 寫 meta（用全目錄 fingerprint，不是 sampled）
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump({
                "count": cur_count,
                "max_mtime": cur_mtime,
                "max_ref_photos": max_photos,
                "built_at": time.time(),
                "embeddings_raw_rows": raw_rows,
                "embeddings_kept_rows": int(mat.shape[0]),
                "embeddings_rejected_rows": len(reject_idx),
                "fiqa_rejected": fiqa_rejected,
                "model_id": _FACE_MODEL_ID,
                "emb_dim": int(mat.shape[1]),
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"[emb-cache] {person_name} meta write failed: {e}")

    # 寫 review 側檔：記錄被排除的檔名，給 dashboard 顯示（不動原檔）
    review_path = os.path.join(photo_dir, ".face_embeddings.review.json")
    try:
        with open(review_path, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": time.time(),
                "sampled": len(files),
                "had_face": raw_rows,
                "kept": int(mat.shape[0]),
                "rejected": len(reject_idx),
                "rejected_files": rejected_files,
                "fiqa_rejected": fiqa_rejected,
                "model_id": _FACE_MODEL_ID,
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"[emb-review] {person_name} review write failed: {e}")

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
    #   cache 檔名帶 model_id → SFace/ArcFace 不會撞到（128-D vs 512-D）
    if os.path.isdir(photo_dir):
        cache_path = os.path.join(photo_dir, f".neg_embeddings.{_FACE_MODEL_ID}.npy")
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
                    img = _cv_imread_unicode(fpath)
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
                    if not _pass_fiqa(aligned):
                        continue
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
    # 2026-04-23：加上 dim-safety — 只收跟目前 _FACE_EMB_DIM 相符的 cache，
    # 避免 SFace (128-D) / ArcFace (512-D) 過渡期 vstack 崩潰
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
                    if mat.ndim == 2 and mat.shape[0] >= 3 and mat.shape[1] == _FACE_EMB_DIM:
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


# ──────────────────────────────────────────────────────────
#  HDBSCAN mixed-folder audit — Tier 1 #2 (2026-04-23)
#  偵測某人資料夾裡面是不是混了別人（scraper 爬錯、手動混放）
# ──────────────────────────────────────────────────────────
def _audit_folder_clusters(person_name, min_cluster_size=5, force_rebuild=False):
    """檢查資料夾內部的 embedding 結構，偵測「是不是混了別人」。

    2026-04-23 修訂：原本只跑 HDBSCAN 判定 mixed，但實測 HDBSCAN 對
    「均勻分布的單一身份」（乾淨資料夾）會判成全 noise，會漏掉真正的乾淨情況。
    新版改用三道訊號組合：

      1. Pairwise cosine similarity 統計（mean / std / p5 / p95）
         - 乾淨資料夾：mean 高 (~0.4+)、p5 也夠高 (~0.2+)、std 窄
         - 混人資料夾：std 寬、p5 很低 (< 0.1)，因為存在「cross-person pair」
         - 「壞」資料夾（每張都不同人）：mean < 0.15，整批參考不可用
      2. KMeans(k=2) + silhouette_score
         - silhouette >= 0.30 且較小 cluster 占 >= 10% → 確認是 bimodal → mixed
         - 否則 → 視為單一身份
      3. HDBSCAN(leaf) 輔助視覺化（當 silhouette 判為 mixed 時可顯示 example files）

    Returns dict（失敗時回 {"ok": False, "reason": ...}）:
      {
        "ok": True, "person", "model_id", "n_photos",
        "sim_stats": {"mean", "std", "p5", "p95"},
        "verdict": "clean" | "mixed" | "broken" | "unclear",
        "verdict_reason": str,
        "silhouette": float,
        "dominant_fraction": float,   # 最大 cluster / n_photos
        "is_mixed": bool,             # verdict == "mixed"
        "clusters": [ {label, size, fraction, cohesion, example_files:[...]}, ... ],
      }
    """
    photo_dir = os.path.join(DOWNLOAD_ROOT, person_name)
    if not os.path.isdir(photo_dir):
        return {"ok": False, "reason": "folder not found"}

    cache_path = os.path.join(photo_dir, ".face_embeddings.npy")
    files_path = os.path.join(photo_dir, ".face_embeddings.files.json")

    # 沒 cache 或沒檔名清單：rebuild 一次
    need_rebuild = force_rebuild or not os.path.isfile(cache_path) or not os.path.isfile(files_path)
    if need_rebuild:
        if os.path.isfile(cache_path) and not os.path.isfile(files_path):
            try: os.remove(os.path.join(photo_dir, ".face_embeddings.meta.json"))
            except Exception: pass
        _build_reference_embeddings(person_name)
        if not os.path.isfile(cache_path) or not os.path.isfile(files_path):
            return {"ok": False, "reason": "rebuild failed"}

    try:
        mat = np.load(cache_path)
    except Exception as e:
        return {"ok": False, "reason": f"load failed: {e}"}
    if mat.ndim != 2 or mat.shape[0] < 6:
        return {"ok": False, "reason": f"insufficient embeddings ({mat.shape[0]} rows)"}

    try:
        with open(files_path, "r", encoding="utf-8") as f:
            files_meta = json.load(f)
        files = files_meta.get("files", [])
        if len(files) != mat.shape[0]:
            return {"ok": False, "reason":
                    f"files.json out of sync ({len(files)} names vs {mat.shape[0]} embs); "
                    f"?force=1 可修復"}
    except Exception as e:
        return {"ok": False, "reason": f"files.json read failed: {e}"}

    model_id = files_meta.get("model_id") or _FACE_MODEL_ID

    # 確保 L2-normalized
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1
    M = mat / norms
    n = int(M.shape[0])

    # ── 訊號 1：pairwise sim 統計 ──
    S = M @ M.T
    iu = np.triu_indices(n, k=1)
    vals = S[iu]
    s_mean = float(vals.mean())
    s_std  = float(vals.std())
    s_p5   = float(np.percentile(vals, 5))
    s_p95  = float(np.percentile(vals, 95))

    # ── 訊號 2：KMeans(k=2) + silhouette（always run）──
    silhouette = None
    km_labels = np.zeros(n, dtype=int)
    try:
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score
        if n >= 6:
            # n_init='auto' 自 sklearn 1.4 起可用；放 2 比較保守
            km = KMeans(n_clusters=2, n_init=5, random_state=0)
            km_labels = km.fit_predict(M)
            # 只有兩邊都至少 2 個才算 silhouette
            u, c = np.unique(km_labels, return_counts=True)
            if len(u) == 2 and min(c) >= 2:
                # silhouette 用 cosine 相似度距離
                D = 1.0 - S
                np.fill_diagonal(D, 0.0)
                D = np.clip(D, 0.0, 2.0)
                try:
                    silhouette = float(silhouette_score(D, km_labels, metric="precomputed"))
                except Exception:
                    silhouette = None
    except Exception as e:
        logging.warning(f"[audit] KMeans/silhouette failed: {e}")

    # ── 綜合判定 ──
    # 定義閾值（base on sanity test 觀察的 ArcFace 分佈）
    CLEAN_MEAN = 0.30
    CLEAN_P5   = 0.15
    BROKEN_MEAN = 0.15
    SIL_THRESH  = 0.15     # silhouette >= 0.15 表示兩 cluster 確實有分離
    MIN_SECOND  = 0.10

    verdict = "unclear"
    reason = ""

    # 依 km_labels 計算兩 cluster size（for mixed 判定）
    u, c = np.unique(km_labels, return_counts=True)
    smaller_size = int(min(c)) if len(u) == 2 else n
    smaller_frac = smaller_size / n

    if s_mean < BROKEN_MEAN:
        verdict = "broken"
        reason = f"mean sim={s_mean:.2f} < {BROKEN_MEAN}，本人照片彼此都不像，參考可能整批錯人"
    elif (silhouette is not None and silhouette >= SIL_THRESH
            and smaller_frac >= MIN_SECOND
            and s_p5 < CLEAN_P5):
        verdict = "mixed"
        reason = (f"silhouette={silhouette:.2f}>={SIL_THRESH}, "
                  f"smaller cluster={smaller_frac*100:.0f}%, p5={s_p5:.2f}"
                  f" → 兩群明顯分離")
    elif s_mean >= CLEAN_MEAN and s_p5 >= CLEAN_P5:
        verdict = "clean"
        reason = f"mean={s_mean:.2f}, p5={s_p5:.2f} → 單一身份"
    else:
        verdict = "unclear"
        sil_str = f"{silhouette:.2f}" if silhouette is not None else "n/a"
        reason = (f"mean={s_mean:.2f}, p5={s_p5:.2f}, silhouette={sil_str}")

    # ── 組出 clusters 顯示：mixed 時用 km_labels，其他視為單一 cluster ──
    clusters = []
    if verdict == "mixed":
        label_values = sorted(set(km_labels.tolist()))
        for lbl in label_values:
            idx = np.where(km_labels == lbl)[0]
            if len(idx) == 0: continue
            sub = M[idx]
            if len(idx) > 1:
                S_sub = sub @ sub.T
                iu_sub = np.triu_indices(len(idx), k=1)
                cohesion = float(S_sub[iu_sub].mean())
            else:
                cohesion = 1.0
            centroid = sub.mean(axis=0)
            centroid /= (np.linalg.norm(centroid) + 1e-9)
            dists = 1.0 - (sub @ centroid)
            order = np.argsort(dists)
            examples = [files[int(idx[k])] for k in order[:6]]
            clusters.append({
                "label": int(lbl),
                "size": int(len(idx)),
                "fraction": float(len(idx) / n),
                "cohesion": round(cohesion, 4),
                "example_files": examples,
            })
        clusters.sort(key=lambda c: c["size"], reverse=True)
    else:
        # 單一 cluster：全部放進 cluster 0，example 用離 centroid 最近的 6 張
        centroid = M.mean(axis=0)
        centroid /= (np.linalg.norm(centroid) + 1e-9)
        dists = 1.0 - (M @ centroid)
        order = np.argsort(dists)
        examples = [files[int(k)] for k in order[:6]]
        clusters.append({
            "label": 0,
            "size": n,
            "fraction": 1.0,
            "cohesion": round(s_mean, 4),
            "example_files": examples,
        })

    dominant_fraction = clusters[0]["fraction"] if clusters else 0.0

    return {
        "ok": True,
        "person": person_name,
        "model_id": model_id,
        "n_photos": n,
        "sim_stats": {
            "mean": round(s_mean, 4),
            "std":  round(s_std, 4),
            "p5":   round(s_p5, 4),
            "p95":  round(s_p95, 4),
        },
        "silhouette": round(silhouette, 4) if silhouette is not None else None,
        "verdict": verdict,
        "verdict_reason": reason,
        "dominant_fraction": round(dominant_fraction, 4),
        "is_mixed": (verdict == "mixed"),
        "n_clusters": len(clusters),
        "clusters": clusters,
    }


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


# ── 節拍對齊：偵測音訊 onset（鼓點/強音） ──
_ONSET_CACHE = {}


def _extract_onset_times(video_path, hop_sec=0.05):
    """
    從影片音訊抽取 onset（節拍/強音起點）時刻列表。
    hop_sec=0.05 → 20Hz 解析度，足以分辨 250 BPM 以下的節拍。
    回傳 np.ndarray[float] 秒數；失敗回傳 None。
    """
    cached = _ONSET_CACHE.get(video_path)
    if cached is not None:
        return cached
    try:
        import subprocess as _sp
        ffmpeg = _get_ffmpeg()
        cmd = [ffmpeg, "-i", video_path, "-vn", "-ac", "1", "-ar", "16000",
               "-f", "s16le", "-hide_banner", "-loglevel", "error", "-"]
        proc = _sp.run(cmd, capture_output=True, timeout=180)
        if proc.returncode != 0 or not proc.stdout:
            _ONSET_CACHE[video_path] = None
            return None
        samples = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
        sr = 16000
        hop = max(1, int(sr * hop_sec))
        win = hop * 2  # 0.1s window
        if len(samples) < win:
            _ONSET_CACHE[video_path] = None
            return None
        num = (len(samples) - win) // hop + 1
        # RMS envelope
        env = np.empty(num, dtype=np.float32)
        for i in range(num):
            s = i * hop
            env[i] = np.sqrt(np.mean(samples[s:s + win] ** 2))
        # Onset strength = positive gradient (rising energy)
        diff = np.diff(env, prepend=env[0])
        strength = np.maximum(diff, 0)
        if strength.max() <= 0:
            _ONSET_CACHE[video_path] = None
            return None
        # Normalize & peak-pick
        strength /= strength.max()
        try:
            from scipy.signal import find_peaks
            # min peak height = 0.15, min distance = 0.15s (< 400BPM)
            min_dist = max(1, int(0.15 / hop_sec))
            peaks, _ = find_peaks(strength, height=0.15, distance=min_dist)
        except Exception:
            # fallback: simple local max
            peaks = []
            for i in range(1, len(strength) - 1):
                if strength[i] > 0.15 and strength[i] >= strength[i - 1] and strength[i] >= strength[i + 1]:
                    peaks.append(i)
            peaks = np.array(peaks, dtype=np.int64)
        if len(peaks) == 0:
            _ONSET_CACHE[video_path] = None
            return None
        onset_times = peaks.astype(np.float32) * hop_sec
        _ONSET_CACHE[video_path] = onset_times
        return onset_times
    except Exception as e:
        logging.warning(f"onset extract failed for {video_path}: {e}")
        _ONSET_CACHE[video_path] = None
        return None


def _snap_cut_to_beat(onset_times, t_sec, window_sec=1.5):
    """
    把切點對齊到 ±window_sec 內最近的 onset（鼓點/強音起點）。
    找不到合適 onset 時回傳 None — 呼叫端可退回 quiet-snap。
    """
    if onset_times is None or len(onset_times) == 0:
        return None
    lo = t_sec - window_sec
    hi = t_sec + window_sec
    mask = (onset_times >= lo) & (onset_times <= hi)
    candidates = onset_times[mask]
    if len(candidates) == 0:
        return None
    idx = int(np.argmin(np.abs(candidates - t_sec)))
    return float(candidates[idx])


# ══════════════════════════════════════════════════════════════════════════════
# Wave 1 / 2 helpers — shot boundary, BGM beat, target-face ratio, blur/shake
# ══════════════════════════════════════════════════════════════════════════════

_SCENE_BOUNDARY_CACHE = {}

try:
    import scenedetect as _scenedetect_mod  # noqa: F401
    HAS_SCENEDETECT = True
except Exception:
    HAS_SCENEDETECT = False


def _detect_scene_boundaries(video_path, threshold=27.0, min_scene_len_sec=0.6):
    """
    用 PySceneDetect 找出影片的 shot boundaries（鏡頭切換時間點）。
    回傳 np.ndarray[float] 秒數；失敗或未安裝 → None。
    使用 ContentDetector（對舞台/fancam 中強烈色調變化最敏感），
    AdaptiveDetector 對「內部閃光燈/舞台燈光」較魯棒，兩者擇一。
    """
    if not HAS_SCENEDETECT:
        return None
    cached = _SCENE_BOUNDARY_CACHE.get(video_path)
    if cached is not None:
        return cached if len(cached) else None

    # 嘗試從 video scan cache 讀
    try:
        cache = _load_video_cache(video_path)
        if cache and "shot_boundaries" in cache:
            arr = np.asarray(cache["shot_boundaries"], dtype=np.float32)
            _SCENE_BOUNDARY_CACHE[video_path] = arr
            return arr if len(arr) else None
    except Exception:
        pass

    try:
        from scenedetect import SceneManager, open_video, ContentDetector
        video = open_video(video_path)
        sm = SceneManager()
        sm.add_detector(ContentDetector(threshold=threshold,
                                         min_scene_len=int(min_scene_len_sec * 24)))
        sm.detect_scenes(video=video, show_progress=False)
        scenes = sm.get_scene_list()
        # scene_list: list of (start_tc, end_tc). 我們收集所有 scene 起點（除了第 0 個 = 0s）
        # 以及最後一個 scene 的終點（做為尾邊界）
        boundaries = []
        for i, (start_tc, end_tc) in enumerate(scenes):
            t = start_tc.get_seconds()
            if i > 0:  # 第 0 個是影片開頭，不算 shot boundary
                boundaries.append(t)
        if boundaries and scenes:
            boundaries.append(scenes[-1][1].get_seconds())
        arr = np.array(sorted(set(boundaries)), dtype=np.float32)
        _SCENE_BOUNDARY_CACHE[video_path] = arr
        try:
            _save_video_cache(video_path, {"shot_boundaries": arr.tolist()})
        except Exception:
            pass
        return arr if len(arr) else None
    except Exception as e:
        logging.warning(f"scene detect failed for {os.path.basename(video_path)}: {e}")
        _SCENE_BOUNDARY_CACHE[video_path] = np.array([], dtype=np.float32)
        return None


def _snap_cut_to_shot_boundary(boundaries, t_sec, clip_dur, window_sec=0.6,
                                usable_end=None):
    """
    把切點對齊到 ±window_sec 內最近的 shot boundary（鏡頭切換點）。
    - clip_dur：片段長度；用於檢查新切點 + clip_dur 是否仍落在可用範圍內
    - usable_end：片段結束上限；預設為 None (不檢查)
    - 回傳新 t_sec（float）或 None（沒合適邊界）
    """
    if boundaries is None or len(boundaries) == 0:
        return None
    lo = t_sec - window_sec
    hi = t_sec + window_sec
    mask = (boundaries >= lo) & (boundaries <= hi)
    cand = boundaries[mask]
    if len(cand) == 0:
        return None
    idx = int(np.argmin(np.abs(cand - t_sec)))
    new_t = float(cand[idx])
    if new_t < 0:
        return None
    if usable_end is not None and (new_t + clip_dur > usable_end):
        return None
    return new_t


def _extract_onset_times_from_audio(audio_path, hop_sec=0.05):
    """
    從獨立音訊檔（BGM wav/mp3/flac）抽取 onset。邏輯跟 _extract_onset_times 相同，
    只是來源是音檔而非影片。結果快取在全域 _ONSET_CACHE（key 為 audio_path）。
    """
    cached = _ONSET_CACHE.get(audio_path)
    if cached is not None:
        return cached
    try:
        import subprocess as _sp
        ffmpeg = _get_ffmpeg()
        cmd = [ffmpeg, "-i", audio_path, "-vn", "-ac", "1", "-ar", "16000",
               "-f", "s16le", "-hide_banner", "-loglevel", "error", "-"]
        proc = _sp.run(cmd, capture_output=True, timeout=180)
        if proc.returncode != 0 or not proc.stdout:
            _ONSET_CACHE[audio_path] = None
            return None
        samples = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
        sr = 16000
        hop = max(1, int(sr * hop_sec))
        win = hop * 2
        if len(samples) < win:
            _ONSET_CACHE[audio_path] = None
            return None
        num = (len(samples) - win) // hop + 1
        env = np.empty(num, dtype=np.float32)
        for i in range(num):
            s = i * hop
            env[i] = np.sqrt(np.mean(samples[s:s + win] ** 2))
        diff = np.diff(env, prepend=env[0])
        strength = np.maximum(diff, 0)
        if strength.max() <= 0:
            _ONSET_CACHE[audio_path] = None
            return None
        strength /= strength.max()
        try:
            from scipy.signal import find_peaks
            min_dist = max(1, int(0.15 / hop_sec))
            peaks, _ = find_peaks(strength, height=0.15, distance=min_dist)
        except Exception:
            peaks = []
            for i in range(1, len(strength) - 1):
                if (strength[i] > 0.15 and strength[i] >= strength[i - 1]
                        and strength[i] >= strength[i + 1]):
                    peaks.append(i)
            peaks = np.array(peaks, dtype=np.int64)
        if len(peaks) == 0:
            _ONSET_CACHE[audio_path] = None
            return None
        onset_times = peaks.astype(np.float32) * hop_sec
        _ONSET_CACHE[audio_path] = onset_times
        return onset_times
    except Exception as e:
        logging.warning(f"bgm onset extract failed for {os.path.basename(audio_path)}: {e}")
        _ONSET_CACHE[audio_path] = None
        return None


def _analyze_bgm_energy(bgm_path, hop_sec=1.0, win_sec=2.0):
    """Wave 4 E1: 回傳 BGM 時間-能量（RMS）曲線，用來偵測 chorus（高能量區）。

    回傳 (times_array, energy_array_normalized)，失敗回 (None, None)。
    energy 以 95 百分位做正規化，chorus 通常 > 0.75，verse 通常 < 0.5。
    """
    try:
        import subprocess as _sp
        ffmpeg = _get_ffmpeg()
        cmd = [ffmpeg, "-i", bgm_path, "-vn", "-ac", "1", "-ar", "16000",
               "-f", "s16le", "-hide_banner", "-loglevel", "error", "-"]
        proc = _sp.run(cmd, capture_output=True, timeout=180)
        if proc.returncode != 0 or not proc.stdout:
            return None, None
        samples = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
        sr = 16000
        hop = max(1, int(sr * hop_sec))
        win = max(hop * 2, int(sr * win_sec))
        if len(samples) < win:
            return None, None
        num = (len(samples) - win) // hop + 1
        energy = np.empty(num, dtype=np.float32)
        for i in range(num):
            s = i * hop
            energy[i] = float(np.sqrt(np.mean(samples[s:s + win] ** 2)))
        # 用 95 百分位 (robust to peaks) 正規化到 [0,1.2]
        p95 = float(np.percentile(energy, 95))
        if p95 > 0:
            energy = np.clip(energy / p95, 0, 1.2).astype(np.float32)
        times = (np.arange(num, dtype=np.float32) * hop_sec + win_sec / 2).astype(np.float32)
        return times, energy
    except Exception as e:
        logging.warning(f"bgm energy analysis failed: {e}")
        return None, None


def _bgm_energy_at(times, energy, t):
    """查詢時間 t 處的 BGM 能量值（插值）；越界回 0.5（中性）。"""
    if times is None or energy is None or len(times) == 0:
        return 0.5
    if t <= times[0]:
        return float(energy[0])
    if t >= times[-1]:
        return float(energy[-1])
    i = int(np.searchsorted(times, t))
    i = max(1, min(len(times) - 1, i))
    t0, t1 = float(times[i - 1]), float(times[i])
    e0, e1 = float(energy[i - 1]), float(energy[i])
    if t1 == t0:
        return e0
    r = (t - t0) / (t1 - t0)
    return e0 + (e1 - e0) * r


def _extract_bgm_beat_grid(bgm_path, target_clip_dur=3.0):
    """
    為 BGM 計算「切點網格」：每個 clip 的目標長度應該吻合的時間點。
    做法：抽 onset，然後尋找主 BPM（onset 間距的眾數），把時間線切成
    downbeat 節拍（每 4 拍）。回傳：
      - beat_times: np.ndarray 拍點時間（秒）
      - downbeat_times: np.ndarray 每 4 拍 (downbeat)
      - bpm: float
      - intervals: np.ndarray 相鄰 downbeat 間距
    失敗回傳 (None, None, None, None)。
    """
    onsets = _extract_onset_times_from_audio(bgm_path)
    if onsets is None or len(onsets) < 4:
        return None, None, None, None

    # 找主 beat 間距：取 onset 相鄰差的中位數
    diffs = np.diff(onsets)
    # 過濾過短（雙擊）/ 過長的間距
    diffs = diffs[(diffs > 0.15) & (diffs < 1.5)]
    if len(diffs) == 0:
        return None, None, None, None
    # 眾數比中位數更穩：把差值 bin 到 0.02 秒
    bins = np.round(diffs / 0.02).astype(np.int32)
    uniq, cnt = np.unique(bins, return_counts=True)
    beat_interval = float(uniq[int(np.argmax(cnt))]) * 0.02
    beat_interval = max(0.3, min(1.2, beat_interval))  # 50~200 BPM
    bpm = 60.0 / beat_interval

    # 以 onset[0] 為 phase 0，生成 beat grid
    if len(onsets) == 0:
        return None, None, None, None
    t0 = float(onsets[0])
    # BGM 總長（從音檔取）
    try:
        dur = _probe_duration(_get_ffmpeg(), bgm_path)
    except Exception:
        dur = None
    end_t = dur if dur and dur > t0 + beat_interval else (onsets[-1] + beat_interval * 4)
    beat_times = np.arange(t0, end_t, beat_interval, dtype=np.float32)

    # downbeat：每 4 拍一個
    downbeat_times = beat_times[::4]

    # downbeat 間距（用來決定 clip 長度）
    # 目標 clip duration 對應幾拍？
    beats_per_clip = max(1, int(round(target_clip_dur / beat_interval)))
    # 讓 clip 長度落在 beats_per_clip 的整數倍
    cut_grid = beat_times[::beats_per_clip]

    logging.info(f"bgm beat grid: bpm={bpm:.1f}, beat={beat_interval:.3f}s, "
                 f"beats_per_clip={beats_per_clip}, {len(cut_grid)} cut points")
    return beat_times, cut_grid, bpm, beats_per_clip


# ── Target-face presence （A3）──
def _measure_clip_target_face_ratio(video_path, t_start, dur, ref_embeddings,
                                     neg_embeddings=None, sample_hz=2.0):
    """
    密集抽樣檢查「clip 內目標人物出現率」。
    - sample_hz=2.0 → 每 0.5 秒取一幀
    - 回傳 ratio ∈ [0, 1]，失敗回 None（呼叫端可保守認為 OK）
    """
    if ref_embeddings is None or len(ref_embeddings) == 0:
        return None
    try:
        detector, recognizer = _build_face_models()
        if detector is None or recognizer is None:
            return None
    except Exception:
        return None

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return None

    try:
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        if w <= 0 or h <= 0:
            return None
        detector.setInputSize((w, h))

        thresh = EXTRACT_CONFIG["similarity_threshold"] * 0.90
        n_samples = max(3, int(round(dur * sample_hz)))
        hits = 0
        for i in range(n_samples):
            t = t_start + (dur * (i + 0.5) / n_samples)
            cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            _, faces = detector.detect(frame)
            if faces is None or len(faces) == 0:
                continue
            # 取最大臉
            faces_sorted = sorted(faces, key=lambda f: f[2] * f[3], reverse=True)
            matched = False
            for f in faces_sorted[:3]:  # 最多檢查前 3 大臉，避免小臉路人
                try:
                    aligned = recognizer.alignCrop(frame, f)
                    emb = recognizer.feature(aligned).flatten()
                    nrm = np.linalg.norm(emb)
                    if nrm > 0:
                        emb = emb / nrm
                    sims = ref_embeddings @ emb
                    max_sim = float(np.max(sims))
                    if max_sim >= thresh and _passes_negative_check(
                            emb, ref_embeddings, neg_embeddings, max_sim):
                        matched = True
                        break
                except Exception:
                    continue
            if matched:
                hits += 1
        return float(hits) / n_samples if n_samples > 0 else 0.0
    finally:
        cap.release()


# ── Per-clip motion quality （A4）──
def _measure_clip_motion_quality(video_path, t_start, dur, sample_hz=4.0):
    """
    量測 clip 的清晰度與手震等級。
    - sharpness: 所有採樣幀的 Laplacian variance 中位數（越大越銳利；< 40 ≈ 糊）
    - shake: 相鄰採樣幀 dense optical flow 的平均幅度（越大越晃；正常 < 3，手震 > 6）
    回傳 (sharpness, shake) 或 (None, None)。
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return None, None
    try:
        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        n = max(3, int(round(dur * sample_hz)))
        grays = []
        lap_vars = []
        for i in range(n):
            t = t_start + (dur * (i + 0.5) / n)
            cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            g = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            # 縮小到 320px 寬度以加速（保持縱橫比）
            _h, _w = g.shape[:2]
            if _w > 320:
                _sc = 320.0 / _w
                g = cv2.resize(g, (320, int(_h * _sc)))
            lap_vars.append(float(cv2.Laplacian(g, cv2.CV_64F).var()))
            grays.append(g)

        if len(lap_vars) == 0 or len(grays) < 2:
            return None, None

        sharpness = float(np.median(lap_vars))

        # shake = mean magnitude of Farneback dense flow across consecutive samples
        shake_mags = []
        for i in range(len(grays) - 1):
            try:
                flow = cv2.calcOpticalFlowFarneback(
                    grays[i], grays[i + 1], None,
                    0.5, 2, 15, 2, 5, 1.2, 0)
                mag = np.sqrt(flow[..., 0] ** 2 + flow[..., 1] ** 2)
                shake_mags.append(float(np.mean(mag)))
            except Exception:
                continue
        shake = float(np.median(shake_mags)) if shake_mags else 0.0
        return sharpness, shake
    finally:
        cap.release()


def _face_pose_smile_score(face):
    """Wave 5 C2: 用 YuNet 5-pt landmarks 估算 pose + smile 分數，回傳 (pose, smile) ∈ [0,1]²。

    YuNet 每張臉的 shape=[15]：
        0-3   bbox (x,y,w,h)
        4,5   右眼 (x,y)
        6,7   左眼
        8,9   鼻尖
        10,11 右嘴角
        12,13 左嘴角
        14    信心
    """
    try:
        fw = float(face[2]); fh = float(face[3])
        if fw <= 1 or fh <= 1:
            return 0.5, 0.3
        rex, rey = float(face[4]), float(face[5])
        lex, ley = float(face[6]), float(face[7])
        nx, ny = float(face[8]), float(face[9])
        rmx, rmy = float(face[10]), float(face[11])
        lmx, lmy = float(face[12]), float(face[13])

        # Pose: 正臉評分
        # 1) 兩眼水平差（越小越正）
        eye_dy = abs(ley - rey) / fh  # 0~
        # 2) 鼻尖偏離雙眼中線
        eye_cx = (lex + rex) / 2.0
        nose_off = abs(nx - eye_cx) / max(1.0, fw)
        # 3) 嘴巴中心也要靠近雙眼中線（避免側臉）
        mouth_cx = (lmx + rmx) / 2.0
        mouth_off = abs(mouth_cx - eye_cx) / max(1.0, fw)
        # 綜合：每項轉 [0,1]（0=理想，越大越差）再相乘折扣
        _p1 = max(0.0, 1.0 - eye_dy * 5.0)       # eye_dy > 0.2 → 0
        _p2 = max(0.0, 1.0 - nose_off * 5.0)     # nose_off > 0.2 → 0
        _p3 = max(0.0, 1.0 - mouth_off * 5.0)
        pose = (_p1 * 0.3 + _p2 * 0.4 + _p3 * 0.3)
        pose = max(0.0, min(1.0, pose))

        # Smile: 嘴巴水平距離 / 臉寬
        mouth_w = ((lmx - rmx) ** 2 + (lmy - rmy) ** 2) ** 0.5
        mw_ratio = mouth_w / max(1.0, fw)
        # 一般口 0.25~0.30；微笑 0.30~0.38；大笑 > 0.40
        if mw_ratio <= 0.24:
            smile = 0.0
        elif mw_ratio >= 0.40:
            smile = 1.0
        else:
            smile = (mw_ratio - 0.24) / 0.16
        smile = max(0.0, min(1.0, smile))
        return pose, smile
    except Exception:
        return 0.5, 0.3


def _whole_body_face_score(face_area, frame_area):
    """鐘形曲線：全身遠景（臉佔 ~2.5%）最高分，中景以上被壓分。
    - < 0.5%：太遠，0 分
    - 0.5% ~ 2.5%：升到 1.0（遠景全身）
    - 2.5% ~ 5%：陡降到 0.7（半身）
    - 5% ~ 10%：降到 0.4（胸上）
    - > 10%：特寫，鎖在 0.2
    """
    if frame_area <= 0:
        return 0.0
    r = face_area / frame_area
    if r < 0.005:
        return 0.0
    if r < 0.025:
        return (r - 0.005) / 0.02
    if r < 0.05:
        return 1.0 - (r - 0.025) * 12.0
    if r < 0.10:
        return max(0.2, 0.7 - (r - 0.05) * 6.0)
    return 0.2


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

        # 畫質懲罰係數（品質濾網）：太暗、過曝、太糊的片段扣分
        # — 不直接跳過（避免把 clip 全體 0 分），而是乘在 combined 上
        # 亮度：最佳在 80-180；邊界以線性衰減到 0.5 權重
        if mean_brightness < 50:
            quality_mult = 0.5 + (mean_brightness - 30) / 40.0  # 30→0.5, 50→1.0
        elif mean_brightness > 210:
            quality_mult = 1.0 - (mean_brightness - 210) / 90.0  # 210→1.0, 240→0.67
        else:
            quality_mult = 1.0
        quality_mult = max(0.3, min(1.0, quality_mult))
        # 銳利度：Laplacian variance < 40 視為模糊（手持搖晃 / 失焦）
        try:
            _gray_q = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            _lap_var = float(cv2.Laplacian(_gray_q, cv2.CV_64F).var())
            if _lap_var < 40:
                quality_mult *= 0.5 + _lap_var / 80.0  # 0→0.5, 40→1.0
        except Exception:
            pass

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
                    best_target_face = None  # 記錄 landmarks 供 pose/smile 評分
                    largest_face = None
                    largest_face_area = 0
                    for f in faces:
                        area = f[2] * f[3]
                        if area > largest_face_area:
                            largest_face_area = area
                            largest_face = f
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
                                    if area > best_person_area:
                                        best_person_area = area
                                        best_target_face = f
                        except Exception:
                            pass
                        if not is_target and area > other_max_area:
                            other_max_area = area
                    target_area = best_person_area
                    face_score = _whole_body_face_score(best_person_area, frame_area)
                    # Wave 5 C2: pose + smile 分數
                    _pose_face = best_target_face if best_target_face is not None else largest_face
                    if _pose_face is not None and face_score > 0:
                        pose_sc, smile_sc = _face_pose_smile_score(_pose_face)
                    else:
                        pose_sc, smile_sc = 0.5, 0.3
                else:
                    # 通用評分：任何人的最大臉
                    areas = [f[2] * f[3] for f in faces]
                    _maxi = int(np.argmax(areas))
                    face_score = _whole_body_face_score(areas[_maxi], frame_area)
                    target_area = areas[_maxi]
                    pose_sc, smile_sc = _face_pose_smile_score(faces[_maxi])
            else:
                pose_sc, smile_sc = 0.5, 0.3
        else:
            pose_sc, smile_sc = 0.5, 0.3

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
            else:
                combined *= quality_mult  # 畫質懲罰：暗/過曝/模糊扣分
                # Wave 5 C2: pose + smile 加成（最多 +20%）。
                # 只在 face_score > 0（有臉可評）時生效
                if face_score > 0:
                    _expr_mult = 1.0 + 0.12 * pose_sc + 0.08 * smile_sc
                    combined *= _expr_mult

        scores.append({"time": t, "score": combined, "duration": duration,
                       "is_vertical": is_vertical,
                       "audio_energy": audio_score,
                       "n_faces": n_faces,
                       "target_area": float(target_area) / max(frame_area, 1),
                       "other_max_area": float(other_max_area) / max(frame_area, 1),
                       "pose": float(pose_sc),
                       "smile": float(smile_sc)})
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


# ── 搜尋變體輪替 + 影片級冷卻 ──
# 目的：同一個人物多次生成時，強迫搜尋用不同變體、強迫排除最近幾次用過的影片，
# 避免 YouTube 對相同 query 穩定回傳同一批 top-N 被我們永遠拿到。
def _query_rotation_path(person):
    if not person:
        return None
    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)
    return os.path.join(extract_dir, "_query_rotation.json")


def _get_rotation_idx(person, key):
    """讀取輪替索引（依 video_type 分別計數）。"""
    import json
    path = _query_rotation_path(person)
    if not path or not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return int(data.get(key, 0))
    except Exception:
        return 0


def _bump_rotation_idx(person, key):
    """用完一輪後 +1，下次跑會切到下一組變體。"""
    import json
    path = _query_rotation_path(person)
    if not path:
        return
    try:
        data = {}
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        data[key] = int(data.get(key, 0)) + 1
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def _video_history_path(person):
    if not person:
        return None
    extract_dir = os.path.join(YT_EXTRACTS, sanitize_name(person))
    os.makedirs(extract_dir, exist_ok=True)
    return os.path.join(extract_dir, "_video_history.json")


def _load_video_cooldown(person, keep_runs=3):
    """回傳最近 keep_runs 次生成用過的 video_id 集合（用來從候選池硬排除）。"""
    import json
    path = _video_history_path(person)
    if not path or not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            runs = json.load(f) or []
        recent = runs[-keep_runs:]
        vids = set()
        for run in recent:
            for v in (run.get("video_ids") or []):
                if v:
                    vids.add(v)
        return vids
    except Exception:
        return set()


def _save_video_cooldown(person, video_ids):
    """追加本次用到的 video_ids 到歷史。只保留最近 20 次 run。"""
    import json
    path = _video_history_path(person)
    if not path or not video_ids:
        return
    try:
        runs = []
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                runs = json.load(f) or []
        runs.append({
            "ts": datetime.now().isoformat(),
            "video_ids": list(set(video_ids)),
        })
        runs = runs[-20:]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(runs, f, ensure_ascii=False)
    except Exception:
        pass


def _extract_video_id(url_or_basename):
    """從檔名或 URL 抓 video id。檔名格式通常是 {vid}_{person}.mp4。"""
    if not url_or_basename:
        return ""
    s = str(url_or_basename)
    # URL
    import re as _re
    m = _re.search(r"[?&]v=([A-Za-z0-9_-]{6,})", s)
    if m: return m.group(1)
    m = _re.search(r"(?:youtu\.be|shorts|video)/([A-Za-z0-9_-]{6,})", s)
    if m: return m.group(1)
    # basename: {vid}_{person}.mp4 or {digits}_{person}.mp4 (tiktok)
    base = os.path.basename(s)
    base = os.path.splitext(base)[0]
    # split on _ once from left; first token is often the id
    if "_" in base:
        return base.split("_", 1)[0]
    return base


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


def _detect_face_track(video_path, t_start, clip_dur, sample_hz=4.0, sigma_sec=0.25):
    """FACE_TRACK_V1 — 密集採樣 + 雙向 smoothing，回傳每幀 crop 中心。

    - 在 clip 區間內每 1/sample_hz 秒偵測一次臉
    - 遺失幀用線性插值補（轉身、側臉會短暫失偵）
    - 以中心式 Gaussian 卷積平滑（離線處理，零 lag）
    - 內插到 30Hz 輸出 — FFmpeg sendcmd 會按時間點逐格套 crop x/y

    回傳: (samples, src_W, src_H) 或 (None, 0, 0)
          samples = [(t_rel, cx, cy), ...] 在 SOURCE 像素座標
    """
    try:
        import cv2 as _cv2
        import numpy as _np
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None, 0, 0
        W = int(cap.get(_cv2.CAP_PROP_FRAME_WIDTH))
        H = int(cap.get(_cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(_cv2.CAP_PROP_FPS) or 25.0
        if W <= 0 or H <= 0 or clip_dur <= 0:
            cap.release()
            return None, 0, 0
        detector = _cv2.FaceDetectorYN.create(YUNET_MODEL, "", (W, H), 0.6, 0.3, 5000)

        n = max(2, int(round(clip_dur * sample_hz)))
        step = clip_dur / (n - 1) if n > 1 else 0.0
        known_t, known_x, known_y = [], [], []
        prev_cx, prev_cy = None, None
        # 跳躍閥值：一步 (1/sample_hz 秒) 內臉中心移動超過這個距離就視為偵測錯誤
        max_jump = max(W, H) * 0.25
        for i in range(n):
            rel_t = i * step
            cap.set(_cv2.CAP_PROP_POS_FRAMES, int((t_start + rel_t) * fps))
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            _, faces = detector.detect(frame)
            if faces is None or len(faces) == 0:
                continue
            # 第一幀：取最大臉；之後：取離前一個中心最近的臉（時間連續性）
            if prev_cx is None:
                face = max(faces, key=lambda f: f[2] * f[3])
            else:
                face = min(faces, key=lambda f: (f[0] + f[2] / 2 - prev_cx) ** 2
                                              + (f[1] + f[3] / 2 - prev_cy) ** 2)
            cx = float(face[0] + face[2] / 2)
            cy = float(face[1] + face[3] / 2)
            # 離群點過濾：若與前一點距離超過閥值，丟棄此偵測
            if prev_cx is not None:
                if ((cx - prev_cx) ** 2 + (cy - prev_cy) ** 2) ** 0.5 > max_jump:
                    continue
            known_t.append(rel_t)
            known_x.append(cx)
            known_y.append(cy)
            prev_cx, prev_cy = cx, cy
        cap.release()
        if not known_t:
            return None, 0, 0

        kt = _np.array(known_t)
        kx = _np.array(known_x)
        ky = _np.array(known_y)
        dense_t = _np.arange(0, clip_dur + 1e-6, 1.0 / 30.0)
        dx = _np.interp(dense_t, kt, kx)
        dy = _np.interp(dense_t, kt, ky)

        sigma_samples = sigma_sec * 30.0
        win = max(3, int(sigma_samples * 3))
        if len(dx) > 2 * win + 1:
            k = _np.arange(-win, win + 1)
            g = _np.exp(-k * k / (2 * sigma_samples * sigma_samples))
            g /= g.sum()
            dx = _np.convolve(dx, g, mode='same')
            dy = _np.convolve(dy, g, mode='same')

        return [(float(t), float(x), float(y)) for t, x, y in zip(dense_t, dx, dy)], W, H
    except Exception:
        return None, 0, 0


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


_WATERMARK_CACHE = {}

def _detect_watermark_regions(video_path, max_regions=4):
    """偵測來源影片的靜態浮水印 / 頻道 logo 區塊（V2，參考 DanceMashup/logo_detect）。

    改進：30 幀、縮到 480px、mean 排除均勻填充、close 15×15×2、
    pad=12 iterative box merge —— 多字元 logo（MPD 직캠 / 입덕직캠）
    會合併成完整區塊。
    """
    try:
        cached = _WATERMARK_CACHE.get(video_path)
        if cached is not None:
            return cached
        import cv2 as _cv2
        import numpy as _np
        cap = _cv2.VideoCapture(video_path)
        if not cap.isOpened():
            _WATERMARK_CACHE[video_path] = tuple()
            return tuple()
        total = int(cap.get(_cv2.CAP_PROP_FRAME_COUNT))
        fw = int(cap.get(_cv2.CAP_PROP_FRAME_WIDTH))
        fh = int(cap.get(_cv2.CAP_PROP_FRAME_HEIGHT))
        if total < 30 or fw <= 0 or fh <= 0:
            cap.release()
            _WATERMARK_CACHE[video_path] = tuple()
            return tuple()

        # 跳過前後 5%（避開 title card / outro）
        margin = max(1, int(total * 0.05))
        idx_start = margin
        idx_end = max(margin + 1, total - margin)
        n_samples = min(30, max(8, idx_end - idx_start))
        indices = _np.linspace(idx_start, idx_end - 1, n_samples).astype(int)

        # 縮到 480px 做偵測 —— 快且穩
        scale = 480.0 / max(fw, fh) if max(fw, fh) > 480 else 1.0
        dw = max(1, int(round(fw * scale)))
        dh = max(1, int(round(fh * scale)))

        frames = []
        for fi in indices:
            cap.set(_cv2.CAP_PROP_POS_FRAMES, int(fi))
            ok, fr = cap.read()
            if not ok or fr is None:
                continue
            if scale != 1.0:
                fr = _cv2.resize(fr, (dw, dh), interpolation=_cv2.INTER_AREA)
            frames.append(_cv2.cvtColor(fr, _cv2.COLOR_BGR2GRAY))
        cap.release()
        if len(frames) < 8:
            _WATERMARK_CACHE[video_path] = tuple()
            return tuple()

        stack = _np.stack(frames).astype(_np.float32)
        std = stack.std(axis=0)
        mean = stack.mean(axis=0)

        mask = (std < 8.0).astype(_np.uint8) * 255
        # 排除近均勻填充（全黑 letterbox / 全白）
        mask[(mean < 8) | (mean > 248)] = 0

        # 較大 close kernel —— 讓多字元 logo 的字與字合併
        k_close = _cv2.getStructuringElement(_cv2.MORPH_RECT, (15, 15))
        k_open = _cv2.getStructuringElement(_cv2.MORPH_RECT, (3, 3))
        mask = _cv2.morphologyEx(mask, _cv2.MORPH_CLOSE, k_close, iterations=2)
        mask = _cv2.morphologyEx(mask, _cv2.MORPH_OPEN, k_open, iterations=1)

        nlab, _labels, stats, _cent = _cv2.connectedComponentsWithStats(
            mask, connectivity=8)
        frame_area = dw * dh
        candidates = []
        for i in range(1, nlab):
            x, y, cw, ch, area = stats[i]
            ar = area / frame_area
            if ar < 0.0003 or ar > 0.10:
                continue
            cx_rel = (x + cw / 2.0) / dw
            cy_rel = (y + ch / 2.0) / dh
            # 中心主體區拒絕
            if 0.30 < cx_rel < 0.70 and 0.25 < cy_rel < 0.75:
                continue
            aspect = cw / max(1, ch)
            if aspect < 0.15 or aspect > 8.0:
                continue
            candidates.append((ar, x, y, cw, ch))

        candidates.sort(reverse=True)
        candidates = candidates[:max_regions]

        # BOX_MERGE：放大 pad 到 12px，迭代合併重疊/相接區塊
        inv = 1.0 / scale if scale != 0 else 1.0
        pad = 12
        raw_boxes = []
        for _ar, x, y, cw, ch in candidates:
            sx = max(0, int(round(x * inv)) - pad)
            sy = max(0, int(round(y * inv)) - pad)
            sw = min(fw - sx, int(round(cw * inv)) + 2 * pad)
            sh = min(fh - sy, int(round(ch * inv)) + 2 * pad)
            if sw <= 1 or sh <= 1:
                continue
            raw_boxes.append([sx, sy, sx + sw, sy + sh])

        def _touch(a, b):
            ax1, ay1, ax2, ay2 = a
            bx1, by1, bx2, by2 = b
            return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)

        changed = True
        while changed:
            changed = False
            used = [False] * len(raw_boxes)
            out = []
            for i, b in enumerate(raw_boxes):
                if used[i]:
                    continue
                cur = list(b)
                for j in range(i + 1, len(raw_boxes)):
                    if used[j]:
                        continue
                    if _touch(cur, raw_boxes[j]):
                        cur = [min(cur[0], raw_boxes[j][0]),
                               min(cur[1], raw_boxes[j][1]),
                               max(cur[2], raw_boxes[j][2]),
                               max(cur[3], raw_boxes[j][3])]
                        used[j] = True
                        changed = True
                out.append(cur)
                used[i] = True
            raw_boxes = out

        result_list = []
        for x1, y1, x2, y2 in raw_boxes:
            bw, bh = x2 - x1, y2 - y1
            if bw <= 1 or bh <= 1:
                continue
            result_list.append((int(x1), int(y1), int(bw), int(bh)))

        result = tuple(result_list)
        _WATERMARK_CACHE[video_path] = result
        if result:
            logging.info(f"watermark detected in {os.path.basename(video_path)}: "
                         f"{len(result)} region(s) {result}")
        else:
            logging.info(f"watermark scan {os.path.basename(video_path)}: "
                         f"no region passed filters (std_min={float(std.min()):.1f})")
        return result
    except Exception as _e:
        logging.warning(f"watermark detection failed for {video_path}: {_e}")
        _WATERMARK_CACHE[video_path] = tuple()
        return tuple()


# ── LaMA 浮水印去除：CPU inpainting，只對 watermark 區域做局部處理 ──
_LAMA_MODEL = None
_LAMA_INIT_FAILED = False


def _get_lama():
    """Lazy-load LaMA；載入失敗或套件缺失則回傳 None"""
    global _LAMA_MODEL, _LAMA_INIT_FAILED
    if _LAMA_INIT_FAILED:
        return None
    if _LAMA_MODEL is not None:
        return _LAMA_MODEL
    try:
        from simple_lama_inpainting import SimpleLama
        _LAMA_MODEL = SimpleLama()
        logging.info("LaMA model loaded (CPU)")
        return _LAMA_MODEL
    except Exception as _e:
        logging.warning(f"LaMA init failed ({_e}); falling back to FFmpeg delogo")
        _LAMA_INIT_FAILED = True
        return None


def _lama_clean_segment(src_video, t_start, duration, boxes, out_path,
                         ffmpeg="ffmpeg", context_pad=40):
    """用 LaMA 把 clip segment 的 watermark 區域抹掉，輸出到 out_path。
    boxes: list of (x, y, w, h) in SOURCE pixel coords.
    回傳 True 成功 / False 失敗（呼叫端應 fallback 到 delogo）
    """
    lama = _get_lama()
    if lama is None or not boxes:
        return False
    try:
        import cv2 as _cv2
        import numpy as _np
        from PIL import Image

        cap = _cv2.VideoCapture(src_video)
        if not cap.isOpened():
            return False
        fps = cap.get(_cv2.CAP_PROP_FPS) or 30.0
        W = int(cap.get(_cv2.CAP_PROP_FRAME_WIDTH))
        H = int(cap.get(_cv2.CAP_PROP_FRAME_HEIGHT))
        n_frames = max(1, int(round(duration * fps)))
        cap.set(_cv2.CAP_PROP_POS_FRAMES, int(t_start * fps))

        fourcc = _cv2.VideoWriter_fourcc(*'mp4v')
        writer = _cv2.VideoWriter(out_path, fourcc, fps, (W, H))
        if not writer.isOpened():
            cap.release()
            return False

        # 預先計算每個 box 的裁切範圍（含 context padding）
        crop_regions = []
        for (bx, by, bw, bh) in boxes:
            cx1 = max(0, bx - context_pad)
            cy1 = max(0, by - context_pad)
            cx2 = min(W, bx + bw + context_pad)
            cy2 = min(H, by + bh + context_pad)
            # mask 位置（在 crop 內的相對座標）
            mx1 = bx - cx1
            my1 = by - cy1
            crop_regions.append((cx1, cy1, cx2, cy2, mx1, my1, bw, bh))

        t0 = time.time()
        for i in range(n_frames):
            ok, frame = cap.read()
            if not ok or frame is None:
                break
            for (cx1, cy1, cx2, cy2, mx1, my1, bw, bh) in crop_regions:
                patch = frame[cy1:cy2, cx1:cx2]
                if patch.size == 0:
                    continue
                mask = _np.zeros((cy2 - cy1, cx2 - cx1), dtype=_np.uint8)
                mask[my1:my1 + bh, mx1:mx1 + bw] = 255
                try:
                    pil_img = Image.fromarray(_cv2.cvtColor(patch, _cv2.COLOR_BGR2RGB))
                    pil_mask = Image.fromarray(mask)
                    out_pil = lama(pil_img, pil_mask)
                    cleaned = _cv2.cvtColor(_np.array(out_pil), _cv2.COLOR_RGB2BGR)
                    if cleaned.shape[:2] != patch.shape[:2]:
                        cleaned = _cv2.resize(cleaned, (patch.shape[1], patch.shape[0]))
                    frame[cy1:cy2, cx1:cx2] = cleaned
                except Exception as _e:
                    logging.warning(f"LaMA inpaint frame={i} box={(cx1,cy1,cx2,cy2)} failed: {_e}")
                    continue
            writer.write(frame)
        cap.release()
        writer.release()
        dt = time.time() - t0
        logging.info(f"LaMA cleaned {n_frames} frames ({len(boxes)} regions) in {dt:.1f}s")

        # 把原音訊 mux 回來：out_path 目前只有影片，需要搭配原 src_video 的音軌
        video_only = out_path + ".v.mp4"
        try:
            os.replace(out_path, video_only)
            mux_cmd = [
                ffmpeg, "-y",
                "-i", video_only,
                "-ss", f"{t_start:.3f}", "-t", f"{duration:.3f}", "-i", src_video,
                "-map", "0:v", "-map", "1:a?",
                "-c:v", "copy", "-c:a", "copy",
                out_path,
            ]
            import subprocess as _sp
            _r = _sp.run(mux_cmd, capture_output=True, timeout=60)
            if _r.returncode != 0:
                # mux 失敗（可能 src 沒音軌），留下純影片
                os.replace(video_only, out_path)
            else:
                try: os.remove(video_only)
                except Exception: pass
        except Exception as _e:
            logging.warning(f"LaMA audio mux failed: {_e}")
            try:
                if os.path.exists(video_only):
                    os.replace(video_only, out_path)
            except Exception: pass
        return True
    except Exception as _e:
        logging.warning(f"LaMA segment cleaning failed: {_e}")
        return False


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
        _cv_imwrite_unicode(cover_path, best_frame, [_cv2.IMWRITE_JPEG_QUALITY, 92])
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
    # 僅保留強訊號；移除 " vs "/"best of"/"top N"/"cover"/"편집"/"모음"/"커버"
    # 等常見直拍 / 翻唱 / 標題噪音字，降低誤判
    "compilation", "mashup", "mash-up",
    "fmv", "f.m.v", "fan edit", "fanedit",
    "transition",
    "best moments", "cute moments", "funny moments",
    "reaction", " meme", "memes",
    "tiktok edit", "tiktok comp", "see the difference",
    "parody", "cover by", "duet",
    "raising the expectations",
    # 中韓日（保留明確剪輯 / 合集類）
    "편집본", "剪輯", "合集", "まとめ",
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

        return samples >= 3 and hits >= 4  # 提高門檻：需要 text + corner 兩種訊號都中
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
                            person=None, prefer_vertical=False,
                            min_presence_ratio=0.25, allow_multi_member=False,
                            bgm_path=None, ref_embeddings=None, neg_embeddings=None,
                            target_ratio_min=0.0, shake_max=None, sharpness_min=None):
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
            if _face_presence_ratio < min_presence_ratio:
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

    def _clip_multi_member_stats(video_path, t_start, dur):
        """
        Returns dict:
          - is_multi: True 代表「整個 clip 都是目標非主角」應該丟棄
          - dominance: 目標平均 area / 其他最大 area（>1 代表目標更大）
          - mean_target_area: 目標平均相對面積
          - frac_non_dominant: 有多少秒目標 < 其他人（完全被蓋過）
        規則：
          - >=50% 秒數「完全被蓋過」（target_area < other_max_area × 0.8）→ is_multi=True
          - 剩下狀況（即使有多人但目標較大/相當）→ is_multi=False
          - 相容舊行為但更寬鬆：允許「目標主導的多人鏡頭」（通常是 fancam 帶到別人）
        """
        idx = _score_index.get(video_path)
        default = {"is_multi": False, "dominance": 1.0,
                   "mean_target_area": 0.0, "frac_non_dominant": 0.0}
        if not idx:
            return default
        total_sec = 0
        non_dom = 0
        sum_ta = 0.0
        sum_oa = 0.0
        sum_ta_sq = 0.0
        for sec in range(int(t_start), int(t_start + dur) + 1):
            s = idx.get(sec)
            if not s:
                continue
            total_sec += 1
            n = s.get("n_faces", 0)
            ta = float(s.get("target_area", 0.0))
            oa = float(s.get("other_max_area", 0.0))
            sum_ta += ta
            sum_oa += oa
            sum_ta_sq += ta * ta
            if n >= 2 and ta > 0 and oa > ta * 0.8:
                # 其他人 > 目標 × 0.8 → 這秒目標非主導
                non_dom += 1
            elif n >= 2 and ta <= 0 and oa > 0.01:
                # 偵測不到目標但有其他大臉 → 這秒是別人
                non_dom += 1
        if total_sec == 0:
            return default
        frac = non_dom / total_sec
        mean_ta = sum_ta / total_sec
        mean_oa = sum_oa / total_sec
        dom = mean_ta / max(mean_oa, 0.001) if mean_oa > 0 else float("inf")
        return {
            "is_multi": frac >= 0.5,
            "dominance": dom,
            "mean_target_area": mean_ta,
            "frac_non_dominant": frac,
        }

    def _clip_is_multi_member(video_path, t_start, dur):
        """Backward-compat wrapper around _clip_multi_member_stats."""
        return _clip_multi_member_stats(video_path, t_start, dur)["is_multi"]

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

        # Per-clip 多人偵測（B1 dominance-aware）：
        # 新邏輯：只要目標主導 >= 50% 秒數就放行；整個 clip 目標都被蓋過才丟
        # allow_multi_member=True 時略過這個檢查（fallback 最後一層保險閥）
        mm_stats = _clip_multi_member_stats(v, c["time"], clip_duration)
        c["_dominance"] = mm_stats["dominance"]
        c["_frac_non_dominant"] = mm_stats["frac_non_dominant"]
        if not allow_multi_member and mm_stats["is_multi"]:
            _rej["multi"] += 1
            logging.info(f"multi-member rejected: {os.path.basename(v)} "
                         f"@{c['time']:.1f}s (frac_non_dom={mm_stats['frac_non_dominant']:.2f})")
            continue
        # B1 加分：目標明顯主導 (dominance >= 2) 小幅加分
        if mm_stats["dominance"] >= 2.0:
            c["score"] *= 1.05

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

    # ── 切點對齊（shot-first → beat → quiet-fallback） ──
    # 優先順序：
    #   1. Shot boundary（PySceneDetect 偵測到的鏡頭切換）— 視覺連續性最強訊號
    #   2. Beat：若提供 BGM 則用 BGM beats，否則用來源音訊 onsets
    #   3. Quiet-snap：音訊能量最低處
    #
    # 理由：
    #   - 切到鏡頭中間最容易被觀眾感到「沒剪過」。Shot boundary 吸附能消除這個。
    #   - BGM beat 對齊讓最終成片的節奏吻合要鋪的背景音樂，而非素材本身的音訊。
    #   - 若 BGM 無 → 退回來源 onset，保持舊行為相容。
    energy_by_video = {}
    for video_path, scores in all_scores:
        if scores and isinstance(scores[0], dict) and "audio_energy" in scores[0]:
            energy_by_video[video_path] = np.array(
                [s.get("audio_energy", 0.0) for s in scores], dtype=np.float32)

    # BGM onset（若有提供）— 注意這個會影響「預期的最終 timeline cut 點」，
    # 但因為 selected clip 的起點是 source-video time，直接用 BGM onset snap
    # source time 在語意上不對。所以 BGM onset 主要用於 compile 階段決定 clip 輸出長度。
    # 這裡的 snap 行為保持用來源音訊 onset（避免混淆），BGM 對齊落在 compile。
    # — 除非來源是 mute（audio_mode），此時 source onset 無意義，才改用 BGM onset 粗略對齊。

    shot_snap_count = 0
    beat_snap_count = 0
    quiet_snap_count = 0
    for c in selected:
        vid_dur = c.get("src_duration", 9999)
        tail_buffer_snap = min(5.0, vid_dur * 0.15) if vid_dur < 30 else 5.0
        usable_end_snap = max(clip_duration, vid_dur - tail_buffer_snap)
        original_t = c["time"]
        new_t = None
        # 1. Shot boundary snap（最高優先）
        if HAS_SCENEDETECT:
            try:
                bounds = _detect_scene_boundaries(c["video"])
                if bounds is not None and len(bounds) > 0:
                    cand = _snap_cut_to_shot_boundary(
                        bounds, original_t, clip_duration,
                        window_sec=0.6, usable_end=usable_end_snap)
                    if cand is not None:
                        new_t = cand
                        shot_snap_count += 1
            except Exception as _sd_e:
                logging.debug(f"shot snap skipped: {_sd_e}")
        # 2. Beat-snap（若 shot snap 沒命中）
        if new_t is None:
            onsets = _extract_onset_times(c["video"])
            if onsets is not None:
                cand = _snap_cut_to_beat(onsets, original_t, window_sec=1.5)
                if cand is not None and 0 <= cand and cand + clip_duration <= usable_end_snap:
                    new_t = cand
                    beat_snap_count += 1
        # 3. Quiet-snap
        if new_t is None:
            energy = energy_by_video.get(c["video"])
            if energy is None or len(energy) == 0:
                continue
            cand = _snap_cut_to_quiet(energy, original_t, window_sec=1.5)
            if 0 <= cand and cand + clip_duration <= usable_end_snap:
                new_t = cand
                quiet_snap_count += 1
        if new_t is not None:
            c["time"] = new_t
    if shot_snap_count + beat_snap_count + quiet_snap_count > 0:
        logging.info(f"cut snap: shot={shot_snap_count} beat={beat_snap_count} "
                     f"quiet={quiet_snap_count} of {len(selected)}")

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

    # ── A3: Target-face 密集抽樣過濾（2Hz）──
    # 跟 min_presence_ratio（1Hz 計算）互補；這是對已選 clip 的最後確認。
    # 設 0 代表略過這個檢查。
    if target_ratio_min > 0 and ref_embeddings is not None:
        a3_kept = []
        a3_rej = 0
        for c in selected:
            try:
                r = _measure_clip_target_face_ratio(
                    c["video"], c["time"], clip_duration,
                    ref_embeddings, neg_embeddings, sample_hz=2.0)
            except Exception as _a3_e:
                logging.debug(f"A3 measure failed: {_a3_e}")
                r = None
            if r is None:
                a3_kept.append(c)  # 量測失敗視為通過（寬容）
                continue
            c["_target_ratio"] = float(r)
            if r >= target_ratio_min:
                a3_kept.append(c)
            else:
                a3_rej += 1
        if a3_rej:
            logging.info(f"A3 target-face filter: {a3_rej} rejected (min={target_ratio_min:.2f})")
        selected = a3_kept

    # ── A4: Blur / Shake 品質過濾 ──
    if (shake_max is not None or sharpness_min is not None) and selected:
        a4_kept = []
        a4_rej = {"blur": 0, "shake": 0}
        for c in selected:
            try:
                sharpness, shake = _measure_clip_motion_quality(
                    c["video"], c["time"], clip_duration, sample_hz=4.0)
            except Exception as _a4_e:
                logging.debug(f"A4 measure failed: {_a4_e}")
                sharpness, shake = None, None
            if sharpness is not None:
                c["_sharpness"] = float(sharpness)
            if shake is not None:
                c["_shake"] = float(shake)
            bad = False
            if sharpness_min is not None and sharpness is not None and sharpness < sharpness_min:
                a4_rej["blur"] += 1
                bad = True
            if shake_max is not None and shake is not None and shake > shake_max:
                a4_rej["shake"] += 1
                bad = True
            if not bad:
                a4_kept.append(c)
        if a4_rej["blur"] + a4_rej["shake"] > 0:
            logging.info(f"A4 quality filter: blur={a4_rej['blur']} shake={a4_rej['shake']} rejected")
        selected = a4_kept

    # 清理內部欄位
    for s in selected:
        s.pop("_used", None)
        s.pop("_phash", None)
        s.pop("_fan_edit", None)

    return selected


def _probe_video_size(filepath):
    """回傳 (w, h) 或 (None, None)。"""
    try:
        cap = cv2.VideoCapture(filepath)
        if not cap.isOpened():
            return None, None
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()
        if w > 0 and h > 0:
            return w, h
    except Exception:
        pass
    return None, None


def _find_realesrgan_binary():
    """尋找 realesrgan-ncnn-vulkan 可執行檔。失敗回 None。"""
    import shutil as _sh
    for name in ("realesrgan-ncnn-vulkan", "realesrgan-ncnn-vulkan.exe"):
        p = _sh.which(name)
        if p:
            return p
    # 其他常見安裝路徑
    for guess in (
        r"C:\tools\realesrgan\realesrgan-ncnn-vulkan.exe",
        os.path.expandvars(r"%USERPROFILE%\Desktop\realesrgan-ncnn-vulkan\realesrgan-ncnn-vulkan.exe"),
    ):
        if os.path.isfile(guess):
            return guess
    return None


def _upscale_final_video(output_path, target, ffmpeg=None, has_audio=True):
    """Wave 6 C4: 若來源解析度 < target，用 Real-ESRGAN (可用時) 或 ffmpeg Lanczos 升頻。

    target: "720p" / "1080p" / "auto"（"auto" → 若 < 720p 升到 720p）
    失敗保留原檔。
    """
    import subprocess
    if ffmpeg is None:
        ffmpeg = _get_ffmpeg()
    sw, sh = _probe_video_size(output_path)
    if not sw or not sh:
        return False

    # 判斷目標
    tgt_map = {"720p": 720, "1080p": 1080}
    if target == "auto":
        # 短邊 < 720 就升到 720
        short = min(sw, sh)
        if short >= 720:
            return False
        tgt_h = 720
    else:
        tgt_h = tgt_map.get(target, 0)
        if tgt_h <= 0:
            return False
        # 已達或超過目標就不做
        if min(sw, sh) >= tgt_h:
            return False

    _tmp = output_path + ".up.mp4"
    # ── 優先：Real-ESRGAN ncnn-vulkan ──
    _bin = _find_realesrgan_binary()
    if _bin:
        # Real-ESRGAN 只能對「一系列圖片」升頻。先抽幀 → 升頻 → 重組。
        # 成本太高，僅在來源極低 (< 480p) 時啟用
        if min(sw, sh) < 480:
            try:
                import tempfile as _tf
                _scratch = _tf.mkdtemp(prefix="resrgan_")
                _in_dir = os.path.join(_scratch, "in")
                _out_dir = os.path.join(_scratch, "out")
                os.makedirs(_in_dir); os.makedirs(_out_dir)
                # 抽幀
                subprocess.run(
                    [ffmpeg, "-y", "-i", output_path,
                     os.path.join(_in_dir, "f%06d.png")],
                    capture_output=True, timeout=600)
                # 升頻（scale = 2 即可覆蓋大多 480→720/1080 情境）
                subprocess.run(
                    [_bin, "-i", _in_dir, "-o", _out_dir, "-s", "2",
                     "-n", "realesrgan-x4plus-anime", "-f", "png"],
                    capture_output=True, timeout=3600)
                # 重組 + 從原檔複製音軌
                _cmd = [
                    ffmpeg, "-y",
                    "-framerate", "30",
                    "-i", os.path.join(_out_dir, "f%06d.png"),
                    "-i", output_path,
                    "-map", "0:v", "-map", "1:a?",
                    "-vf", f"scale=-2:{tgt_h}:flags=lanczos",
                    "-c:v", "libx264", "-preset", "medium", "-crf", "20",
                    "-c:a", "copy" if has_audio else "copy",
                    "-pix_fmt", "yuv420p",
                    _tmp,
                ]
                _r = subprocess.run(_cmd, capture_output=True, timeout=1800)
                shutil.rmtree(_scratch, ignore_errors=True)
                if _r.returncode == 0 and os.path.isfile(_tmp) and os.path.getsize(_tmp) > 1024:
                    os.replace(_tmp, output_path)
                    logging.info(f"upscale via Real-ESRGAN: {sw}x{sh} → target h={tgt_h}")
                    return True
                logging.warning(f"Real-ESRGAN pipeline failed, fallback to Lanczos")
            except Exception as _e:
                logging.warning(f"Real-ESRGAN failed: {_e}; fallback Lanczos")

    # ── Fallback：ffmpeg Lanczos ──
    try:
        _cmd = [
            ffmpeg, "-y", "-i", output_path,
            "-vf", f"scale=-2:{tgt_h}:flags=lanczos,unsharp=5:5:0.4:3:3:0.0",
            "-c:v", "libx264", "-preset", "medium", "-crf", "20",
            "-pix_fmt", "yuv420p",
        ]
        if has_audio:
            _cmd += ["-c:a", "copy"]
        else:
            _cmd += ["-an"]
        _cmd.append(_tmp)
        _r = subprocess.run(_cmd, capture_output=True, timeout=900)
        if _r.returncode == 0 and os.path.isfile(_tmp) and os.path.getsize(_tmp) > 1024:
            os.replace(_tmp, output_path)
            logging.info(f"upscale via Lanczos: {sw}x{sh} → target h={tgt_h}")
            return True
        if os.path.isfile(_tmp):
            try: os.remove(_tmp)
            except Exception: pass
        _serr = (_r.stderr[-300:] if _r.stderr else b'').decode(errors='replace')
        logging.warning(f"Lanczos upscale failed rc={_r.returncode}: {_serr}")
        return False
    except Exception as _e:
        logging.warning(f"upscale exception: {_e}")
        return False


def _interpolate_to_60fps(output_path, ffmpeg=None, has_audio=True):
    """Wave 6 C6: 用 minterpolate (MCI + AOBMC) 把成品升到 60fps。失敗保留原檔。"""
    import subprocess
    if ffmpeg is None:
        ffmpeg = _get_ffmpeg()
    if not os.path.isfile(output_path):
        return False
    _tmp = output_path + ".60fps.mp4"
    try:
        _cmd = [
            ffmpeg, "-y", "-i", output_path,
            "-vf", "minterpolate=fps=60:mi_mode=mci:mc_mode=aobmc:me_mode=bidir:vsbmc=1",
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-pix_fmt", "yuv420p",
        ]
        if has_audio:
            _cmd += ["-c:a", "copy"]
        else:
            _cmd += ["-an"]
        _cmd.append(_tmp)
        _r = subprocess.run(_cmd, capture_output=True, timeout=1200)
        if _r.returncode == 0 and os.path.isfile(_tmp) and os.path.getsize(_tmp) > 1024:
            os.replace(_tmp, output_path)
            logging.info(f"interpolate_60fps OK: "
                         f"{os.path.getsize(output_path)/1024/1024:.1f}MB")
            return True
        if os.path.isfile(_tmp):
            try: os.remove(_tmp)
            except Exception: pass
        _serr = (_r.stderr[-300:] if _r.stderr else b'').decode(errors='replace')
        logging.warning(f"interpolate_60fps failed rc={_r.returncode}: {_serr}")
        return False
    except Exception as _e:
        logging.warning(f"interpolate_60fps exception: {_e}")
        return False


def _two_pass_loudnorm(output_path, target_I=-14.0, target_TP=-1.5, target_LRA=11.0,
                       timeout_sec=600):
    """Wave 4 F1: 對最終成品做 two-pass EBU R128 loudnorm（in-place）。

    第一遍 analyze 量測；第二遍帶 measured 參數做精準壓縮 + linear=true 預防削波。
    失敗時保留原檔。回傳 True 表示成功替換了 output_path。
    """
    import subprocess, json, re as _re
    ffmpeg = _get_ffmpeg()
    if not os.path.isfile(output_path):
        return False

    # Pass 1: analyze
    try:
        af1 = (f"loudnorm=I={target_I}:TP={target_TP}:LRA={target_LRA}:"
               f"print_format=json")
        r1 = subprocess.run(
            [ffmpeg, "-hide_banner", "-nostats", "-i", output_path,
             "-vn", "-af", af1, "-f", "null", "-"],
            capture_output=True, text=True, timeout=timeout_sec)
        stderr = r1.stderr or ""
        # loudnorm 印出的 JSON 位在 stderr 最後一段 { ... }
        matches = _re.findall(r'\{[^{}]*"input_i"[^{}]*\}', stderr, _re.DOTALL)
        if not matches:
            logging.warning(f"two_pass_loudnorm: pass1 JSON not found (rc={r1.returncode})")
            return False
        stats = json.loads(matches[-1])
        measured_I = stats.get("input_i")
        measured_TP = stats.get("input_tp")
        measured_LRA = stats.get("input_lra")
        measured_thresh = stats.get("input_thresh")
        offset = stats.get("target_offset")
        if any(v is None for v in (measured_I, measured_TP, measured_LRA,
                                    measured_thresh, offset)):
            logging.warning(f"two_pass_loudnorm: pass1 stats incomplete: {stats}")
            return False
    except Exception as e:
        logging.warning(f"two_pass_loudnorm: pass1 failed: {e}")
        return False

    # Pass 2: apply
    try:
        af2 = (f"loudnorm=I={target_I}:TP={target_TP}:LRA={target_LRA}:"
               f"measured_I={measured_I}:measured_TP={measured_TP}:"
               f"measured_LRA={measured_LRA}:measured_thresh={measured_thresh}:"
               f"offset={offset}:linear=true:print_format=summary")
        tmp_out = output_path + ".ln2.mp4"
        r2 = subprocess.run(
            [ffmpeg, "-y", "-hide_banner", "-nostats", "-i", output_path,
             "-af", af2,
             "-c:v", "copy",
             "-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2",
             tmp_out],
            capture_output=True, timeout=timeout_sec)
        if r2.returncode == 0 and os.path.isfile(tmp_out) and os.path.getsize(tmp_out) > 1024:
            os.replace(tmp_out, output_path)
            logging.info(f"two_pass_loudnorm OK: measured_I={measured_I} → target {target_I}")
            return True
        else:
            _serr = (r2.stderr[-300:] if r2.stderr else b'').decode(errors='replace')
            logging.warning(f"two_pass_loudnorm: pass2 failed rc={r2.returncode}: {_serr}")
            if os.path.isfile(tmp_out):
                try: os.remove(tmp_out)
                except Exception: pass
            return False
    except Exception as e:
        logging.warning(f"two_pass_loudnorm: pass2 exception: {e}")
        return False


def _compile_highlight(clips, clip_duration, output_path, transition,
                       transition_dur, resolution, progress_cb=None,
                       audio_mode="original", loudnorm_params=None,
                       name_tag=None, cover_photo=None, cover_duration=1.0,
                       bgm_path=None, bgm_volume=1.0, bgm_start=0.0,
                       color_normalize=True,
                       enhance=False, stabilize=False,
                       interpolate_60fps=False, upscale_target=None,
                       two_pass_loudnorm=True):
    """用 FFmpeg 把片段合成精華影片

    新增參數：
      bgm_path: 背景音樂檔案（wav/mp3/flac/m4a）。提供時：
                - 若 audio_mode in ("mute","bgm")：成品音軌 = 純 BGM
                - 若 audio_mode == "bgm+source" 或 "original": BGM 鋪底 + 來源音訊 duck -12dB
                - clip 長度會吸附到 BGM downbeat 間距，讓剪接點落在節拍上
      bgm_volume: BGM 增益倍率（1.0 = 原始，0.7 = -3dB，0.5 = -6dB）
      bgm_start: BGM 從第幾秒開始取（跳過前奏）
      color_normalize: 啟用 CLAHE + gamma 微調，讓跨來源色調一致
      enhance: Wave 5 C3 - 每段加 hqdn3d 輕降噪 + unsharp 銳化
      stabilize: Wave 6 C5 - 對晃度適中的段落做 vidstab 2-pass 穩定
      interpolate_60fps: Wave 6 C6 - 成品以 minterpolate 升至 60fps
      upscale_target: Wave 6 C4 - 若來源 < 720p，升頻到此目標（"720p"/"1080p"/"auto"/None）
      two_pass_loudnorm: Wave 4 F1 - 成品做 two-pass EBU R128 loudnorm（audio_mode!=mute 才跑）
    """
    ffmpeg = _get_ffmpeg()
    import tempfile, subprocess, math

    # ── Wave 1 A2 / Wave 3 B3: BGM 節拍分析 ──
    bgm_beat_grid = None  # np.ndarray cut_grid (每幾拍切一次的時間點)
    bgm_bpm = None
    bgm_energy_times = None  # Wave 4 E1
    bgm_energy_values = None
    if bgm_path and os.path.isfile(bgm_path):
        try:
            _, _cut_grid, _bpm, _bpc = _extract_bgm_beat_grid(bgm_path, clip_duration)
            if _cut_grid is not None and len(_cut_grid) >= 2:
                bgm_beat_grid = _cut_grid
                bgm_bpm = _bpm
                logging.info(f"bgm beat grid ready: {len(_cut_grid)} cut points, "
                             f"bpm={_bpm:.1f}")
        except Exception as _bgm_e:
            logging.warning(f"bgm beat analysis failed: {_bgm_e}")
        # Wave 4 E1: BGM 能量曲線（chorus-adaptive cut length 用）
        try:
            _etimes, _evals = _analyze_bgm_energy(bgm_path)
            if _etimes is not None and _evals is not None and len(_evals) >= 3:
                bgm_energy_times = _etimes
                bgm_energy_values = _evals
                _hi = float(np.sum(_evals >= 0.75)) / len(_evals)
                _lo = float(np.sum(_evals < 0.35)) / len(_evals)
                logging.info(f"bgm energy ready: {len(_evals)} frames, "
                             f"chorus≈{_hi*100:.0f}% verse≈{_lo*100:.0f}%")
        except Exception as _ee:
            logging.warning(f"bgm energy analysis failed: {_ee}")

    # ── Wave 3 B3: BGM 存在時縮短轉場時長（flash cut）──
    # 節拍切換用短轉場（100~150ms）比長 crossfade 更有節奏感；節拍本身會遮住切點
    _orig_xfade_dur = transition_dur
    if bgm_beat_grid is not None and transition == "crossfade" and transition_dur > 0.2:
        transition_dur = min(transition_dur, 0.15)
        logging.info(f"BGM present: xfade tightened from {_orig_xfade_dur:.2f}s → "
                     f"{transition_dur:.2f}s (flash cut)")

    logging.info(f"compile_highlight: {len(clips)} clips, clip_dur={clip_duration}, "
                 f"transition={transition}, resolution={resolution}, audio={audio_mode}, "
                 f"bgm={'ON' if bgm_path else 'OFF'}, color_norm={color_normalize}")

    res_map = {"720p": (1280, 720), "1080p": (1920, 1080),
                "720p_v": (720, 1280), "1080p_v": (1080, 1920)}
    tw, th = res_map.get(resolution, (1280, 720))

    # ── BGM 模式下的音訊策略 ──
    # 支援三種：
    #   "bgm"        → source 完全靜音，成品只有 BGM
    #   "bgm+source" → BGM 鋪底 + source 以 -12dB duck 混入
    #   else (original/mute) → 保留舊行為（不覆蓋 source 音軌）
    _has_bgm = bgm_path and os.path.isfile(bgm_path)
    _bgm_duck_source = (audio_mode == "bgm+source") and _has_bgm
    _bgm_only = (audio_mode == "bgm") and _has_bgm
    # 若使用者選 "bgm" 但沒提供 BGM，自動退回 mute
    if audio_mode in ("bgm", "bgm+source") and not _has_bgm:
        logging.warning(f"audio_mode={audio_mode} but no BGM supplied, fallback to mute")
        audio_mode = "mute"

    temp_dir = tempfile.mkdtemp()
    try:
        # Phase 1: 擷取每個片段並統一解析度（精確裁切到 clip_duration 秒）
        # 快取各來源影片的實際時長，避免重複探測
        _src_dur_cache = {}
        seg_files = []
        # ── Clip duration：BGM 優先（吸附到 downbeat 間距），否則用 rhythm pattern ──
        _rhythm_pattern = [1.0, 0.8, 1.15, 0.75, 1.2, 0.85, 1.1, 0.9]

        def _target_duration_for(idx, clips_total):
            """決定第 idx 個 clip 的目標時長（秒）。

            Wave 4 E1：若 BGM 能量 >= 0.75（副歌），縮短至約 1/2 拍（快切）；
            若 < 0.35（主歌/bridge），拉長至 1.4×（慢切）。
            """
            if idx == 0:
                return clip_duration  # 第 0 個保持標準長度做 hook
            if bgm_beat_grid is not None and len(bgm_beat_grid) >= idx + 1:
                # 用 BGM 的相鄰 cut_grid 間距
                diff = float(bgm_beat_grid[idx] - bgm_beat_grid[idx - 1])
                if 1.2 <= diff <= 5.0:
                    if bgm_energy_times is not None:
                        _t = float(bgm_beat_grid[idx - 1])
                        _e = _bgm_energy_at(bgm_energy_times, bgm_energy_values, _t)
                        if _e >= 0.75:
                            diff *= 0.5   # chorus: faster cuts
                        elif _e >= 0.55:
                            diff *= 0.75
                        elif _e < 0.35:
                            diff *= 1.4   # quiet verse: slower cuts
                        diff = max(1.5, min(5.0, diff))
                    return diff
            # fallback rhythm pattern
            return max(1.5, clip_duration * _rhythm_pattern[(idx - 1) % len(_rhythm_pattern)])

        for i, clip in enumerate(clips):
            seg_path = os.path.join(temp_dir, f"clip_{i:04d}.mp4")
            start = max(0, clip["time"] - 0.3)

            # 探測來源影片的實際時長，確保不會超出範圍（防止凍結畫面）
            src_video = clip["video"]
            if src_video not in _src_dur_cache:
                _src_dur_cache[src_video] = _probe_duration(ffmpeg, src_video)
            src_dur = _src_dur_cache[src_video]

            target_dur = _target_duration_for(i, len(clips))
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
            # ── 偵測靜態浮水印 / 頻道 logo，生成 delogo 前綴（來源座標）──
            _wm_boxes = _detect_watermark_regions(src_video)
            _wm_prefix = ""
            if _wm_boxes:
                _src_w_for_wm = None
                _src_h_for_wm = None
                try:
                    _cap_wm = cv2.VideoCapture(src_video)
                    _src_w_for_wm = int(_cap_wm.get(cv2.CAP_PROP_FRAME_WIDTH))
                    _src_h_for_wm = int(_cap_wm.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    _cap_wm.release()
                except Exception:
                    _src_w_for_wm = _src_h_for_wm = 0
                # ── Face-overlap veto：delogo 不可覆蓋到偵測到的人臉 ──
                _face_bboxes = []
                try:
                    _cap_f = cv2.VideoCapture(src_video)
                    _fw = int(_cap_f.get(cv2.CAP_PROP_FRAME_WIDTH))
                    _fh = int(_cap_f.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    _fps_f = _cap_f.get(cv2.CAP_PROP_FPS) or 25.0
                    _det = cv2.FaceDetectorYN.create(YUNET_MODEL, "", (_fw, _fh), 0.5, 0.3, 5000)
                    # 在 clip 區間的前、中、後三個時間點取樣
                    for _ft in (clip["time"], clip["time"] + actual_clip_dur / 2, clip["time"] + actual_clip_dur - 0.1):
                        _cap_f.set(cv2.CAP_PROP_POS_FRAMES, int(max(0, _ft) * _fps_f))
                        _ok, _fr = _cap_f.read()
                        if not _ok or _fr is None:
                            continue
                        _, _fcs = _det.detect(_fr)
                        if _fcs is None:
                            continue
                        for _f in _fcs:
                            # 輕微外擴頭髮/下巴；不要到頭頂以上太多
                            _pad_x = _f[2] * 0.08
                            _pad_y_top = _f[3] * 0.10
                            _pad_y_bot = _f[3] * 0.15
                            _face_bboxes.append((
                                _f[0] - _pad_x, _f[1] - _pad_y_top,
                                _f[0] + _f[2] + _pad_x, _f[1] + _f[3] + _pad_y_bot))
                    _cap_f.release()
                except Exception:
                    _face_bboxes = []

                def _overlap_ratio(wx, wy, ww, wh):
                    """回傳 watermark 與任一張臉的最大重疊面積比例（佔 watermark 面積）"""
                    w_area = max(1, ww * wh)
                    best = 0.0
                    for fx1, fy1, fx2, fy2 in _face_bboxes:
                        ix1 = max(wx, fx1); iy1 = max(wy, fy1)
                        ix2 = min(wx + ww, fx2); iy2 = min(wy + wh, fy2)
                        if ix2 > ix1 and iy2 > iy1:
                            r = ((ix2 - ix1) * (iy2 - iy1)) / w_area
                            if r > best: best = r
                    return best

                _wm_boxes_safe = []
                for _b in _wm_boxes:
                    _ratio = _overlap_ratio(*_b)
                    if _ratio > 0.25:
                        logging.info(f"  delogo box {_b} skipped: {_ratio*100:.0f}% overlaps face")
                        continue
                    _wm_boxes_safe.append(_b)
                _wm_boxes = tuple(_wm_boxes_safe)

                # ── 先嘗試 LaMA 局部 inpainting ──
                _lama_src = None
                if _wm_boxes and _get_lama() is not None:
                    _lama_out = os.path.join(temp_dir, f"clip_{i:04d}_lama.mp4")
                    _lama_ok = _lama_clean_segment(
                        src_video, start, actual_clip_dur,
                        list(_wm_boxes), _lama_out, ffmpeg=ffmpeg)
                    if _lama_ok and os.path.exists(_lama_out) and os.path.getsize(_lama_out) > 1000:
                        _lama_src = _lama_out

                if _lama_src:
                    # LaMA 成功：swap 來源，skip delogo 前綴
                    src_video = _lama_src
                    start = 0.0
                    clip = dict(clip); clip["time"] = 0.0
                    _wm_prefix = ""
                else:
                    # Fallback：FFmpeg delogo
                    _parts = []
                    for (wx, wy, ww, wh) in _wm_boxes:
                        wx = max(1, wx - 3)
                        wy = max(1, wy - 3)
                        ww += 6
                        wh += 6
                        if _src_w_for_wm and wx + ww >= _src_w_for_wm:
                            ww = _src_w_for_wm - wx - 1
                        if _src_h_for_wm and wy + wh >= _src_h_for_wm:
                            wh = _src_h_for_wm - wy - 1
                        if ww < 4 or wh < 4:
                            continue
                        _parts.append(f"delogo=x={wx}:y={wy}:w={ww}:h={wh}")
                    if _parts:
                        _wm_prefix = ",".join(_parts) + ","

            # 若來源是橫式 (W > H) 且目標是垂直 (tw < th)，用 face-TRACKED crop
            # (FACE_TRACK_V1) 逐幀偵測 + 雙向 smoothing → sendcmd 餵 FFmpeg 每幀 x/y
            # 否則用原本的 blurred background + fit-within 方案
            _face_crop_chain = None
            _face_sendcmd_path = None
            if tw < th:  # 目標是直式
                _track, _sw, _sh = _detect_face_track(
                    src_video, clip["time"], actual_clip_dur)
                if _track and _sw > _sh:
                    # 等比縮放到「剛好覆蓋」目標畫布
                    _scale = max(tw / _sw, th / _sh)
                    _nw = int(_sw * _scale)
                    _nh = int(_sh * _scale)
                    import tempfile as _tmf
                    _lines = []
                    for _rel_t, _cx, _cy in _track:
                        _cx_s = _cx * _scale
                        _cy_s = _cy * _scale
                        # 三分法構圖：水平置中；垂直置於上 1/3（經典電影/肖像取景）
                        # 把臉部中心放在 th/3 從頂，眼睛大約會落在上三分線上
                        _xc = max(0, min(_nw - tw, int(_cx_s - tw / 2)))
                        _yc = max(0, min(_nh - th, int(_cy_s - th / 3)))
                        _lines.append(f"{_rel_t:.3f} crop x {_xc};")
                        _lines.append(f"{_rel_t:.3f} crop y {_yc};")
                    _fd, _face_sendcmd_path = _tmf.mkstemp(
                        suffix=".sendcmd", prefix="crop_", dir=temp_dir)
                    with os.fdopen(_fd, 'w', encoding='utf-8') as _sf:
                        _sf.write("\n".join(_lines))
                    # FFmpeg filter 內的路徑需要把冒號跳脫
                    _fp = _face_sendcmd_path.replace("\\", "/").replace(":", r"\:")
                    _face_crop_chain = (
                        f"[0:v]{_wm_prefix}scale={_nw}:{_nh},"
                        f"sendcmd=f='{_fp}',crop={tw}:{th}:0:0,"
                        f"setpts=PTS-STARTPTS"
                    )

            if _face_crop_chain is not None:
                _base_chain = _face_crop_chain
            else:
                _base_chain = (
                    f"[0:v]{_wm_prefix}split=2[main][bg];"
                    f"[bg]scale={tw}:{th}:force_original_aspect_ratio=increase,"
                    f"crop={tw}:{th},boxblur=20:5[blur];"
                    f"[main]scale={tw}:{th}:force_original_aspect_ratio=decrease[fg];"
                    f"[blur][fg]overlay=(W-w)/2:(H-h)/2,setpts=PTS-STARTPTS"
                )
            # ── B2: 色彩 / 曝光正規化 ──
            # 讓不同來源（官攝 vs 電視台 vs fancam）的色調更一致。
            # 做法：
            #   - eq 濾鏡微調 gamma、saturation；避免過度。
            #   - 亮度修正：依來源亮度 vs 目標 50% 差異補償。
            # 為保守起見，只做 **輕度** 修正。太重會毀畫面。
            _color_chain = ""
            if color_normalize:
                # 量測來源中段一幀的平均亮度 (0~255)
                try:
                    _cap_b = cv2.VideoCapture(src_video)
                    _cap_b.set(cv2.CAP_PROP_POS_MSEC, (start + actual_clip_dur / 2) * 1000)
                    _okb, _frb = _cap_b.read()
                    _cap_b.release()
                    if _okb and _frb is not None:
                        _mean_y = float(cv2.cvtColor(_frb, cv2.COLOR_BGR2YUV)[:, :, 0].mean())
                        # 目標 Y = 118（稍亮於中灰，偏向人像標準）
                        # brightness delta in [-0.05, +0.05]
                        _br = max(-0.05, min(0.05, (118.0 - _mean_y) / 1200.0))
                    else:
                        _br = 0.0
                except Exception:
                    _br = 0.0
                # eq: contrast=1.03, brightness=_br, saturation=1.05, gamma=1.00
                _color_chain = f",eq=contrast=1.03:brightness={_br:.3f}:saturation=1.05"

            # Wave 5 C3: 輕降噪 + 銳化（UI toggle）
            _enhance_chain = ""
            if enhance:
                _enhance_chain = ",hqdn3d=1.5:1.5:6:6,unsharp=5:5:0.6:5:5:0.0"

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
                vf_complex = _base_chain + _color_chain + _enhance_chain + drawtext + "[outv]"
            else:
                vf_complex = _base_chain + _color_chain + _enhance_chain + "[outv]"
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
            if _face_sendcmd_path:
                try: os.remove(_face_sendcmd_path)
                except Exception: pass
            if os.path.isfile(seg_path) and os.path.getsize(seg_path) > 0:
                # 驗證擷取出的片段沒有凍結（檢查實際長度）
                seg_dur = _probe_duration(ffmpeg, seg_path)
                if seg_dur and seg_dur < 0.5:
                    logging.warning(f"  clip {i}: too short ({seg_dur:.1f}s), skip")
                    os.remove(seg_path)
                else:
                    # Wave 6 C5: 對晃度適中（A4 shake 5~9）的段落做 vidstab 2-pass
                    if stabilize:
                        _shk = clip.get("_shake")
                        if _shk is not None and 5.0 <= float(_shk) <= 9.0:
                            _trf_path = seg_path + ".trf"
                            _stab_out = seg_path + ".stab.mp4"
                            try:
                                _rc1 = subprocess.run(
                                    [ffmpeg, "-y", "-i", seg_path,
                                     "-vf", f"vidstabdetect=shakiness=5:accuracy=15:result={_trf_path}",
                                     "-f", "null", "-"],
                                    capture_output=True, timeout=180)
                                if _rc1.returncode == 0 and os.path.isfile(_trf_path):
                                    _rc2 = subprocess.run(
                                        [ffmpeg, "-y", "-i", seg_path,
                                         "-vf", f"vidstabtransform=input={_trf_path}:smoothing=30:crop=keep,unsharp=5:5:0.8:3:3:0.4",
                                         "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                                         "-c:a", "copy", "-pix_fmt", "yuv420p",
                                         _stab_out],
                                        capture_output=True, timeout=180)
                                    if _rc2.returncode == 0 and os.path.isfile(_stab_out) and os.path.getsize(_stab_out) > 1024:
                                        os.replace(_stab_out, seg_path)
                                        logging.info(f"  clip {i}: vidstab applied (shake={_shk:.2f})")
                            except Exception as _vse:
                                logging.warning(f"  clip {i}: vidstab failed: {_vse}")
                            finally:
                                for _p in (_trf_path, _stab_out):
                                    if os.path.isfile(_p):
                                        try: os.remove(_p)
                                        except Exception: pass
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

        # ── 封面圖：在所有片段之前插入一張 N 秒的靜止畫面 ──
        # 用途：TikTok / YouTube 會取「影片第一幀」當自動縮圖，放一張漂亮的照片
        # 當封面可以大幅提高點擊率。
        _pending_cover_seg = None
        if cover_photo and os.path.isfile(cover_photo):
            try:
                cover_seg = os.path.join(temp_dir, "clip_cover.mp4")
                # 相同的 scale+pad 邏輯跟其他 seg 一致，確保 xfade 串接不出錯
                vf_cover = (f"scale={tw}:{th}:force_original_aspect_ratio=increase,"
                            f"crop={tw}:{th},setsar=1,fps=30,format=yuv420p")
                # 單一幀模式：< 0.1s 一律當作「只要封面不要播放」，用單幀
                # 單幀 @ 30fps ≈ 33ms，觀眾肉眼幾乎無感，但仍是影片的第一幀
                # → TikTok / YouTube 自動抓首幀當縮圖時會用到這張
                _cd = float(cover_duration) if cover_duration is not None else 0.0
                _single_frame = (_cd < 0.1)

                cmd_cover = [
                    ffmpeg, "-y",
                    "-loop", "1",
                    "-framerate", "30",
                    "-i", cover_photo,
                ]
                if _single_frame:
                    # 若主影片有聲軌，封面也要有（靜音）以便 concat 一致
                    if has_audio:
                        cmd_cover += [
                            "-f", "lavfi",
                            "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
                        ]
                    cmd_cover += [
                        "-frames:v", "1",
                        "-vf", vf_cover,
                        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                        "-pix_fmt", "yuv420p",
                        "-r", "30",
                    ]
                    if has_audio:
                        # 單幀影片的音軌長度 = 1 幀 ≈ 1/30s
                        cmd_cover += ["-c:a", "aac", "-b:a", "128k", "-shortest"]
                    else:
                        cmd_cover += ["-an"]
                else:
                    if has_audio:
                        cmd_cover += [
                            "-f", "lavfi",
                            "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
                        ]
                    cmd_cover += [
                        "-t", f"{_cd:.2f}",
                        "-vf", vf_cover,
                        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                        "-pix_fmt", "yuv420p",
                        "-r", "30",
                    ]
                    if has_audio:
                        cmd_cover += ["-c:a", "aac", "-b:a", "128k", "-shortest"]
                    else:
                        cmd_cover += ["-an"]
                cmd_cover.append(cover_seg)

                rc = subprocess.run(cmd_cover, capture_output=True, timeout=60)
                if rc.returncode == 0 and os.path.isfile(cover_seg) and os.path.getsize(cover_seg) > 0:
                    # 不放進 seg_files（xfade 會過濾 < 0.5s 的片段，單幀會被丟掉）
                    # 改成記下來，等主體影片合成完後再做一次 concat 把它接在前面
                    _pending_cover_seg = cover_seg
                    _mode_label = "1-frame (~33ms)" if _single_frame else f"{_cd:.2f}s"
                    logging.info(f"cover photo built (will prepend post-concat): "
                                 f"{os.path.basename(cover_photo)} "
                                 f"({_mode_label}, {os.path.getsize(cover_seg)/1024:.0f}KB)")
                else:
                    _pending_cover_seg = None
                    logging.warning(f"cover photo build failed rc={rc.returncode}: "
                                    f"{rc.stderr[-300:] if rc.stderr else b''}")
            except Exception as _cover_e:
                _pending_cover_seg = None
                logging.warning(f"cover photo skipped: {_cover_e}")
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

        # ── 封面後置合併：把單獨編碼好的 cover_seg 接在主體前面 ──
        # 這個步驟獨立於 xfade / concat 之外，因此單幀封面不會被 xfade 的
        # duration 過濾器 (< 0.5s) 丟掉。
        if _pending_cover_seg and os.path.isfile(output_path):
            try:
                _tmp_merged = output_path + ".withcover.mp4"
                _list_cover = os.path.join(temp_dir, "list_cover.txt")
                with open(_list_cover, "w", encoding="utf-8") as _lf:
                    for _sp in (_pending_cover_seg, output_path):
                        _safe = _sp.replace("\\", "/").replace("'", "'\\''")
                        _lf.write(f"file '{_safe}'\n")
                _cmd_merge = [
                    ffmpeg, "-y",
                    "-f", "concat", "-safe", "0",
                    "-i", _list_cover,
                    "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                    "-r", "30", "-pix_fmt", "yuv420p",
                ]
                if has_audio:
                    _cmd_merge += ["-c:a", "aac", "-b:a", "128k", "-ar", "44100"]
                else:
                    _cmd_merge += ["-an"]
                _cmd_merge.append(_tmp_merged)
                _rm = subprocess.run(_cmd_merge, capture_output=True, timeout=300)
                if _rm.returncode == 0 and os.path.isfile(_tmp_merged) and os.path.getsize(_tmp_merged) > 1024:
                    os.replace(_tmp_merged, output_path)
                    logging.info(f"cover prepended post-concat: "
                                 f"final {os.path.getsize(output_path)/1024/1024:.1f}MB")
                else:
                    logging.warning(f"cover post-concat failed rc={_rm.returncode}: "
                                    f"{_rm.stderr[-300:] if _rm.stderr else b''}")
                    if os.path.isfile(_tmp_merged):
                        try: os.remove(_tmp_merged)
                        except Exception: pass
            except Exception as _mce:
                logging.warning(f"cover post-concat exception: {_mce}")

        # ── Wave 3 D1: BGM overlay（鋪底 + 可選 duck）──
        # 在所有其他處理完成之後，把 BGM 疊到最終 output_path 上。
        # 模式：
        #   _bgm_only      → 音軌 = BGM（loudnorm）；來源音訊丟棄
        #   _bgm_duck_source → 音軌 = BGM + 來源 -12dB（sidechain 壓縮可選，先用簡單 mix）
        if _has_bgm and os.path.isfile(output_path) and (_bgm_only or _bgm_duck_source):
            try:
                _vid_dur = _probe_duration(ffmpeg, output_path) or 0
                _tmp_bgm = output_path + ".bgm.mp4"
                # BGM loudnorm 目標：-14 LUFS（跟 source 同標準）
                _lI = (loudnorm_params or {}).get("I", -14)
                _lTP = (loudnorm_params or {}).get("TP", -1.5)
                _lLRA = (loudnorm_params or {}).get("LRA", 11)

                # 淡入 0.3s / 淡出 0.5s，避免突兀開場/結尾
                _fade_out_start = max(0, _vid_dur - 0.5) if _vid_dur else 0
                _vol_db = 20 * math.log10(max(bgm_volume, 0.01)) if bgm_volume > 0 else 0

                if _bgm_only:
                    # BGM 獨家：忽略 output 的音軌，只用 BGM
                    _bgm_af = (
                        f"atrim=start={float(bgm_start):.2f},"
                        f"asetpts=PTS-STARTPTS,"
                        f"afade=t=in:st=0:d=0.3,"
                        + (f"afade=t=out:st={_fade_out_start:.2f}:d=0.5,"
                           if _fade_out_start > 0 else "")
                        + f"loudnorm=I={_lI}:TP={_lTP}:LRA={_lLRA},"
                        f"volume={_vol_db:+.1f}dB"
                    )
                    _cmd_bgm = [
                        ffmpeg, "-y",
                        "-i", output_path,
                        "-i", bgm_path,
                        "-filter_complex",
                        f"[1:a]{_bgm_af}[aout]",
                        "-map", "0:v",
                        "-map", "[aout]",
                        "-c:v", "copy",
                        "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
                        "-shortest",
                        _tmp_bgm,
                    ]
                else:
                    # BGM + 來源混音（來源 ducking 到 -12dB）
                    _bgm_af = (
                        f"[1:a]atrim=start={float(bgm_start):.2f},"
                        f"asetpts=PTS-STARTPTS,"
                        f"afade=t=in:st=0:d=0.3,"
                        + (f"afade=t=out:st={_fade_out_start:.2f}:d=0.5,"
                           if _fade_out_start > 0 else "")
                        + f"loudnorm=I={_lI}:TP={_lTP}:LRA={_lLRA},"
                        f"volume={_vol_db:+.1f}dB[bgm]"
                    )
                    _src_af = "[0:a]volume=-12dB,aresample=async=1:first_pts=0[src]"
                    _mix = "[bgm][src]amix=inputs=2:duration=shortest:dropout_transition=0[aout]"
                    _cmd_bgm = [
                        ffmpeg, "-y",
                        "-i", output_path,
                        "-i", bgm_path,
                        "-filter_complex",
                        f"{_bgm_af};{_src_af};{_mix}",
                        "-map", "0:v",
                        "-map", "[aout]",
                        "-c:v", "copy",
                        "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
                        "-shortest",
                        _tmp_bgm,
                    ]

                _rb = subprocess.run(_cmd_bgm, capture_output=True, timeout=360)
                if (_rb.returncode == 0 and os.path.isfile(_tmp_bgm)
                        and os.path.getsize(_tmp_bgm) > 1024):
                    os.replace(_tmp_bgm, output_path)
                    logging.info(f"BGM overlay done ({'only' if _bgm_only else 'duck+mix'}): "
                                 f"{os.path.getsize(output_path)/1024/1024:.1f}MB")
                else:
                    _serr = (_rb.stderr[-500:] if _rb.stderr else b'').decode(errors='replace')
                    logging.warning(f"BGM overlay failed rc={_rb.returncode}: {_serr}")
                    if os.path.isfile(_tmp_bgm):
                        try: os.remove(_tmp_bgm)
                        except Exception: pass
            except Exception as _bgm_e:
                logging.warning(f"BGM overlay exception: {_bgm_e}")

        # ── Wave 6 C4: Real-ESRGAN / Lanczos 升頻（若來源解析度 < target）──
        if upscale_target and os.path.isfile(output_path):
            try:
                _upscale_final_video(output_path, upscale_target, ffmpeg=ffmpeg,
                                      has_audio=has_audio)
            except Exception as _up_e:
                logging.warning(f"upscale skipped: {_up_e}")

        # ── Wave 6 C6: minterpolate 升至 60fps ──
        if interpolate_60fps and os.path.isfile(output_path):
            try:
                _interpolate_to_60fps(output_path, ffmpeg=ffmpeg, has_audio=has_audio)
            except Exception as _ie:
                logging.warning(f"interpolate_60fps skipped: {_ie}")

        # ── Wave 4 F1: 成品整片 two-pass loudnorm（audio_mode != mute 才跑）──
        if two_pass_loudnorm and os.path.isfile(output_path) and audio_mode != "mute":
            try:
                _two_pass_loudnorm(output_path)
            except Exception as _ln_e:
                logging.warning(f"two_pass_loudnorm skipped: {_ln_e}")

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
    audio_mode = data.get("audio_mode", "original")  # original / mute / bgm / bgm+source
    strategy = data.get("strategy", "balanced")
    clip_duration = float(data.get("clip_duration", 3))
    total_duration = float(data.get("total_duration", 30))
    max_per_video = int(data.get("max_per_video", 5))
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    resolution = data.get("resolution", "720p")

    # BGM 選項
    bgm_path = _resolve_bgm_path(data.get("bgm_path") or data.get("bgm"))
    try:
        bgm_volume = float(data.get("bgm_volume", 1.0))
    except Exception:
        bgm_volume = 1.0
    try:
        bgm_start = float(data.get("bgm_start", 0.0))
    except Exception:
        bgm_start = 0.0

    # Wave 5/6 進階選項
    enhance = bool(data.get("enhance", False))
    stabilize = bool(data.get("stabilize", False))
    interpolate_60fps = bool(data.get("interpolate_60fps", False))
    upscale_target = (data.get("upscale_target") or "").strip() or None
    two_pass_loudnorm_opt = bool(data.get("two_pass_loudnorm", True))

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
                                            prefer_vertical=resolution.endswith("_v"),
                                            bgm_path=bgm_path)
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
                bgm_path=bgm_path, bgm_volume=bgm_volume, bgm_start=bgm_start,
                enhance=enhance, stabilize=stabilize,
                interpolate_60fps=interpolate_60fps, upscale_target=upscale_target,
                two_pass_loudnorm=two_pass_loudnorm_opt,
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

# 任務歷史（保留最近完成/失敗的任務結果，供重連 + 頁面重開用）
# 持久化到 disk（JSON），Flask 重啟或 deploy 不會消失
auto_video_history = {}  # task_id -> {status, label, result_data, ts}
_AV_HISTORY_MAX = 50
_AV_HISTORY_CUTOFF_SEC = 7 * 24 * 3600  # 7 天後才清
_AV_HISTORY_LOCK = threading.Lock()
_AV_HISTORY_PATH = os.path.join(APP_DATA_DIR, "auto_video_history.json")


def _av_load_history():
    """啟動時從 disk 載入歷史。已不存在的輸出檔案會被跳過。"""
    global auto_video_history
    if not os.path.isfile(_AV_HISTORY_PATH):
        return
    try:
        with open(_AV_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        cutoff = time.time() - _AV_HISTORY_CUTOFF_SEC
        cleaned = {}
        for tid, v in data.items():
            if not isinstance(v, dict):
                continue
            if v.get("ts", 0) < cutoff:
                continue
            # Flask 重啟後，任何 "running" 都已是 orphan（執行緒已死）→ 轉成 error
            if v.get("status") == "running":
                v = dict(v)
                v["status"] = "error"
                v["data"] = {"message": "Flask 重啟，任務已中斷"}
            # done 狀態但輸出檔已被刪 → 跳過（避免 UI 出現死連結）
            if v.get("status") == "done":
                rel = (v.get("data") or {}).get("rel_path")
                if rel:
                    full = os.path.join(YT_ROOT, rel.replace("/", os.sep))
                    if not os.path.isfile(full):
                        continue
            cleaned[tid] = v
        auto_video_history = cleaned
        logging.info(f"auto-video history loaded: {len(cleaned)} entries from {_AV_HISTORY_PATH}")
    except Exception as e:
        logging.warning(f"auto-video history load failed: {e}")


def _av_persist_history_nolock():
    """把記憶體 dict 寫到 disk（呼叫者需已拿 lock）。"""
    try:
        tmp = _AV_HISTORY_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(auto_video_history, f, ensure_ascii=False)
        os.replace(tmp, _AV_HISTORY_PATH)
    except Exception as e:
        logging.warning(f"auto-video history persist failed: {e}")


def _av_save_history(task_id, status, label, data=None):
    """儲存任務最終狀態到歷史 + 持久化到 disk"""
    with _AV_HISTORY_LOCK:
        auto_video_history[task_id] = {
            "status": status,  # "running" | "preview" | "done" | "error" | "cancelled"
            "label": label,
            "data": data or {},
            "ts": time.time(),
        }
        # 時間 cutoff：> 7 天的清掉
        cutoff = time.time() - _AV_HISTORY_CUTOFF_SEC
        old = [k for k, v in auto_video_history.items() if v.get("ts", 0) < cutoff]
        for k in old:
            auto_video_history.pop(k, None)
        # 數量 cap：超過 _AV_HISTORY_MAX 就丟掉最舊的
        if len(auto_video_history) > _AV_HISTORY_MAX:
            items = sorted(auto_video_history.items(), key=lambda kv: kv[1].get("ts", 0))
            drop_n = len(auto_video_history) - _AV_HISTORY_MAX
            for k, _ in items[:drop_n]:
                auto_video_history.pop(k, None)
        _av_persist_history_nolock()


# 啟動時載入歷史
_av_load_history()


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
    # 衝突名字：person 可能是 'Chaeyoung_TWICE' — 搜尋詞用 "TWICE Chaeyoung"
    _disp_p, _grp_p = _split_person_key(person)
    _base_q = (f"{_grp_p} {_disp_p}".strip() if _grp_p else _disp_p) if _disp_p != person else person
    search_keyword = f"{_base_q} {extra_keyword}".strip() if extra_keyword else _base_q
    max_videos = int(data.get("max_videos", 5))
    clip_duration = float(data.get("clip_duration", 3))
    total_duration = float(data.get("total_duration", 30))
    strategy = data.get("strategy", "balanced")
    transition = data.get("transition", "crossfade")
    transition_dur = float(data.get("transition_dur", 0.5))
    resolution = data.get("resolution", "720p_v")  # 預設直式
    audio_mode = data.get("audio_mode", "original")
    platform = data.get("platform", "tiktok")
    # 複選支援：如果傳了 platforms=[...]（長度≥2）就視為子集合合併
    _plat_raw = data.get("platforms") or []
    platforms = [p for p in _plat_raw if isinstance(p, str) and p in ("tiktok","youtube","yt_shorts","ig_reels")] if isinstance(_plat_raw, list) else []
    if len(platforms) >= 2:
        platform = "custom"  # pipeline 內會檢查 platforms 走子集合分支
    elif len(platforms) == 1:
        platform = platforms[0]
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
    cover_photo = (data.get("cover_photo") or "").strip() or None
    try:
        cover_duration = float(data.get("cover_duration", 1.0))
    except Exception:
        cover_duration = 1.0

    # BGM 選項
    bgm_path = _resolve_bgm_path(data.get("bgm_path") or data.get("bgm"))
    try:
        bgm_volume = float(data.get("bgm_volume", 1.0))
    except Exception:
        bgm_volume = 1.0
    try:
        bgm_start = float(data.get("bgm_start", 0.0))
    except Exception:
        bgm_start = 0.0

    # Wave 5/6 進階選項
    enhance = bool(data.get("enhance", False))
    stabilize = bool(data.get("stabilize", False))
    interpolate_60fps = bool(data.get("interpolate_60fps", False))
    upscale_target = (data.get("upscale_target") or "").strip() or None
    two_pass_loudnorm_opt = bool(data.get("two_pass_loudnorm", True))

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

    cancel_event = threading.Event()
    with auto_video_tasks_lock:
        auto_video_tasks[task_id] = {
            "queue": q, "status": "running",
            "confirm_event": confirm_event,
            "confirm_data": confirm_data,
            "cancel_event": cancel_event,
            "label": task_label,
            "started_at": time.time(),
        }
    _av_save_history(task_id, "running", task_label)

    def worker():
        try:
            _auto_video_pipeline(
                q, resolved_person, search_keyword, max_videos,
                clip_duration, total_duration, strategy, transition,
                transition_dur, resolution, audio_mode, platform,
                platforms=platforms,
                video_type=video_type, source=source,
                preview=preview, confirm_event=confirm_event,
                confirm_data=confirm_data,
                task_id=task_id, task_label=task_label,
                loudnorm_params=loudnorm_params,
                cover_photo=cover_photo, cover_duration=cover_duration,
                cancel_event=cancel_event,
                bgm_path=bgm_path, bgm_volume=bgm_volume, bgm_start=bgm_start,
                enhance=enhance, stabilize=stabilize,
                interpolate_60fps=interpolate_60fps,
                upscale_target=upscale_target,
                two_pass_loudnorm=two_pass_loudnorm_opt,
            )
        except _AvCancelled:
            logging.info(f"auto-video task {task_id} cancelled by user")
            q.put({"type": "cancelled", "message": "已中止"})
            q.put({"type": "error", "message": "已中止"})
            _av_save_history(task_id, "cancelled", task_label, {"message": "使用者中止"})
        except Exception as e:
            import traceback
            logging.error(f"auto-video error: {traceback.format_exc()}")
            q.put({"type": "error", "message": str(e)})
            _av_save_history(task_id, "error", task_label, {"message": str(e)})

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return jsonify({"task_id": task_id, "label": task_label})


class _AvCancelled(Exception):
    """使用者中止一鍵影片任務（pipeline 內部 checkpoint 拋出）。"""
    pass


def _auto_video_pipeline(q, person, search_keyword, max_videos,
                          clip_duration, total_duration, strategy,
                          transition, transition_dur, resolution,
                          audio_mode, platform, platforms=None,
                          video_type="all",
                          source="online", preview=False,
                          confirm_event=None, confirm_data=None,
                          task_id=None, task_label=None,
                          loudnorm_params=None,
                          cover_photo=None, cover_duration=1.0,
                          cancel_event=None,
                          bgm_path=None, bgm_volume=1.0, bgm_start=0.0,
                          enhance=False, stabilize=False,
                          interpolate_60fps=False, upscale_target=None,
                          two_pass_loudnorm=True):
    """完整 pipeline：搜尋 → 下載 → 擷取 → 精華剪輯（或本地影片 → 擷取 → 精華剪輯）"""
    import subprocess
    import random as _random

    def _check_cancel():
        """檢查是否被使用者中止；若中止則拋出 _AvCancelled。"""
        if cancel_event is not None and cancel_event.is_set():
            raise _AvCancelled()

    # ── 解析使用者挑選的封面圖路徑 ──
    # cover_photo 可以是：
    #   a) "__auto__" → 自動挑 person 資料夾第一張照片
    #   b) 檔名（例如 "photo_001.jpg"，從前端 /api/images/<person> 回傳的 filename）
    #   c) /photos/<person>/<file> 形式的 URL
    #   d) 絕對路徑（不建議，但支援）
    # 一律收斂到實際檔案系統路徑，且限制在 DOWNLOAD_ROOT/<person>/ 底下以防路徑穿越
    #
    # cover_duration: 允許 0 或極小值 → 單一幀模式（影片幾乎不顯示，只影響平台自動縮圖）
    _cover_photo_path = None
    try:
        _cover_dur_raw = float(cover_duration) if cover_duration is not None else 0.0
    except Exception:
        _cover_dur_raw = 0.0
    # 負值視為 0；超過 5s 截斷為 5s
    _cover_dur = max(0.0, min(5.0, _cover_dur_raw))
    # 小於 0.1s 一律走「單一幀」模式
    _cover_single_frame = (_cover_dur < 0.1)

    def _auto_pick_first_photo(_person):
        # 從該成員的照片資料夾隨機挑一張（每次生成都不同，避免同一團成員封面都長一樣）
        try:
            _folder = os.path.join(DOWNLOAD_ROOT, sanitize_name(_person))
            if not os.path.isdir(_folder):
                return None
            _exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
            _cands = [f for f in os.listdir(_folder)
                      if os.path.splitext(f)[1].lower() in _exts
                      and not f.startswith(".")]
            if not _cands:
                return None
            _chosen = _random.choice(_cands)
            return os.path.join(_folder, _chosen)
        except Exception:
            return None

    def _auto_pick_best_cover(_person, max_candidates=30):
        """Wave 4 C7: 從該成員照片資料夾挑「最適合當封面」的一張。

        評分：face_area_ratio × centering × sharpness。
        失敗或沒有可判讀的人臉時 fallback 到 _auto_pick_first_photo。
        """
        try:
            _folder = os.path.join(DOWNLOAD_ROOT, sanitize_name(_person))
            if not os.path.isdir(_folder):
                return None
            _exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
            _cands = [f for f in os.listdir(_folder)
                      if os.path.splitext(f)[1].lower() in _exts
                      and not f.startswith(".")]
            if not _cands:
                return None
            # 取檔案大小前 max_candidates 張（通常高品質 = 大檔）
            _cands.sort(key=lambda n: -os.path.getsize(os.path.join(_folder, n)))
            _cands = _cands[:max_candidates]

            best = None
            best_score = -1.0
            for _fn in _cands:
                _fp = os.path.join(_folder, _fn)
                try:
                    _img = cv2.imread(_fp)
                    if _img is None:
                        continue
                    _h, _w = _img.shape[:2]
                    if _h < 300 or _w < 300:
                        continue
                    _img_area = float(_h * _w)
                    _det = cv2.FaceDetectorYN.create(YUNET_MODEL, "", (_w, _h), 0.6, 0.3, 5000)
                    _, _faces = _det.detect(_img)
                    if _faces is None or len(_faces) == 0:
                        continue
                    # 取最大臉
                    _faces_sorted = sorted(_faces, key=lambda f: f[2] * f[3], reverse=True)
                    _f = _faces_sorted[0]
                    _fx, _fy, _fw, _fh = float(_f[0]), float(_f[1]), float(_f[2]), float(_f[3])
                    # 1) face area ratio（0.03~0.35 最佳；過小=遠景，過大=過近）
                    _far = (_fw * _fh) / _img_area
                    if _far < 0.02 or _far > 0.45:
                        continue
                    _area_score = 1.0 - abs(_far - 0.12) / 0.12
                    _area_score = max(0.1, min(1.0, _area_score))
                    # 2) centering（face center 距離畫面中心）
                    _cx = (_fx + _fw / 2) / _w
                    _cy = (_fy + _fh / 2) / _h
                    _center_dist = ((_cx - 0.5) ** 2 + (_cy - 0.4) ** 2) ** 0.5
                    _center_score = max(0.2, 1.0 - _center_dist * 1.5)
                    # 3) sharpness（face 區域 Laplacian variance）
                    _x1 = max(0, int(_fx)); _y1 = max(0, int(_fy))
                    _x2 = min(_w, int(_fx + _fw)); _y2 = min(_h, int(_fy + _fh))
                    if _x2 <= _x1 or _y2 <= _y1:
                        continue
                    _roi = cv2.cvtColor(_img[_y1:_y2, _x1:_x2], cv2.COLOR_BGR2GRAY)
                    _lap_var = float(cv2.Laplacian(_roi, cv2.CV_64F).var())
                    _sharp_score = min(1.0, _lap_var / 300.0)  # 300 以上都算很銳利
                    if _sharp_score < 0.1:
                        continue
                    # 多臉扣分（solo 封面較好）
                    _face_cnt_penalty = 1.0 if len(_faces) == 1 else 0.75
                    _score = _area_score * _center_score * _sharp_score * _face_cnt_penalty
                    if _score > best_score:
                        best_score = _score
                        best = _fp
                except Exception:
                    continue
            if best:
                logging.info(f"cover pick (smart): {os.path.basename(best)} "
                             f"score={best_score:.3f}")
                return best
            # fallback
            return _auto_pick_first_photo(_person)
        except Exception as _ace:
            logging.warning(f"auto_pick_best_cover failed: {_ace}; fallback random")
            return _auto_pick_first_photo(_person)

    if cover_photo:
        try:
            _cp_raw = str(cover_photo).strip()
            if _cp_raw == "__auto__":
                _cp_candidate = _auto_pick_best_cover(person)
                if _cp_candidate is None:
                    logging.info(f"cover_photo __auto__: no photos for {person}, skipping cover")
                else:
                    logging.info(f"cover_photo __auto__ picked: {_cp_candidate}")
            elif _cp_raw.startswith("/photos/"):
                # URL 形式：/photos/<celeb>/<file>
                _cp_rel = _cp_raw[len("/photos/"):]
                _cp_celeb, _, _cp_file = _cp_rel.partition("/")
                _cp_candidate = os.path.join(DOWNLOAD_ROOT, _cp_celeb, _cp_file)
            elif os.path.isabs(_cp_raw):
                _cp_candidate = _cp_raw
            else:
                # 只給檔名 → 放在當前 person 資料夾
                _cp_candidate = os.path.join(DOWNLOAD_ROOT, sanitize_name(person),
                                              os.path.basename(_cp_raw))
            if _cp_candidate:
                _cp_norm = os.path.normpath(_cp_candidate)
                _root_norm = os.path.normpath(DOWNLOAD_ROOT)
                if _cp_norm.startswith(_root_norm) and os.path.isfile(_cp_norm):
                    _cover_photo_path = _cp_norm
                    _mode_label = "1-frame" if _cover_single_frame else f"{_cover_dur:.2f}s"
                    logging.info(f"cover_photo resolved: {_cp_norm} ({_mode_label})")
                else:
                    logging.warning(f"cover_photo rejected (outside DOWNLOAD_ROOT or missing): {_cp_raw}")
        except Exception as _cpe:
            logging.warning(f"cover_photo parse failed: {_cpe}")

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
            "max_duration": 360,
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
        # 依人物 × 類型輪替：每次跑取連續 4 個變體（wrap-around），下次前進 2 位
        # 保證連續兩次執行拿到的變體組至少有 2 個不同
        n = min(4, len(variants))
        idx = _get_rotation_idx(person, f"qv_{vtype or 'all'}")
        start = (idx * 2) % len(variants)
        picked = [variants[(start + i) % len(variants)] for i in range(n)]
        _bump_rotation_idx(person, f"qv_{vtype or 'all'}")
        logging.info(f"query rotation for {person}/{vtype}: idx={idx} → {picked}")
        return picked

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

        _check_cancel()
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
        if platform == "custom" and platforms:
            platform_label = " + ".join(PLATFORM_LABELS.get(p, p) for p in platforms)
        else:
            platform_label = PLATFORM_LABELS.get(platform, platform)
        _check_cancel()
        q.put({"type": "progress", "phase": "search", "percent": 0,
               "message": f"🔍 搜尋 {platform_label}: {actual_keyword}（目標 {search_count} 部，已有 {len(existing_ids)} 部）..."})

        if platform == "custom" and platforms:
            # 子集合合併：把 search_count 平均分配到選中的平台
            _n = max(1, len(platforms))
            _per = max(1, search_count // _n)
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
                   "message": f"🔍 子集合搜尋 {platform_label}（每平台 ~{_per} 部）..."})
            _pct = 1
            for plat in platforms:
                _check_cancel()
                _pct += 1
                try:
                    if plat == "tiktok":
                        _b = _tiktok_search(actual_keyword, _per)
                    elif plat == "yt_shorts":
                        _b = _youtube_multi_search(search_keyword, video_type, _per, q, shorts=True)
                    elif plat == "youtube":
                        _b = _youtube_multi_search(search_keyword, video_type, _per, q, shorts=False)
                    elif plat == "ig_reels":
                        _b = []  # IG 需登入，子集合合併時預設跳過
                        logging.info("custom-platform: IG Reels skipped (requires login)")
                    else:
                        _b = []
                    q.put({"type": "progress", "phase": "search", "percent": _pct,
                           "message": f"  {PLATFORM_LABELS.get(plat, plat)}：{_merge(_b, plat)} 部"})
                except Exception as e:
                    logging.warning(f"custom-platform: {plat} search failed: {e}")

            search_results = merged_results
            logging.info(f"custom-platform merged {len(search_results)} unique videos from {platforms}")

        elif platform == "all":
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
                        # 縮圖門檻比 extract 稍低 (-0.05)：縮圖畫面通常比 extract 雜
                        _thumb_thr = max(0.20, EXTRACT_CONFIG["similarity_threshold"] - 0.05)
                        status, score = _thumbnail_face_match(
                            img, _ref_for_thumb, _thumb_det, _thumb_rec,
                            threshold=_thumb_thr, neg_embeddings=_neg_for_thumb)
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

        # 上限 4：YouTube 對同一 IP 開太多連線會 rate-limit（HTTP 429 / bot 防護），
        # 尤其多人同時跑（6 人 × 30 影片 = 180 個 yt-dlp 一起連）幾乎必炸。
        # 可透過環境變數 AUTO_VIDEO_PARALLEL_DL 覆寫。
        try:
            _cap = int(os.environ.get("AUTO_VIDEO_PARALLEL_DL", "4"))
        except Exception:
            _cap = 4
        PARALLEL_DL = max(1, min(len(search_results), _cap))

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
            # YouTube 自 2024 末起對匿名 IP 強制要求 cookies（"Sign in to confirm you're not a bot"）。
            # 繞法：指定多個 player_client，讓 yt-dlp 從「不需登入」的 client 合出可用 format。
            # - mweb: mobile web, 有 video-only + audio-only 的分離 stream
            # - android: 預合併的 mp4（360p / 720p）作為 fallback
            # - web_safari: Safari UA 的 client，format pool 不一樣
            # 可透過環境變數 YT_PLAYER_CLIENTS 覆寫。
            _pc = os.environ.get("YT_PLAYER_CLIENTS", "mweb,android,web_safari")
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "--no-playlist", "--windows-filenames", "--restrict-filenames",
                "--file-access-retries", "10",
                # bv*+ba/best: 任意 best video + best audio，高度限制移除（mweb 可能沒有 720p 以下）
                "-f", "bv*[height<=720]+ba/best[height<=720]/bv*+ba/best",
                "--merge-output-format", "mp4",
                "-o", out_tpl, "--newline", "--no-warnings",
                "--write-info-json",  # 寫 {id}.info.json 讓後續擷取文案用
                "--extractor-args", f"youtube:player_client={_pc}",
            ]
            if FFMPEG_LOCATION:
                cmd += ["--ffmpeg-location", FFMPEG_LOCATION]
            cmd.append(url)

            logging.info(f"auto-video dl[{vi}]: {url}")

            _dl_ok = False
            for _attempt in range(2):
                _last_lines = []  # 保留最後幾行 yt-dlp 輸出供 debug
                try:
                    proc = subprocess.Popen(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        cwd=dl_dir,
                        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                    )
                    for raw_line in proc.stdout:
                        line = raw_line.decode("utf-8", errors="replace").strip()
                        if line:
                            _last_lines.append(line)
                            if len(_last_lines) > 20:
                                _last_lines.pop(0)
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
                    # 取最後幾行有錯誤關鍵字的輸出寫進 log
                    _err_lines = [l for l in _last_lines
                                  if any(k in l for k in ('ERROR', 'error', 'HTTP', '429',
                                                          '403', 'Sign in', 'unavailable',
                                                          'Unable to', 'failed'))]
                    _tail = " | ".join(_err_lines[-3:]) or (_last_lines[-1] if _last_lines else "")
                    logging.warning(f"auto-video dl[{vi}] attempt {_attempt+1} failed rc={proc.returncode}: {_tail[:400]}")
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
            def _safe_mtime(x):
                try:
                    return os.path.getmtime(os.path.join(dl_dir, x))
                except OSError:
                    return 0  # 檔案在 exists/getmtime 之間被 rename（yt-dlp .part → .mp4）
            if vid_id:
                for f in sorted(os.listdir(dl_dir),
                                key=_safe_mtime,
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
                if cancel_event is not None and cancel_event.is_set():
                    # 取消剩下還沒跑的 future
                    for f in futures:
                        f.cancel()
                    raise _AvCancelled()
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
                # 註：新 cache 機制有 fingerprint 自動失效，這裡其實可以省略，
                # 但顯式移除可以避免 race condition（下游讀 cache 時可能剛好命中舊 mtime）
                for _p in (".face_embeddings.npy", ".face_embeddings.meta.json",
                           ".face_embeddings.review.json"):
                    fp = os.path.join(photo_dir, _p)
                    if os.path.isfile(fp):
                        try: os.remove(fp)
                        except OSError: pass
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
    _check_cancel()
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

        # 並行加速 #3：face-extract 階段改成 ThreadPoolExecutor
        # _scan_video_for_person 內部呼叫 _build_face_models()，每次都是獨立的
        # YuNet + SFace，不會踩到 OpenCV DNN 的 thread-safety 問題。
        # max_workers=3：與 scoring 階段同一量級，配合 cv2.setNumThreads(10)
        # 可讓單機 ~7 核同時吃滿；再多會被 I/O / ffmpeg 子行程爭搶抵銷。
        _ref_sig_outer = _embeddings_sig(ref)
        _neg_sig_outer = _embeddings_sig(neg_ref)
        _extract_new = {}  # vi -> result_path
        _extract_lock = threading.Lock()
        _ex_total = len(scan_list)
        _ex_done_counter = [0]
        # 並行回報進度聚合：各 worker 目前分析到的 pct（vi -> 0~1），完成後移除
        _active_ex_pcts = {}
        _last_reported_ex_p = [42]  # 單調遞增下限，避免前後抖動

        def _extract_one(vi, dlf):
            video_path = dlf["full_path"]
            vname = dlf["filename"]

            with _extract_lock:
                _active_ex_pcts[vi] = 0.0
                partial = _ex_done_counter[0] + sum(_active_ex_pcts.values())
                pct_base = 42 + int(partial / max(_ex_total, 1) * 23)
                pct_base = min(pct_base, 65)
                if pct_base < _last_reported_ex_p[0]:
                    pct_base = _last_reported_ex_p[0]
                else:
                    _last_reported_ex_p[0] = pct_base

            q.put({"type": "progress", "phase": "extract", "percent": pct_base,
                   "message": f"🔍 掃描 {vi+1}/{_ex_total}: {vname[:40]}..."})

            def scan_cb(pct):
                with _extract_lock:
                    _active_ex_pcts[vi] = pct
                    partial = _ex_done_counter[0] + sum(_active_ex_pcts.values())
                    new_p = 42 + int(partial / max(_ex_total, 1) * 23)
                    new_p = min(new_p, 65)
                    if new_p < _last_reported_ex_p[0]:
                        new_p = _last_reported_ex_p[0]
                    else:
                        _last_reported_ex_p[0] = new_p
                q.put({"type": "progress", "phase": "extract", "percent": new_p,
                       "message": f"掃描 {vname[:30]}... {int(pct*100)}%"})

            try:
                # Per-video 快取：若影片未改且 ref/neg 特徵沒變就直接用快取
                _cached = _load_video_cache(video_path)
                timestamps = None
                if (_cached and _cached.get("ref_sig") == _ref_sig_outer
                        and _cached.get("neg_sig") == _neg_sig_outer
                        and "timestamps" in _cached):
                    timestamps = _cached["timestamps"]
                    q.put({"type": "progress", "phase": "extract", "percent": pct_base + 2,
                           "message": f"⚡ {vname[:40]} 快取命中（{len(timestamps)} 個時間點）"})
                else:
                    try:
                        timestamps = _scan_video_for_person(video_path, ref, scan_cb,
                                                              neg_embeddings=neg_ref)
                        _save_video_cache(video_path, {
                            "ref_sig": _ref_sig_outer, "neg_sig": _neg_sig_outer,
                            "timestamps": timestamps,
                        })
                    except Exception as _se:
                        logging.warning(f"scan failed for {vname}: {_se}")
                        q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                               "message": f"⚠️ {vname[:40]} 掃描失敗，略過（{_se}）"})
                        return
                if not timestamps:
                    q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                           "message": f"⏭️ {vname[:40]} 中未偵測到 {person}"})
                    return

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
                    cutoff = _src_dur - 5.0
                    trimmed = []
                    for s, e in segments:
                        if s >= cutoff:
                            continue
                        e = min(e, cutoff)
                        if e - s >= 1.0:
                            trimmed.append((s, e))
                    segments = trimmed
                    if not segments:
                        q.put({"type": "progress", "phase": "extract", "percent": pct_base + 4,
                               "message": f"⏭️ {vname[:40]}: 片段都在片尾區，跳過"})
                        return

                total_dur = sum(e - s for s, e in segments)

                base = os.path.splitext(os.path.basename(vname))[0]
                out_name_ext = f"{base}_{sanitize_name(person)}.mp4"
                vtype = _infer_video_type_from_path(video_path, sanitize_name(person))
                if vtype:
                    _typed_extract_dir = os.path.join(extract_dir, vtype)
                else:
                    _typed_extract_dir = extract_dir
                # mkdir 可能被多 thread 同時呼叫，exist_ok 保證安全
                os.makedirs(_typed_extract_dir, exist_ok=True)
                out_path = os.path.join(_typed_extract_dir, out_name_ext)
                result = _extract_segments(video_path, segments, out_path)

                if result and os.path.isfile(result):
                    with _extract_lock:
                        _extract_new[vi] = result
                        _done_p = 42 + int((_ex_done_counter[0] + 1) / max(_ex_total, 1) * 23)
                        _done_p = min(_done_p, 65)
                        if _done_p < _last_reported_ex_p[0]:
                            _done_p = _last_reported_ex_p[0]
                        else:
                            _last_reported_ex_p[0] = _done_p
                    q.put({"type": "progress", "phase": "extract",
                           "percent": _done_p,
                           "message": f"✅ 擷取 {len(segments)} 段（{total_dur:.1f}秒）from {vname[:30]}"})
            finally:
                with _extract_lock:
                    _ex_done_counter[0] += 1
                    _active_ex_pcts.pop(vi, None)

        from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
        _ex_workers = min(3, max(1, _ex_total))
        logging.info(f"face-extract: {_ex_total} videos with {_ex_workers} parallel workers")
        with _TPE(max_workers=_ex_workers, thread_name_prefix="extract") as _exe:
            _futs = [_exe.submit(_extract_one, vi, dlf)
                     for vi, dlf in enumerate(scan_list)]
            for _f in _ac(_futs):
                try: _f.result()
                except Exception as _fe:
                    logging.warning(f"extract worker crashed: {_fe}")

        # 依原始順序把新擷取的影片附加到 extracted_files（已有的在前）
        for _vi in sorted(_extract_new.keys()):
            extracted_files.append(_extract_new[_vi])

    if not extracted_files:
        q.put({"type": "error", "message": f"所有影片中都未偵測到 {person}"})
        return

    q.put({"type": "progress", "phase": "extract", "percent": 70,
           "message": f"🎯 擷取完成：{len(extracted_files)} 個影片包含 {person}"})

    # ── Phase 4: 精華剪輯 (70-100%) ──
    _check_cancel()
    q.put({"type": "progress", "phase": "highlight", "percent": 70,
           "message": "🎬 分析影片，挑選精彩片段..."})

    all_scores_map = {}  # vi -> (vpath, scores)
    _score_ref_sig = _embeddings_sig(ref)
    _score_neg_sig = _embeddings_sig(neg_ref)
    _cache_hits = 0
    _cache_hits_lock = threading.Lock()
    # Per-video 快取：若影片、ref/neg、strategy、scoring 版本都沒變才用快取
    # _SCORE_VERSION: 每次改 scoring 邏輯就遞增，讓舊快取失效
    _SCORE_VERSION = 6  # v6: 加入畫質懲罰（暗/過曝/模糊扣分）

    # 並行加速 #2：把 scoring 迴圈改成 ThreadPoolExecutor（3 個 worker）
    # _score_video_highlights 內部自己 build 一份 detector+recognizer（見 _build_face_models），
    # 因此每個 thread 都有獨立的 YuNet / SFace 實例，不會踩到 thread-safety。
    # max_workers=3：配合 cv2/torch 10 核心，避免過度爭搶導致 context switch 抖動。
    _total_vids = len(extracted_files)
    _done_counter = [0]
    _done_lock = threading.Lock()
    # 各 worker 目前分析到的 pct（vi -> 0~1），完成後從 dict 移除
    _active_pcts = {}
    _last_reported_p = [70]  # 單調遞增下限，不讓進度條回退

    def _score_one(vi, vpath):
        nonlocal _cache_hits
        vname = os.path.basename(vpath)

        # 進度條防抖動：
        # - partial = 完成數 + 所有 active worker 當下 pct 的總和
        #   （比「最後一個 worker 的 pct」更穩定，反映整體進度）
        # - 強制單調遞增：不讓 p 回退，避免多 worker 各報各的造成前後跳
        def score_cb(pct):
            with _done_lock:
                _active_pcts[vi] = pct
                partial = _done_counter[0] + sum(_active_pcts.values())
                new_p = 70 + int(partial / max(_total_vids, 1) * 15)
                new_p = min(new_p, 85)
                if new_p < _last_reported_p[0]:
                    new_p = _last_reported_p[0]  # 不回退
                else:
                    _last_reported_p[0] = new_p
            q.put({"type": "progress", "phase": "highlight", "percent": new_p,
                   "message": f"分析 {vname[:30]}... {int(pct*100)}%"})

        scores = None
        _cached = _load_video_cache(vpath)
        if (_cached and _cached.get("score_ref_sig") == _score_ref_sig
                and _cached.get("score_neg_sig") == _score_neg_sig
                and _cached.get("strategy") == strategy
                and _cached.get("score_version") == _SCORE_VERSION
                and "scores" in _cached):
            scores = _cached["scores"]
            with _cache_hits_lock:
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
                       "percent": 70 + int((vi + 1) / max(_total_vids, 1) * 15),
                       "message": f"⚠️ {vname[:40]} 評分失敗，略過"})
                scores = None

        with _done_lock:
            _done_counter[0] += 1
            _active_pcts.pop(vi, None)  # 完成後從 active 清掉，避免重複計入
        return vi, vpath, scores

    from concurrent.futures import ThreadPoolExecutor, as_completed
    _score_workers = min(3, max(1, _total_vids))
    logging.info(f"scoring {_total_vids} videos with {_score_workers} parallel workers")
    with ThreadPoolExecutor(max_workers=_score_workers, thread_name_prefix="score") as _ex:
        _futures = [_ex.submit(_score_one, vi, vpath)
                    for vi, vpath in enumerate(extracted_files)]
        for _fut in as_completed(_futures):
            try:
                vi, vpath, scores = _fut.result()
                if scores:
                    all_scores_map[vi] = (vpath, scores)
            except Exception as _fe:
                logging.warning(f"score worker crashed: {_fe}")

    # 還原原始順序（以 vi 為 key），後面的 clip 挑選邏輯可能依賴輸入順序的可預測性
    all_scores = [all_scores_map[vi] for vi in sorted(all_scores_map.keys())]

    if _cache_hits > 0:
        q.put({"type": "progress", "phase": "highlight", "percent": 85,
               "message": f"⚡ 評分快取命中 {_cache_hits}/{len(extracted_files)} 部"})

    if not all_scores:
        q.put({"type": "error", "message": "所有擷取影片都無法分析"})
        return

    # ── Wave 5 S11: 來源等級品質加權 ──
    # 用 source 的解析度、檔案大小（近似 bitrate）、時長合理性，
    # 對 per-frame score 加乘數（0.85 ~ 1.15），讓品質好的來源整體勝出。
    try:
        src_weights = {}
        for vpath, _sc in all_scores:
            try:
                _sw, _sh = _probe_video_size(vpath)
                _fsize = os.path.getsize(vpath) if os.path.isfile(vpath) else 0
                _dur = _sc[0].get("duration") if _sc else None
                # 解析度分數：720p=1.0；1080p=1.08；1440p+=1.12；<720=0.92
                _short = min(_sw or 0, _sh or 0)
                if _short >= 1440:
                    _rscore = 1.12
                elif _short >= 1080:
                    _rscore = 1.08
                elif _short >= 720:
                    _rscore = 1.0
                elif _short >= 480:
                    _rscore = 0.92
                else:
                    _rscore = 0.85
                # bitrate proxy (MB/min)：6~20 最佳；< 2 粗糙，> 40 也扣（可能是再壓）
                if _dur and _dur > 1 and _fsize > 0:
                    _mbpm = (_fsize / 1024 / 1024) / (_dur / 60.0)
                    if _mbpm < 2:
                        _bscore = 0.90
                    elif _mbpm < 6:
                        _bscore = 0.97
                    elif _mbpm <= 20:
                        _bscore = 1.03
                    elif _mbpm <= 40:
                        _bscore = 1.0
                    else:
                        _bscore = 0.95
                else:
                    _bscore = 1.0
                # 時長合理性：15~600s 最佳，極短/極長扣
                if _dur:
                    if 15 <= _dur <= 600:
                        _dscore = 1.0
                    elif _dur < 15:
                        _dscore = 0.92
                    elif _dur <= 1800:
                        _dscore = 0.95
                    else:
                        _dscore = 0.90
                else:
                    _dscore = 1.0
                _w = _rscore * _bscore * _dscore
                _w = max(0.80, min(1.18, _w))
                src_weights[vpath] = _w
            except Exception:
                src_weights[vpath] = 1.0
        # 套用加權到 per-frame score
        _weighted = 0
        for vpath, _sc in all_scores:
            _w = src_weights.get(vpath, 1.0)
            if abs(_w - 1.0) < 0.01:
                continue
            for _s in _sc:
                _s["score"] = float(_s.get("score", 0.0)) * _w
            _weighted += 1
        if _weighted > 0:
            _avg_w = sum(src_weights.values()) / max(1, len(src_weights))
            logging.info(f"S11 source-weight applied to {_weighted}/{len(all_scores)} "
                         f"videos, avg_weight={_avg_w:.3f}")
    except Exception as _s11e:
        logging.warning(f"S11 source weighting skipped: {_s11e}")

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
    # ── Wave 1/2 進階品質參數 ──
    # 預設值設得寬鬆，避免把主選砍光；只有後段 fallback 才會關掉。
    _A3_TARGET_RATIO = 0.30   # A3：目標人物在片段內出現比例 ≥ 30%（2Hz 密集抽樣）
    _A4_SHAKE_MAX   = 9.0     # A4：光流平均 magnitude ≤ 9.0（太搖晃）
    _A4_SHARP_MIN   = 60.0    # A4：Laplacian variance ≥ 60（太糊）
    # 嚴格限制每部影片片段數（主選 + 替補都用同一個上限）
    all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                              adjusted_total * _extra_factor,
                                              max_per_video,
                                              person=sanitize_name(person),
                                              prefer_vertical=_want_vertical,
                                              bgm_path=bgm_path,
                                              ref_embeddings=ref,
                                              neg_embeddings=neg_ref,
                                              target_ratio_min=_A3_TARGET_RATIO,
                                              shake_max=_A4_SHAKE_MAX,
                                              sharpness_min=_A4_SHARP_MIN)
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
                                                  prefer_vertical=_want_vertical,
                                                  bgm_path=bgm_path,
                                                  ref_embeddings=ref,
                                                  neg_embeddings=neg_ref,
                                                  target_ratio_min=_A3_TARGET_RATIO,
                                                  shake_max=_A4_SHAKE_MAX,
                                                  sharpness_min=_A4_SHARP_MIN)
    if len(all_candidates) < _needed_for_target and max_per_video <= 2:
        logging.info(f"second pass got only {len(all_candidates)} candidates for {_needed_for_target} "
                     f"needed, retrying with max_per_video=3")
        all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                                  adjusted_total * _extra_factor,
                                                  3,
                                                  person=sanitize_name(person),
                                                  prefer_vertical=_want_vertical,
                                                  bgm_path=bgm_path,
                                                  ref_embeddings=ref,
                                                  neg_embeddings=neg_ref,
                                                  target_ratio_min=_A3_TARGET_RATIO,
                                                  shake_max=_A4_SHAKE_MAX,
                                                  sharpness_min=_A4_SHARP_MIN)
    # 保險閥：嚴格 0.34 全軍覆沒時，放寬時間覆蓋率到 0.20 再試（身分比對仍嚴格）
    # 並放寬 A3/A4 門檻，避免過濾太嚴
    if not all_candidates:
        logging.warning("strict face_presence_ratio 0.34 yielded 0 candidates — "
                        "fallback to 0.20 (identity check still strict)")
        all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                                  adjusted_total * _extra_factor,
                                                  3,
                                                  person=sanitize_name(person),
                                                  prefer_vertical=_want_vertical,
                                                  min_presence_ratio=0.20,
                                                  bgm_path=bgm_path,
                                                  ref_embeddings=ref,
                                                  neg_embeddings=neg_ref,
                                                  target_ratio_min=0.20,
                                                  shake_max=15.0,
                                                  sharpness_min=40.0)
    # 第三層保險閥：放寬 + 關掉 multi-member 過濾
    # 小團體（QWER 4 人、ITZY 5 人…）的 fancam 鏡頭常常帶到其他成員，
    # multi-member 過濾太嚴會把所有候選砍光。最後一搏關掉這個檢查。
    if not all_candidates:
        logging.warning("fallback 0.20 also yielded 0 candidates — "
                        "final fallback with multi-member filter disabled")
        all_candidates = _select_highlight_clips(all_scores, clip_duration,
                                                  adjusted_total * _extra_factor,
                                                  3,
                                                  person=sanitize_name(person),
                                                  prefer_vertical=_want_vertical,
                                                  min_presence_ratio=0.20,
                                                  allow_multi_member=True,
                                                  bgm_path=bgm_path)
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
                cover_photo=_cover_photo_path,
                cover_duration=_cover_dur,
                bgm_path=bgm_path, bgm_volume=bgm_volume, bgm_start=bgm_start,
                enhance=enhance, stabilize=stabilize,
                interpolate_60fps=interpolate_60fps,
                upscale_target=upscale_target,
                two_pass_loudnorm=two_pass_loudnorm,
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
            # 給前端重建 `?dl=Person #Person #group #한글.mp4` 用
            "person": person,
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
                if evt.get("type") in ("done", "error", "cancelled"):
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


def _av_cancel_one(tid):
    """中止單一任務：設 cancel_event、推 cancelled 訊息、觸發 confirm_event（讓 worker 從 wait 中醒來）、更新 history。回傳是否成功找到任務。"""
    with auto_video_tasks_lock:
        task = auto_video_tasks.get(tid)
    if not task:
        return False, "任務不存在或已結束"
    try:
        ce = task.get("cancel_event")
        if ce:
            ce.set()
        q = task.get("queue")
        label = task.get("label", "")
        if q:
            q.put({"type": "cancelled", "message": "⏹ 使用者中止", "label": label})
            q.put({"type": "error", "message": "已中止"})  # 讓 SSE 主迴圈收尾
        # 如果在 preview 模式卡在 confirm_event.wait()，推個空選擇讓 worker 繼續走
        cfe = task.get("confirm_event")
        cfd = task.get("confirm_data")
        if cfe and cfd is not None and not cfe.is_set():
            cfd[0] = []  # 空選 → pipeline 視為取消
            cfe.set()
    except Exception as e:
        logging.warning(f"cancel task {tid}: {e}")
    _av_save_history(tid, "cancelled", task.get("label", ""), {"message": "使用者中止"})
    return True, None


@app.route("/api/auto-video/stop/<task_id>", methods=["POST"])
def api_auto_video_stop(task_id):
    """中止單一一鍵影片任務。"""
    ok, err = _av_cancel_one(task_id)
    if not ok:
        return jsonify({"error": err}), 404
    return jsonify({"ok": True, "task_id": task_id})


@app.route("/api/auto-video/stop-all", methods=["POST"])
def api_auto_video_stop_all():
    """一鍵中止所有進行中的一鍵影片任務。"""
    with auto_video_tasks_lock:
        tids = list(auto_video_tasks.keys())
    stopped = []
    for tid in tids:
        ok, _ = _av_cancel_one(tid)
        if ok:
            stopped.append(tid)
    return jsonify({"ok": True, "stopped": stopped, "count": len(stopped)})


@app.route("/api/auto-video/dismiss/<task_id>", methods=["POST", "DELETE"])
def api_auto_video_dismiss(task_id):
    """把任務從歷史清掉 — 讓前端叉叉按了之後重新整理也不會再出現。
    僅清 in-memory + disk history；已完成的輸出檔不動。
    若任務仍在跑，不處理（請先用 /stop）。"""
    with auto_video_tasks_lock:
        still_active = task_id in auto_video_tasks
    if still_active:
        return jsonify({"error": "任務仍在跑，請先中止"}), 409
    with _AV_HISTORY_LOCK:
        existed = auto_video_history.pop(task_id, None) is not None
        _av_persist_history_nolock()
    return jsonify({"ok": True, "removed": existed, "task_id": task_id})


@app.route("/api/auto-video/dismiss-all", methods=["POST", "DELETE"])
def api_auto_video_dismiss_all():
    """把所有「非 running」的歷史一次清光。"""
    with auto_video_tasks_lock:
        active_ids = set(auto_video_tasks.keys())
    with _AV_HISTORY_LOCK:
        dropped = [tid for tid in list(auto_video_history.keys()) if tid not in active_ids]
        for tid in dropped:
            auto_video_history.pop(tid, None)
        _av_persist_history_nolock()
    return jsonify({"ok": True, "removed": len(dropped)})


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
    <select id="hl-audio-mode" onchange="hlAudioModeChange()">
      <option value="original" selected>保留原音</option>
      <option value="mute">靜音</option>
      <option value="bgm">純 BGM</option>
      <option value="bgm+source">BGM + 原音（ducking）</option>
    </select>
  </div>
</div>
<div id="hl-bgm-wrap" style="margin-top:8px;display:none;padding:8px;background:var(--bg2);border-radius:6px">
  <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
    <label style="font-size:.8em;color:var(--txt2);min-width:48px">BGM</label>
    <select id="hl-bgm-select" style="flex:1;min-width:160px;font-size:.85em"></select>
    <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="bgmRefresh('hl')">🔄</button>
    <input type="file" id="hl-bgm-upload" accept=".wav,.mp3,.flac,.m4a,.aac,.ogg" style="display:none" onchange="bgmUploadFile(this,'hl')">
    <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="document.getElementById('hl-bgm-upload').click()">📤 上傳</button>
  </div>
  <div style="display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap">
    <label style="font-size:.78em;color:var(--txt2)">音量</label>
    <input type="range" id="hl-bgm-vol" min="0" max="2" step="0.05" value="1.0" style="flex:1;min-width:80px">
    <span id="hl-bgm-vol-lbl" style="font-size:.78em;color:var(--txt2);min-width:34px">1.00</span>
    <label style="font-size:.78em;color:var(--txt2);margin-left:4px">起始(s)</label>
    <input type="number" id="hl-bgm-start" min="0" step="0.5" value="0" style="width:60px;font-size:.82em">
  </div>
</div>
<details style="margin-top:8px">
  <summary style="font-size:.82em;cursor:pointer;color:var(--txt2)">⚙️ 進階畫質設定（Wave 4/5/6）</summary>
  <div style="padding:8px;background:var(--bg2);border-radius:6px;margin-top:4px;display:flex;flex-direction:column;gap:4px;font-size:.82em">
    <label><input type="checkbox" id="hl-opt-ln" checked> 🎚️ 成品 Two-pass Loudnorm (-14 LUFS)</label>
    <label><input type="checkbox" id="hl-opt-enhance"> ✨ 輕降噪 + 銳化（hqdn3d + unsharp）</label>
    <label><input type="checkbox" id="hl-opt-stab"> 🎯 手震偵測穩定化（vidstab 2-pass，僅對中度晃動生效）</label>
    <label><input type="checkbox" id="hl-opt-60fps"> 🎞️ 60fps 插幀（minterpolate；耗時 ×2）</label>
    <div style="display:flex;align-items:center;gap:6px">
      <label style="color:var(--txt2)">升頻</label>
      <select id="hl-opt-upscale" style="font-size:.82em">
        <option value="">關閉</option>
        <option value="auto">自動（短邊 &lt; 720p 才升）</option>
        <option value="720p">升至 720p</option>
        <option value="1080p">升至 1080p</option>
      </select>
    </div>
  </div>
</details>
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
<div class="row" id="av-cover-row" style="align-items:center;gap:8px;flex-wrap:wrap">
  <label>封面圖</label>
  <span id="av-cover-status" style="display:inline-flex;align-items:center;gap:6px;padding:4px 10px;background:rgba(100,200,100,0.12);border:1px dashed rgba(100,200,100,0.4);border-radius:6px;font-size:.82em">
    <span id="av-cover-status-icon">🎲</span>
    <span id="av-cover-status-text">自動挑（隨機一張）</span>
    <img id="av-cover-thumb" style="display:none;width:32px;height:32px;object-fit:cover;border-radius:4px;margin-left:4px">
  </span>
  <button type="button" class="btn btn-sec" onclick="_avOpenCoverPicker()"
          style="font-size:.8em;padding:4px 10px">🖼️ 改自己挑</button>
  <button type="button" id="av-cover-reset" onclick="_avClearCover()"
          style="display:none;background:none;border:1px solid var(--border);border-radius:6px;padding:3px 10px;font-size:.78em;cursor:pointer" title="恢復成自動挑">↩️ 回自動</button>
  <label style="font-size:.78em;color:var(--txt2);margin-left:8px">封面秒數</label>
  <select id="av-cover-dur" style="font-size:.8em;padding:3px 6px" title="封面秒數；選「僅縮圖」= 只存在 1 幀，平台用來當縮圖但觀眾幾乎看不見">
    <option value="0" selected>僅縮圖（1 幀 ≈ 33ms）</option>
    <option value="0.3">0.3 秒</option>
    <option value="0.5">0.5 秒</option>
    <option value="1.0">1.0 秒</option>
    <option value="1.5">1.5 秒</option>
    <option value="2.0">2.0 秒</option>
  </select>
  <span id="av-cover-hint" style="flex-basis:100%;font-size:.72em;color:var(--txt2);margin-top:4px">
    💡 預設從資料庫隨機挑一張當封面（每次生成不一樣）。「僅縮圖」= 影片不會顯示這張圖，但 TikTok/YT 會拿來當縮圖
  </span>
  <input type="hidden" id="av-cover-filename" value="__auto__">
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
    <label style="font-size:.8em;color:var(--txt2)">平台（可複選）</label>
    <div id="av-platform-chips" style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">
      <label class="av-plat-chip" style="display:inline-flex;align-items:center;gap:3px;padding:3px 8px;border:1px solid var(--brd);border-radius:14px;font-size:.82em;cursor:pointer;user-select:none;background:var(--bg2)">
        <input type="checkbox" class="av-plat" value="tiktok" style="margin:0"> TikTok
      </label>
      <label class="av-plat-chip" style="display:inline-flex;align-items:center;gap:3px;padding:3px 8px;border:1px solid var(--brd);border-radius:14px;font-size:.82em;cursor:pointer;user-select:none;background:var(--bg2)">
        <input type="checkbox" class="av-plat" value="youtube" checked style="margin:0"> YouTube
      </label>
      <label class="av-plat-chip" style="display:inline-flex;align-items:center;gap:3px;padding:3px 8px;border:1px solid var(--brd);border-radius:14px;font-size:.82em;cursor:pointer;user-select:none;background:var(--bg2)">
        <input type="checkbox" class="av-plat" value="yt_shorts" checked style="margin:0"> YT Shorts
      </label>
      <label class="av-plat-chip" style="display:inline-flex;align-items:center;gap:3px;padding:3px 8px;border:1px solid var(--brd);border-radius:14px;font-size:.82em;cursor:pointer;user-select:none;background:var(--bg2)">
        <input type="checkbox" class="av-plat" value="ig_reels" style="margin:0"> IG Reels
      </label>
      <button type="button" class="btn" onclick="_avPlatToggleAll()" style="font-size:.75em;padding:2px 8px;margin-left:4px" title="一鍵全選 / 全不選">全部</button>
    </div>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">影片類型</label>
    <select id="av-video-type">
      <option value="all">全部</option>
      <option value="dance">舞蹈</option>
      <option value="fancam" selected>直拍</option>
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
      <option value="10">10 部</option>
      <option value="20" selected>20 部</option>
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
      <option value="3">3 秒</option>
      <option value="4">4 秒</option>
      <option value="5">5 秒</option>
      <option value="6" selected>6 秒</option>
      <option value="8">8 秒</option>
      <option value="10">10 秒</option>
      <option value="15">15 秒</option>
    </select>
  </div>
  <div>
    <label style="font-size:.8em;color:var(--txt2)">總長度</label>
    <select id="av-total-dur">
      <option value="15">15 秒</option>
      <option value="30">30 秒</option>
      <option value="45" selected>45 秒</option>
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
    <select id="av-audio" onchange="avAudioModeChange()">
      <option value="original">保留原音</option>
      <option value="mute" selected>靜音</option>
      <option value="bgm">純 BGM</option>
      <option value="bgm+source">BGM + 原音（ducking）</option>
    </select>
  </div>
</div>
<div id="av-bgm-wrap" style="margin-top:8px;display:none;padding:8px;background:var(--bg2);border-radius:6px">
  <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
    <label style="font-size:.8em;color:var(--txt2);min-width:48px">BGM</label>
    <select id="av-bgm-select" style="flex:1;min-width:160px;font-size:.85em"></select>
    <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="bgmRefresh('av')">🔄</button>
    <input type="file" id="av-bgm-upload" accept=".wav,.mp3,.flac,.m4a,.aac,.ogg" style="display:none" onchange="bgmUploadFile(this,'av')">
    <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="document.getElementById('av-bgm-upload').click()">📤 上傳</button>
  </div>
  <div style="display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap">
    <label style="font-size:.78em;color:var(--txt2)">音量</label>
    <input type="range" id="av-bgm-vol" min="0" max="2" step="0.05" value="1.0" style="flex:1;min-width:80px">
    <span id="av-bgm-vol-lbl" style="font-size:.78em;color:var(--txt2);min-width:34px">1.00</span>
    <label style="font-size:.78em;color:var(--txt2);margin-left:4px">起始(s)</label>
    <input type="number" id="av-bgm-start" min="0" step="0.5" value="0" style="width:60px;font-size:.82em">
  </div>
</div>
<details style="margin-top:8px">
  <summary style="font-size:.82em;cursor:pointer;color:var(--txt2)">⚙️ 進階畫質設定（Wave 4/5/6）</summary>
  <div style="padding:8px;background:var(--bg2);border-radius:6px;margin-top:4px;display:flex;flex-direction:column;gap:4px;font-size:.82em">
    <label><input type="checkbox" id="av-opt-ln"> 🎚️ 成品 Two-pass Loudnorm (-14 LUFS)</label>
    <label><input type="checkbox" id="av-opt-enhance" checked> ✨ 輕降噪 + 銳化（hqdn3d + unsharp）</label>
    <label><input type="checkbox" id="av-opt-stab"> 🎯 手震偵測穩定化（vidstab 2-pass，僅對中度晃動生效）</label>
    <label><input type="checkbox" id="av-opt-60fps"> 🎞️ 60fps 插幀（minterpolate；耗時 ×2）</label>
    <div style="display:flex;align-items:center;gap:6px">
      <label style="color:var(--txt2)">升頻</label>
      <select id="av-opt-upscale" style="font-size:.82em">
        <option value="">關閉</option>
        <option value="auto" selected>自動（短邊 &lt; 720p 才升）</option>
        <option value="720p">升至 720p</option>
        <option value="1080p">升至 1080p</option>
      </select>
    </div>
  </div>
</details>
<div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
  <button class="btn btn-pri" id="btn-auto-video" onclick="autoVideoStart(false)"
          style="font-size:1em;padding:10px 32px">🚀 一鍵生成影片</button>
  <button class="btn btn-sec" onclick="autoVideoStart(true)"
          style="font-size:1em;padding:10px 24px">🎞️ 先挑選再合成</button>
  <button class="btn" id="btn-av-stop-all" onclick="_avStopAll()"
          style="font-size:1em;padding:10px 24px;background:#fee2e2;color:#b91c1c;border:1px solid #fca5a5"
          title="中止所有進行中的一鍵影片任務">⏹ 一鍵中止</button>
  <button class="btn" id="btn-av-download-all" onclick="_avDownloadAllAndClear()"
          style="font-size:1em;padding:10px 24px;background:#dcfce7;color:#166534;border:1px solid #86efac;display:none"
          title="下載當前批次全部完成的影片，下載完後自動清除卡片">⬇ 一鍵下載全部</button>
  <button class="btn" onclick="openPhotosHealth()"
          style="font-size:1em;padding:10px 20px;background:#e0e7ff;color:#3730a3;border:1px solid #a5b4fc"
          title="檢視每位成員的照片數、face embedding 狀態、可能混到的照片">🏥 照片健檢</button>
  <div id="av-multi-mode-wrap" style="margin-left:auto;display:flex;align-items:center;gap:6px;padding:6px 10px;background:rgba(100,150,255,0.08);border-radius:8px">
    <label for="av-multi-mode" style="font-size:.82em;color:var(--txt2);margin:0" title="選「全團員」或多人時才生效">多人模式</label>
    <select id="av-multi-mode" style="font-size:.85em;padding:4px 8px">
      <option value="parallel" selected>⚡ 並行（同時跑）</option>
      <option value="sequential">⏩ 依序（一個接一個）</option>
    </select>
  </div>
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

<!-- 照片健檢彈窗 -->
<div class="modal" id="photos-health-modal">
<div class="modal-box" style="max-width:1000px">
<div class="modal-head">
  <h3>🏥 照片健檢 Dashboard</h3>
  <span id="ph-summary" style="font-size:.82em;color:var(--txt2);margin-left:8px"></span>
  <button class="modal-close" onclick="closeModal('photos-health-modal')">&times;</button>
</div>
<div class="modal-body">
  <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center">
    <label style="font-size:.85em;color:var(--txt2)">篩選狀態:</label>
    <select id="ph-status-filter" onchange="renderPhotosHealth()" style="font-size:.88em">
      <option value="all">全部</option>
      <option value="problem">⚠️ 只看有問題的</option>
      <option value="ok">✅ 只看正常</option>
    </select>
    <label style="font-size:.85em;color:var(--txt2);margin-left:12px">搜尋:</label>
    <input type="text" id="ph-search" oninput="renderPhotosHealth()" placeholder="成員名或團名" style="font-size:.85em;padding:4px 10px">
    <span style="flex:1"></span>
    <button class="btn" onclick="loadPhotosHealth(true)" style="font-size:.82em;padding:4px 12px">🔄 重新載入</button>
  </div>
  <div id="ph-table-wrap" style="max-height:70vh;overflow-y:auto">
    <div style="text-align:center;padding:40px;color:var(--txt2)">載入中…</div>
  </div>
</div>
</div>
</div>

<!-- 聚類 audit 彈窗（HDBSCAN 混人偵測） -->
<div class="modal" id="cluster-audit-modal">
<div class="modal-box" style="max-width:760px">
<div class="modal-head">
  <h3>🔬 資料夾聚類 Audit</h3>
  <span id="ca-subtitle" style="font-size:.8em;color:var(--txt2);margin-left:8px"></span>
  <button class="modal-close" onclick="closeModal('cluster-audit-modal')">&times;</button>
</div>
<div class="modal-body">
  <div id="ca-body">
    <div style="text-align:center;padding:40px;color:var(--txt2)">載入中…</div>
  </div>
</div>
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
      <select id="cb-audio" style="font-size:.88em" onchange="cbAudioModeChange()">
        <option value="original" selected>保留原音</option>
        <option value="mute">靜音</option>
        <option value="bgm">純 BGM</option>
        <option value="bgm+source">BGM + 原音（ducking）</option>
      </select>
    </div>
    <span style="flex:1"></span>
    <button class="btn btn-pri" onclick="cbCompile()" style="font-size:.95em;padding:8px 24px">
      🎬 合成選取的片段
    </button>
  </div>
  <div id="cb-bgm-wrap" style="margin-top:8px;display:none;padding:8px;background:var(--bg2);border-radius:6px">
    <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">
      <label style="font-size:.8em;color:var(--txt2);min-width:48px">BGM</label>
      <select id="cb-bgm-select" style="flex:1;min-width:160px;font-size:.85em"></select>
      <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="bgmRefresh('cb')">🔄</button>
      <input type="file" id="cb-bgm-upload" accept=".wav,.mp3,.flac,.m4a,.aac,.ogg" style="display:none" onchange="bgmUploadFile(this,'cb')">
      <button class="btn" style="font-size:.78em;padding:4px 8px" onclick="document.getElementById('cb-bgm-upload').click()">📤 上傳</button>
    </div>
    <div style="display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap">
      <label style="font-size:.78em;color:var(--txt2)">音量</label>
      <input type="range" id="cb-bgm-vol" min="0" max="2" step="0.05" value="1.0" style="flex:1;min-width:80px">
      <span id="cb-bgm-vol-lbl" style="font-size:.78em;color:var(--txt2);min-width:34px">1.00</span>
      <label style="font-size:.78em;color:var(--txt2);margin-left:4px">起始(s)</label>
      <input type="number" id="cb-bgm-start" min="0" step="0.5" value="0" style="width:60px;font-size:.82em">
    </div>
  </div>
  <details style="margin-top:8px">
    <summary style="font-size:.82em;cursor:pointer;color:var(--txt2)">⚙️ 進階畫質設定（Wave 4/5/6）</summary>
    <div style="padding:8px;background:var(--bg2);border-radius:6px;margin-top:4px;display:flex;flex-direction:column;gap:4px;font-size:.82em">
      <label><input type="checkbox" id="cb-opt-ln" checked> 🎚️ 成品 Two-pass Loudnorm (-14 LUFS)</label>
      <label><input type="checkbox" id="cb-opt-enhance"> ✨ 輕降噪 + 銳化（hqdn3d + unsharp）</label>
      <label><input type="checkbox" id="cb-opt-stab"> 🎯 手震偵測穩定化（vidstab 2-pass，僅對中度晃動生效）</label>
      <label><input type="checkbox" id="cb-opt-60fps"> 🎞️ 60fps 插幀（minterpolate;耗時 ×2）</label>
      <div style="display:flex;align-items:center;gap:6px">
        <label style="color:var(--txt2)">升頻</label>
        <select id="cb-opt-upscale" style="font-size:.82em">
          <option value="">關閉</option>
          <option value="auto">自動（短邊 &lt; 720p 才升）</option>
          <option value="720p">升至 720p</option>
          <option value="1080p">升至 1080p</option>
        </select>
      </div>
    </div>
  </details>
</div>
</div>
</div>

<script>
// ── BGM 共用工具 ──────────────────────────────────────
// prefix: 'hl' / 'av' / 'cb' — 對應 highlight / auto-video / clip-browser 三張卡
window._bgmFiles = [];
async function bgmRefresh(prefix){
  try{
    const r = await fetch('/api/bgm/list');
    const d = await r.json();
    window._bgmFiles = d.files || [];
    ['hl','av','cb'].forEach(p=>{
      const sel = document.getElementById(p+'-bgm-select');
      if(!sel) return;
      const cur = sel.value;
      sel.innerHTML = '<option value="">（未選擇）</option>' + window._bgmFiles.map(f=>
        '<option value="'+encodeURIComponent(f.name)+'">'+f.name+' ('+(f.size_kb||0)+' KB)</option>'
      ).join('');
      if(cur) sel.value = cur;
    });
  }catch(e){ console.warn('bgm list fail', e); }
}
async function bgmUploadFile(input, prefix){
  const f = input.files && input.files[0];
  if(!f) return;
  const fd = new FormData();
  fd.append('file', f);
  try{
    const r = await fetch('/api/bgm/upload',{method:'POST',body:fd});
    const d = await r.json();
    if(d.error){ alert('BGM 上傳失敗：'+d.error); return; }
    await bgmRefresh(prefix);
    const sel = document.getElementById(prefix+'-bgm-select');
    if(sel && d.name) sel.value = encodeURIComponent(d.name);
  }catch(e){ alert('BGM 上傳失敗：'+e); }
  input.value = '';
}
function bgmInjectOpts(prefix, opts){
  const sel = document.getElementById(prefix+'-bgm-select');
  if(!sel) return;
  const v = sel.value;
  if(!v) return;
  opts.bgm_path = decodeURIComponent(v);
  const vol = document.getElementById(prefix+'-bgm-vol');
  const st = document.getElementById(prefix+'-bgm-start');
  if(vol) opts.bgm_volume = parseFloat(vol.value) || 1.0;
  if(st) opts.bgm_start = parseFloat(st.value) || 0;
}
// Wave 4/5/6 進階設定 — 把勾選狀態塞進 POST body
function _injectAdvOpts(prefix, opts){
  const _ln = document.getElementById(prefix+'-opt-ln');
  const _en = document.getElementById(prefix+'-opt-enhance');
  const _st = document.getElementById(prefix+'-opt-stab');
  const _fp = document.getElementById(prefix+'-opt-60fps');
  const _up = document.getElementById(prefix+'-opt-upscale');
  if(_ln) opts.two_pass_loudnorm = !!_ln.checked;
  if(_en) opts.enhance = !!_en.checked;
  if(_st) opts.stabilize = !!_st.checked;
  if(_fp) opts.interpolate_60fps = !!_fp.checked;
  if(_up) opts.upscale_target = (_up.value || '').trim();
}
function _bgmToggleWrap(prefix){
  const sel = document.getElementById(prefix+'-audio-mode') || document.getElementById(prefix+'-audio');
  const wrap = document.getElementById(prefix+'-bgm-wrap');
  if(!sel || !wrap) return;
  const needBgm = (sel.value === 'bgm' || sel.value === 'bgm+source');
  wrap.style.display = needBgm ? '' : 'none';
  if(needBgm && (!window._bgmFiles || !window._bgmFiles.length)) bgmRefresh(prefix);
}
function hlAudioModeChange(){ _bgmToggleWrap('hl'); }
function avAudioModeChange(){ _bgmToggleWrap('av'); }
function cbAudioModeChange(){ _bgmToggleWrap('cb'); }
// 音量拖動即時更新 label
document.addEventListener('DOMContentLoaded', ()=>{
  ['hl','av','cb'].forEach(p=>{
    const v = document.getElementById(p+'-bgm-vol');
    const l = document.getElementById(p+'-bgm-vol-lbl');
    if(v && l){ v.addEventListener('input', ()=>{ l.textContent = parseFloat(v.value).toFixed(2); }); }
  });
});

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

// ── 照片健檢 Dashboard ──
let _phData = null;
async function loadPhotosHealth(force){
  const wrap = document.getElementById('ph-table-wrap');
  if(!_phData || force){
    wrap.innerHTML = '<div style="text-align:center;padding:40px;color:var(--txt2)">載入中…</div>';
    try{
      const r = await fetch('/api/photos-health');
      _phData = await r.json();
    }catch(e){
      wrap.innerHTML = '<div style="padding:20px;color:#b91c1c">載入失敗: '+e+'</div>';
      return;
    }
  }
  renderPhotosHealth();
}
function openPhotosHealth(){
  document.getElementById('photos-health-modal').classList.add('show');
  loadPhotosHealth(true);
}
function _phFmtAge(ts){
  if(!ts) return '-';
  const s = Math.floor(Date.now()/1000 - ts);
  if(s < 60) return s+'s';
  if(s < 3600) return Math.floor(s/60)+'m';
  if(s < 86400) return Math.floor(s/3600)+'h';
  return Math.floor(s/86400)+'d';
}
function _phStatusBadge(st){
  const MAP = {
    'ok':          ['✅ OK',      '#dcfce7', '#166534'],
    'low_photos':  ['⚠️ 照片少',  '#fef3c7', '#92400e'],
    'no_cache':    ['🔄 未建 cache','#e0e7ff','#3730a3'],
    'low_faces':   ['⚠️ 臉部少',  '#fef3c7', '#92400e'],
    'high_reject': ['⚠️ 污染高',  '#fee2e2', '#b91c1c'],
  };
  const [label, bg, fg] = MAP[st] || ['?', '#e5e7eb', '#374151'];
  return '<span style="padding:2px 8px;border-radius:10px;font-size:.78em;background:'+bg+';color:'+fg+';white-space:nowrap">'+label+'</span>';
}
function renderPhotosHealth(){
  if(!_phData) return;
  const flt = document.getElementById('ph-status-filter').value;
  const q = (document.getElementById('ph-search').value || '').toLowerCase().trim();
  let members = _phData.members || [];
  if(flt === 'problem') members = members.filter(m => m.status !== 'ok');
  else if(flt === 'ok') members = members.filter(m => m.status === 'ok');
  if(q) members = members.filter(m =>
    (m.folder||'').toLowerCase().includes(q) ||
    (m.display||'').toLowerCase().includes(q) ||
    (m.group||'').toLowerCase().includes(q)
  );

  // group by group name
  const byGroup = {};
  for(const m of members){
    const g = m.group || '(無團)';
    if(!byGroup[g]) byGroup[g] = [];
    byGroup[g].push(m);
  }

  // summary
  const total = (_phData.members||[]).length;
  const problems = (_phData.members||[]).filter(m => m.status !== 'ok').length;
  document.getElementById('ph-summary').textContent =
    `共 ${total} 位成員，顯示 ${members.length}，問題 ${problems}`;

  let html = '';
  for(const g of Object.keys(byGroup).sort()){
    const ms = byGroup[g];
    html += '<div style="margin-bottom:14px">';
    html += '<div style="font-weight:600;padding:6px 10px;background:rgba(100,150,255,0.08);border-radius:6px;margin-bottom:6px">'
            + (g === '(無團)' ? g : '🎵 '+g)
            + ' <span style="font-size:.82em;color:var(--txt2);font-weight:normal">('+ms.length+')</span></div>';
    html += '<table style="width:100%;border-collapse:collapse;font-size:.85em">';
    html += '<thead><tr style="border-bottom:1px solid #e5e7eb">'
         +  '<th style="text-align:left;padding:6px 8px;width:26%">folder / display</th>'
         +  '<th style="text-align:right;padding:6px 8px">photos</th>'
         +  '<th style="text-align:right;padding:6px 8px">face kept</th>'
         +  '<th style="text-align:right;padding:6px 8px">rejected</th>'
         +  '<th style="text-align:right;padding:6px 8px">cache age</th>'
         +  '<th style="text-align:center;padding:6px 8px">status</th>'
         +  '<th style="text-align:center;padding:6px 8px">audit</th>'
         +  '</tr></thead><tbody>';
    for(const m of ms){
      const rejPct = (m.face_raw_rows && m.face_rejected_rows)
        ? ' (' + Math.round(m.face_rejected_rows/m.face_raw_rows*100) + '%)'
        : '';
      // 顯示 audit 按鈕：有 cache 的 row 都可以 audit（高 reject 的會特別標紅）
      const hiRej = (m.face_raw_rows && m.face_rejected_rows
                      && m.face_rejected_rows/m.face_raw_rows > 0.2);
      const auditBtn = m.face_cache_exists
        ? '<button class="btn" style="font-size:.72em;padding:2px 8px;'
          + (hiRej ? 'background:#fee2e2;color:#b91c1c;border-color:#fca5a5' : '')
          + '" onclick="openClusterAudit(\''+encodeURIComponent(m.folder)+'\')">🔬</button>'
        : '<span style="font-size:.72em;color:var(--txt2)">-</span>';
      html += '<tr style="border-bottom:1px solid #f3f4f6">'
           +  '<td style="padding:6px 8px"><code style="font-size:.85em">'+m.folder+'</code>'
           +     (m.display && m.display !== m.folder ? '<span style="color:var(--txt2);margin-left:6px">→ '+m.display+'</span>' : '')
           +  '</td>'
           +  '<td style="text-align:right;padding:6px 8px">'+m.photo_count+'</td>'
           +  '<td style="text-align:right;padding:6px 8px">'+(m.face_kept_rows??'-')+'</td>'
           +  '<td style="text-align:right;padding:6px 8px">'+(m.face_rejected_rows??'-')+rejPct+'</td>'
           +  '<td style="text-align:right;padding:6px 8px">'+_phFmtAge(m.face_cache_built_at)+'</td>'
           +  '<td style="text-align:center;padding:6px 8px">'+_phStatusBadge(m.status)+'</td>'
           +  '<td style="text-align:center;padding:6px 8px">'+auditBtn+'</td>'
           +  '</tr>';
    }
    html += '</tbody></table></div>';
  }
  if(!members.length) html = '<div style="text-align:center;padding:40px;color:var(--txt2)">沒有符合的資料</div>';
  document.getElementById('ph-table-wrap').innerHTML = html;
}

// ── HDBSCAN 資料夾聚類 Audit（Tier 1 #2）──
async function openClusterAudit(folderEnc){
  const folder = decodeURIComponent(folderEnc);
  document.getElementById('cluster-audit-modal').classList.add('show');
  document.getElementById('ca-subtitle').textContent = folder;
  const body = document.getElementById('ca-body');
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--txt2)">聚類中（HDBSCAN on cosine distances）…</div>';
  try{
    const r = await fetch('/api/photos-cluster-audit/' + encodeURIComponent(folder));
    const d = await r.json();
    if(!d.ok){
      body.innerHTML = '<div style="padding:20px;color:#b91c1c">❌ '+(d.reason||'unknown error')+'</div>';
      return;
    }
    let html = '';
    // Verdict badge 樣式
    const VERDICT_STYLE = {
      clean:    ['✅ 乾淨（單一身份）', '#dcfce7', '#166534'],
      mixed:    ['⚠️ 疑似混人',        '#fee2e2', '#b91c1c'],
      broken:   ['💥 參考不可用',      '#fef3c7', '#92400e'],
      unclear:  ['❓ 模糊',             '#e5e7eb', '#374151'],
    };
    const [vLabel, vBg, vFg] = VERDICT_STYLE[d.verdict] || ['?','#e5e7eb','#374151'];
    const vBadge = '<span style="padding:4px 10px;border-radius:12px;font-size:.82em;background:'
                  +vBg+';color:'+vFg+';margin-left:8px">'+vLabel+'</span>';

    const s = d.sim_stats || {};
    html += '<div style="background:rgba(100,150,255,0.06);padding:10px 14px;border-radius:8px;margin-bottom:14px;font-size:.9em">'
         +    '<div style="margin-bottom:6px"><b>model:</b> <code>'+(d.model_id||'?')+'</code>'
         +    ' &nbsp;<b>photos:</b> '+d.n_photos
         +    ' &nbsp;<b>clusters:</b> '+d.n_clusters
         +    ' &nbsp;<b>dominant:</b> '+Math.round(d.dominant_fraction*100)+'%'
         +    vBadge + '</div>'
         +    '<div style="font-size:.85em;color:var(--txt2)">'
         +      '<b>sim:</b> mean='+(s.mean??'?')+' std='+(s.std??'?')
         +      ' p5='+(s.p5??'?')+' p95='+(s.p95??'?')
         +      ' &nbsp;<b>silhouette:</b> '+(d.silhouette??'n/a')
         +    '</div>'
         +    (d.verdict_reason
            ? '<div style="font-size:.82em;color:var(--txt2);margin-top:4px;font-style:italic">'+d.verdict_reason+'</div>'
            : '')
         + '</div>';
    if(!d.clusters || !d.clusters.length){
      html += '<div style="padding:20px;color:var(--txt2)">沒有偵測到任何 cluster（全部 noise）</div>';
    }else{
      for(const [i, c] of d.clusters.entries()){
        const isMain = (i === 0);
        const color = isMain ? '#166534' : '#b91c1c';
        const bg = isMain ? '#dcfce7' : '#fee2e2';
        html += '<div style="margin-bottom:14px;border:1px solid '+color+'40;border-radius:8px;padding:10px">'
             +    '<div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">'
             +      '<span style="padding:2px 10px;border-radius:10px;font-size:.8em;background:'+bg+';color:'+color+'">cluster #'+c.label+'</span>'
             +      '<span style="font-size:.85em"><b>'+c.size+'</b> photos ('+Math.round(c.fraction*100)+'%)</span>'
             +      '<span style="font-size:.82em;color:var(--txt2)">cohesion '+c.cohesion.toFixed(3)+'</span>'
             +      '<span style="flex:1"></span>'
             +      (isMain ? '' : '<span style="font-size:.78em;color:#b91c1c">← 可能是另一個人</span>')
             +    '</div>'
             +    '<div style="display:flex;gap:6px;flex-wrap:wrap">';
        for(const fn of (c.example_files||[])){
          const u = '/photos/'+encodeURIComponent(folder)+'/'+encodeURIComponent(fn);
          html += '<img src="'+u+'" loading="lazy" style="width:64px;height:64px;object-fit:cover;border-radius:4px;cursor:pointer" onclick="document.getElementById(\'lightbox-img\').src=this.src; document.getElementById(\'lightbox\').classList.add(\'show\')" title="'+fn+'">';
        }
        html += '</div></div>';
      }
    }
    html += '<div style="text-align:right;margin-top:8px"><button class="btn" style="font-size:.82em;padding:4px 12px" onclick="openClusterAuditForce(\''+encodeURIComponent(folder)+'\')">🔄 強制 rebuild + 重新 audit</button></div>';
    body.innerHTML = html;
  }catch(e){
    body.innerHTML = '<div style="padding:20px;color:#b91c1c">載入失敗: '+e+'</div>';
  }
}
async function openClusterAuditForce(folderEnc){
  const folder = decodeURIComponent(folderEnc);
  const body = document.getElementById('ca-body');
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--txt2)">強制 rebuild + 聚類中（約需 10–15 秒）…</div>';
  try{
    const r = await fetch('/api/photos-cluster-audit/' + encodeURIComponent(folder) + '?force=1');
    // 直接重新 render
    await r.json();
    openClusterAudit(folderEnc);
  }catch(e){
    body.innerHTML = '<div style="padding:20px;color:#b91c1c">載入失敗: '+e+'</div>';
  }
}

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
      // 綁上 task_id 讓叉叉按鈕能呼叫 /dismiss/<id> 清掉歷史
      tc._taskId = t.task_id;
      if(t.status === 'done' && t.data && t.data.file_url){
        // 已完成 — 直接顯示結果
        tc.bar.style.width = '100%';
        tc.pct.textContent = '100%';
        tc.title.textContent = '🎉 完成！ — '+t.label;
        tc.title.style.color = 'var(--ok)';
        tc._finished = true;
        _avStopTimer(tc);
        if(tc.stopBtn){ tc.stopBtn.style.display = 'none'; }
        const d = t.data;
        // 記下 person 才能建漂亮檔名 URL（`Jisoo #Jisoo #blackpink #지수.mp4`）
        tc.person = d.person || '';
        const _dlUrl = _avBuildDownloadUrl(d.file_url, tc.person, d.filename);
        const _prettyName = (() => {
          try{
            const u = new URL(_dlUrl, window.location.origin);
            return u.searchParams.get('dl') || d.filename;
          }catch(_){ return d.filename; }
        })();
        const _prettyAttr = _prettyName.replace(/&/g,'&amp;').replace(/"/g,'&quot;');
        tc._downloadUrl = _dlUrl;
        tc._downloadName = _prettyName;
        tc.msg.innerHTML = '✅ <a href="'+_dlUrl+'" download="'+_prettyAttr+'" target="_blank" style="color:var(--pri);font-weight:600">'
          +_prettyName+'</a> ('+d.file_size_mb+'MB)';
        tc.result.style.display = '';
        tc.result.innerHTML =
          '<div style="display:flex;gap:12px;flex-wrap:wrap;font-size:.85em;margin-bottom:8px">'
          +'<span>✂️ <b>'+d.clips_count+'</b> 個片段</span>'
          +'<span>⏱️ <b>'+d.duration+'</b> 秒</span>'
          +'<span>📦 <b>'+d.file_size_mb+'</b> MB</span></div>'
          +'<video src="'+d.file_url+'" controls style="max-width:100%;max-height:400px;border-radius:12px"></video>'
          + _avRenderCaption(d)
          +'<div style="display:flex;gap:8px;margin-top:8px">'
          +'<a href="'+_dlUrl+'" download="'+_prettyAttr+'" class="btn btn-pri" style="font-size:.85em">⬇️ 下載影片</a>'
          +'<button class="btn" style="font-size:.85em;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5" '
          +'onclick="_avDeleteOutput(this,\''+d.rel_path+'\')">🗑️ 刪除影片</button>'
          +'<button class="btn btn-sec" style="font-size:.85em" '
          +'onclick="document.getElementById(\'tab-autovid\').scrollIntoView({behavior:\'smooth\'})">⬆️ 回到設定</button></div>';
      }else if(t.status === 'error'){
        tc.title.textContent = '❌ 失敗 — '+t.label;
        tc.title.style.color = 'var(--err)';
        tc.msg.textContent = '❌ '+(t.data.message||'');
        tc.msg.style.color = 'var(--err)';
        tc._finished = true;
        _avStopTimer(tc);
        if(tc.stopBtn){ tc.stopBtn.style.display = 'none'; }
      }else{
        // 進行中 — 重新連接 SSE
        tc.msg.textContent = '🔄 重新連線中...';
        _avListenSSE(t.task_id, tc, t.label);
      }
    });
    _avUpdateBatchDownloadBtn();
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
  bgmInjectOpts('hl', opts);
  _injectAdvOpts('hl', opts);
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

// 平台 chip：全選 / 全不選切換；chip 高亮
function _avPlatToggleAll(){
  const cbs = document.querySelectorAll('.av-plat');
  const allOn = Array.from(cbs).every(c=>c.checked);
  cbs.forEach(c=>{ c.checked = !allOn; });
  _avPlatRefreshChips();
}
function _avPlatRefreshChips(){
  document.querySelectorAll('.av-plat').forEach(cb=>{
    const lab = cb.closest('label');
    if(!lab) return;
    if(cb.checked){
      lab.style.background = 'var(--pri-bg, #dbeafe)';
      lab.style.borderColor = 'var(--pri, #3b82f6)';
      lab.style.color = 'var(--pri, #1d4ed8)';
    } else {
      lab.style.background = 'var(--bg2)';
      lab.style.borderColor = 'var(--brd)';
      lab.style.color = '';
    }
  });
}
document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelectorAll('.av-plat').forEach(cb=>{
    cb.addEventListener('change', _avPlatRefreshChips);
  });
  _avPlatRefreshChips();
});

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

// AV_GROUPS / AV_KO_NAMES 在 server-side 由 data/groups.json 注入
// Canonical source: data/groups.json（前後端共用的 single source of truth）
const AV_GROUPS = __AV_GROUPS_JSON__;

// 韓文藝名 — 從 data/groups.json 注入（server-side template substitution）
const AV_KO_NAMES = __AV_KO_NAMES_JSON__;

// ── 衝突名字：同名於多團時以 `Person_GroupSlug` 當 folder key ──
// AV_NAME_GROUPS: person → [group1, group2, ...] 供偵測衝突
const AV_NAME_GROUPS = {};
for(const [g, members] of Object.entries(AV_GROUPS)){
  for(const m of members){
    (AV_NAME_GROUPS[m] = AV_NAME_GROUPS[m] || []).push(g);
  }
}
function _avGroupSlug(g){ return (g||'').replace(/[^A-Za-z0-9]/g, ''); }

// 給 (person, group) 產 folder key：衝突名字才加後綴，其他保留原名
function _avPersonKey(person, group){
  if((AV_NAME_GROUPS[person] || []).length >= 2){
    return person + '_' + _avGroupSlug(group);
  }
  return person;
}

// 把 key 拆回 {person, group}；非衝突 key 維持 {key, ''}
function _avSplitKey(key){
  if(!key || !key.includes('_')) return { person: key || '', group: '' };
  const idx = key.lastIndexOf('_');
  const head = key.slice(0, idx);
  const slug = key.slice(idx + 1);
  if((AV_NAME_GROUPS[head] || []).length >= 2){
    for(const g of Object.keys(AV_GROUPS)){
      if(_avGroupSlug(g) === slug) return { person: head, group: g };
    }
  }
  return { person: key, group: '' };
}
function _avDisplayName(key){ return _avSplitKey(key).person; }

function _avFindGroup(key){
  const {person, group} = _avSplitKey(key);
  if(group) return group;  // key 已經編碼 group
  if(!person) return '';
  const p = person.toLowerCase();
  for(const [g, members] of Object.entries(AV_GROUPS)){
    if(members.some(m => m.toLowerCase() === p)) return g;
  }
  return '';
}

// 完成通知 — 永遠會嗶+閃標題+頁內橫幅；有權限時再加桌面通知
// (HTTP 非 localhost 站點 Chrome 會擋 Notification API，所以頁內提示是主要管道)
function _avBeep(){
  try{
    const ac = new (window.AudioContext||window.webkitAudioContext)();
    [880, 1320].forEach((hz, i) => {
      const o = ac.createOscillator(); const g = ac.createGain();
      o.frequency.value = hz; o.connect(g); g.connect(ac.destination);
      const t0 = ac.currentTime + i*0.18;
      g.gain.setValueAtTime(0.25, t0);
      g.gain.exponentialRampToValueAtTime(0.01, t0 + 0.25);
      o.start(t0); o.stop(t0 + 0.25);
    });
  }catch(_){}
}
let _avTitleFlashTimer = null;
function _avFlashTitle(msg){
  try{
    if(_avTitleFlashTimer){ clearInterval(_avTitleFlashTimer); _avTitleFlashTimer=null; }
    const orig = document.title;
    let on = false;
    _avTitleFlashTimer = setInterval(() => {
      document.title = on ? orig : ('🎉 ' + msg);
      on = !on;
    }, 900);
    // 停止條件：分頁重獲焦點 或 30 秒後自動停
    const stop = () => {
      if(_avTitleFlashTimer){ clearInterval(_avTitleFlashTimer); _avTitleFlashTimer=null; }
      document.title = orig;
      document.removeEventListener('visibilitychange', onVis);
    };
    const onVis = () => { if(document.visibilityState==='visible') stop(); };
    document.addEventListener('visibilitychange', onVis);
    setTimeout(stop, 30000);
  }catch(_){}
}
function _avToast(title, body, url){
  try{
    let host = document.getElementById('av-toast-host');
    if(!host){
      host = document.createElement('div');
      host.id = 'av-toast-host';
      host.style.cssText = 'position:fixed;top:16px;right:16px;z-index:99999;display:flex;flex-direction:column;gap:8px;max-width:360px;';
      document.body.appendChild(host);
    }
    const t = document.createElement('div');
    t.style.cssText = 'background:#1f6feb;color:#fff;padding:12px 14px;border-radius:8px;box-shadow:0 6px 20px rgba(0,0,0,.25);font:14px system-ui,sans-serif;cursor:pointer;opacity:0;transition:opacity .25s;';
    const titleEl = document.createElement('div');
    titleEl.style.cssText = 'font-weight:600;margin-bottom:4px;';
    titleEl.textContent = title;
    const bodyEl = document.createElement('div');
    bodyEl.style.cssText = 'font-size:13px;opacity:.95;word-break:break-all;';
    bodyEl.textContent = body || '';
    t.appendChild(titleEl); t.appendChild(bodyEl);
    const close = () => { t.style.opacity='0'; setTimeout(()=>t.remove(), 300); };
    t.onclick = () => { if(url){ window.open(url, '_blank'); } close(); };
    host.appendChild(t);
    requestAnimationFrame(() => { t.style.opacity='1'; });
    setTimeout(close, 12000);
  }catch(_){}
}
function _avNotify(title, body, url){
  _avBeep();
  _avFlashTitle(title);
  _avToast(title, body, url);
  try{
    if(!('Notification' in window)) return;
    const fire = () => {
      try{
        const n = new Notification(title, { body: body, tag: 'auto-video', icon: '/favicon.ico' });
        if(url){ n.onclick = () => { window.focus(); n.close(); }; }
      }catch(_){}
    };
    if(Notification.permission === 'granted'){ fire(); }
    else if(Notification.permission !== 'denied'){
      Notification.requestPermission().then(p => { if(p==='granted') fire(); });
    }
  }catch(_){}
}

function _avBuildDownloadUrl(fileUrl, personKey, filename){
  if(!fileUrl) return fileUrl;
  const ext = (filename && filename.includes('.')) ? filename.slice(filename.lastIndexOf('.')) : '.mp4';
  if(!personKey) return fileUrl;
  // 衝突名字：key 長 "Chaeyoung_TWICE" → 檔名、hashtag 都用顯示名
  const {person, group: groupFromKey} = _avSplitKey(personKey);
  const group = groupFromKey || _avFindGroup(person);
  // 韓文：先查完整 key（允許 `Chaeyoung_TWICE` 用不同韓文），沒有再 fallback 到顯示名
  const ko = AV_KO_NAMES[personKey]
    || AV_KO_NAMES[person]
    || AV_KO_NAMES[person.charAt(0).toUpperCase()+person.slice(1).toLowerCase()]
    || '';
  const parts = [person, '#'+person];
  if(group) parts.push('#'+group.toLowerCase());
  if(ko) parts.push('#'+ko);
  const pretty = parts.join(' ') + ext;
  const sep = fileUrl.includes('?') ? '&' : '?';
  return fileUrl + sep + 'dl=' + encodeURIComponent(pretty);
}

function avGroupChanged(){
  const g = document.getElementById('av-group').value;
  const sel = document.getElementById('av-person');
  sel.innerHTML = '<option value="">-- 選成員 --</option>';
  if(!g || !AV_GROUPS[g]) return;
  if(AV_GROUPS[g].length > 1){
    const allOpt = document.createElement('option');
    allOpt.value = '__ALL__'; allOpt.textContent = '全團員';
    sel.appendChild(allOpt);
  }
  for(const m of AV_GROUPS[g]){
    const opt = document.createElement('option');
    // 衝突名字（如 Chaeyoung）value 會變成 "Chaeyoung_TWICE"，顯示仍是 "Chaeyoung"
    opt.value = _avPersonKey(m, g);
    opt.textContent = m;
    sel.appendChild(opt);
  }
  if(AV_GROUPS[g].length === 1){ sel.value = _avPersonKey(AV_GROUPS[g][0], g); }
}

(function avInitGroupOptions(){
  const sel = document.getElementById('av-group');
  if(!sel) return;
  for(const g of Object.keys(AV_GROUPS)){
    const opt = document.createElement('option');
    opt.value = g; opt.textContent = g;
    sel.appendChild(opt);
  }
  sel.value = 'IVE';
  avGroupChanged();
  const psel = document.getElementById('av-person');
  const _leeseoKey = _avPersonKey('Leeseo', 'IVE');
  if(psel && [...psel.options].some(o=>o.value===_leeseoKey)) psel.value = _leeseoKey;
})();

async function autoVideoStart(preview){
  const person = document.getElementById('av-person').value.trim();
  const group  = document.getElementById('av-group').value.trim();
  const modeSel = document.getElementById('av-multi-mode');
  const multiMode = modeSel ? modeSel.value : 'parallel';

  // 「全團員」或無人名但有選團 → 多人執行（並行或依序，由 av-multi-mode 決定）
  if(!person || person === '__ALL__'){
    if(!group || !AV_GROUPS[group]){
      return alert('請選擇團體或成員');
    }
    const members = AV_GROUPS[group];
    const modeLabel = (multiMode === 'sequential') ? '依序（一個接一個）' : '並行（同時跑）';
    if(!confirm(`將為 ${group} 的 ${members.length} 位成員各生成一支影片\n執行模式：${modeLabel}\n\n確定？`)) return;

    if(multiMode === 'sequential'){
      // 依序：等前一位完成（或失敗）再啟動下一位
      for(const m of members){
        try{ await _avLaunchOne(_avPersonKey(m, group), preview); }
        catch(_){ /* 單一成員失敗不擋後續 */ }
      }
    } else {
      // 並行：一次全部啟動（沿用原行為）
      for(const m of members){
        _avLaunchOne(_avPersonKey(m, group), preview);
      }
    }
    return;
  }
  _avLaunchOne(person, preview);
}

// ============================================================
// 封面圖挑選：從照片資料庫挑一張當影片開場（TikTok/YT 會用它當縮圖）
// ============================================================
async function _avOpenCoverPicker(){
  const psel = document.getElementById('av-person');
  const gsel = document.getElementById('av-group');
  let person = (psel && psel.value || '').trim();
  const group = (gsel && gsel.value || '').trim();

  // 若選了「全團員」或空白，要求先指定一位成員（預設用團體的第一位）
  if(!person || person === '__ALL__'){
    if(group && AV_GROUPS[group] && AV_GROUPS[group].length){
      const first = AV_GROUPS[group][0];
      if(!confirm('目前選的是「全團員」模式，封面圖需要指定成員。\n要用「'+first+'」的照片庫來挑嗎？\n（封面會套用到每一位成員的影片）')) return;
      person = _avPersonKey(first, group);  // 衝突名字的 key
    } else {
      alert('請先在上方選擇成員（或團體），才能從對應的照片庫挑封面');
      return;
    }
  }

  // 建立/重用 modal
  let modal = document.getElementById('av-cover-modal');
  if(!modal){
    modal = document.createElement('div');
    modal.id = 'av-cover-modal';
    modal.className = 'modal';
    modal.innerHTML =
      '<div class="modal-box" style="max-width:900px">'
      + '<div class="modal-head">'
      +   '<h3 id="av-cover-modal-title">挑選封面圖</h3>'
      +   '<span id="av-cover-modal-count" style="font-size:.82em;color:var(--txt2);margin-left:8px"></span>'
      +   '<button class="modal-close" onclick="closeModal(\'av-cover-modal\')">&times;</button>'
      + '</div>'
      + '<div class="modal-body"><div id="av-cover-modal-grid" class="gallery"></div></div>'
      + '</div>';
    document.body.appendChild(modal);
  }
  document.getElementById('av-cover-modal-title').textContent = _avDisplayName(person) + ' — 挑選封面圖';
  const grid = document.getElementById('av-cover-modal-grid');
  grid.innerHTML = '<p style="color:var(--txt2);padding:20px">載入中...</p>';
  modal.classList.add('show');

  try{
    const r = await fetch('/api/images/' + encodeURIComponent(person));
    const imgs = await r.json();
    grid.innerHTML = '';
    document.getElementById('av-cover-modal-count').textContent = imgs.length + ' 張';
    if(!imgs.length){
      grid.innerHTML = '<p style="color:var(--txt2);padding:20px">此成員尚無照片，請先到照片庫下載</p>';
      return;
    }
    imgs.forEach(img => {
      const div = document.createElement('div');
      div.style.cssText = 'cursor:pointer;position:relative;transition:transform .15s';
      div.onmouseover = function(){ this.style.transform='scale(1.04)'; };
      div.onmouseout  = function(){ this.style.transform='scale(1)'; };
      div.innerHTML = '<img src="'+img.url+'" loading="lazy" style="pointer-events:none">'
        + '<div class="gallery-info">'+img.filename+' ('+img.size_kb+'KB)</div>';
      div.onclick = function(){
        // 記住挑選：用相對於資料庫的路徑（/photos/<celeb>/<file>），後端會解析
        // 注意：不對 person 做 urlencode，後端是直接以 JSON 字串比對檔案系統路徑
        const hidden = document.getElementById('av-cover-filename');
        hidden.value = '/photos/' + person + '/' + img.filename;
        hidden.dataset.person = person;
        // 更新狀態 pill
        const thumb = document.getElementById('av-cover-thumb');
        thumb.src = img.url;
        thumb.style.display = '';
        document.getElementById('av-cover-status-icon').textContent = '🖼️';
        document.getElementById('av-cover-status-text').textContent = _avDisplayName(person) + ' / ' + img.filename;
        document.getElementById('av-cover-status').style.background = 'rgba(100,150,255,0.12)';
        document.getElementById('av-cover-status').style.borderStyle = 'solid';
        document.getElementById('av-cover-status').style.borderColor = 'var(--pri)';
        document.getElementById('av-cover-reset').style.display = '';
        closeModal('av-cover-modal');
      };
      grid.appendChild(div);
    });
  }catch(e){
    grid.innerHTML = '<p style="color:var(--err);padding:20px">載入失敗：'+e+'</p>';
  }
}

function _avClearCover(){
  // 恢復成「自動挑」模式
  const hidden = document.getElementById('av-cover-filename');
  hidden.value = '__auto__';
  delete hidden.dataset.person;
  const thumb = document.getElementById('av-cover-thumb');
  thumb.src = '';
  thumb.style.display = 'none';
  document.getElementById('av-cover-status-icon').textContent = '🎲';
  document.getElementById('av-cover-status-text').textContent = '自動挑（隨機一張）';
  const pill = document.getElementById('av-cover-status');
  pill.style.background = 'rgba(100,200,100,0.12)';
  pill.style.borderStyle = 'dashed';
  pill.style.borderColor = 'rgba(100,200,100,0.4)';
  document.getElementById('av-cover-reset').style.display = 'none';
}

async function _avLaunchOne(person, preview){
  // 第一次啟動任務時順便請求通知權限
  try{
    if('Notification' in window && Notification.permission === 'default'){
      Notification.requestPermission();
    }
  }catch(_){}
  const source = document.getElementById('av-source').value;
  const keyword = document.getElementById('av-keyword').value.trim();
  const autoMode = document.getElementById('av-auto-mode').checked;
  const outputPreset = document.getElementById('av-output-preset').value;

  // 封面圖：預設為 __auto__（自動挑第一張照片）
  const _coverHidden = document.getElementById('av-cover-filename');
  const _coverVal = _coverHidden ? (_coverHidden.value || '').trim() : '';
  // 注意：不能用 `|| 1.0` 因為 0 是合法值（僅縮圖模式）
  let _coverDur = 0;
  try{
    const _cd = document.getElementById('av-cover-dur');
    if(_cd){
      const _parsed = parseFloat(_cd.value);
      _coverDur = isNaN(_parsed) ? 0 : _parsed;
    }
  }catch(_){}

  // 平台：checkbox chip 組，複選時送 platforms[] 陣列；一個時送 platform 字串相容舊 API
  const _platChecked = Array.from(document.querySelectorAll('.av-plat:checked')).map(cb=>cb.value);
  const _platSingle = _platChecked.length === 1 ? _platChecked[0] : (_platChecked.length === 0 ? 'yt_shorts' : 'all');
  const opts = {
    person: person,
    source: source,
    search_keyword: keyword,
    platform: _platSingle,
    platforms: _platChecked,
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
    cover_photo: _coverVal,
    cover_duration: _coverDur,
  };
  bgmInjectOpts('av', opts);
  _injectAdvOpts('av', opts);

  // 顯示用脫掉衝突後綴（`Chaeyoung_TWICE` → `Chaeyoung (TWICE)`），API 仍傳完整 key
  const _disp = _avDisplayName(person);
  const _grpLabel = (_avSplitKey(person).group) ? ' ('+_avSplitKey(person).group+')' : '';
  const taskLabel = _disp + _grpLabel + (keyword ? ' ('+keyword+')' : '') + (preview ? ' [挑選模式]' : '');
  const taskCard = _avCreateTaskCard(taskLabel);
  taskCard.person = person;
  document.getElementById('av-tasks-container').prepend(taskCard.card);
  taskCard.card.scrollIntoView({behavior:'smooth', block:'center'});

  // 回傳 Promise：在 SSE 的 done / error / 連線中斷時 resolve，讓依序模式能 await
  return new Promise(async (resolve)=>{
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
        return resolve({ok:false, error:d.error});
      }
      _avListenSSE(d.task_id, taskCard, taskLabel, resolve);
    }catch(e){
      taskCard.msg.textContent = '連線失敗: '+e;
      taskCard.msg.style.color = 'var(--err)';
      resolve({ok:false, error:String(e)});
    }
  });
}

let _avTaskCounter = 0;
const _avPhaseLabels = {
  search: '🔍 搜尋中',
  download: '📥 下載影片中',
  photos: '📸 下載照片中',
  extract: '🎯 擷取中',
  highlight: '🎬 剪輯中',
};

function _avFormatElapsed(sec){
  sec = Math.max(0, Math.floor(sec));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  const pad = (n) => String(n).padStart(2, '0');
  return h > 0 ? (h + ':' + pad(m) + ':' + pad(s)) : (pad(m) + ':' + pad(s));
}

async function _avStopOne(taskId, btn){
  if(!taskId) return;
  try{
    if(btn){ btn.disabled = true; btn.textContent = '⏳ 中止中...'; }
    const r = await fetch('/api/auto-video/stop/' + encodeURIComponent(taskId), {method:'POST'});
    const d = await r.json().catch(()=>({}));
    if(!r.ok || d.error){
      if(btn){ btn.disabled = false; btn.textContent = '⏹ 中止'; }
      alert('中止失敗：' + (d.error || r.status));
    }
    // 成功時 SSE 會收到 cancelled 事件並更新卡片
  }catch(e){
    if(btn){ btn.disabled = false; btn.textContent = '⏹ 中止'; }
    alert('中止失敗：' + e);
  }
}

async function _avStopAll(){
  if(!confirm('中止所有進行中的一鍵影片任務？')) return;
  const allBtn = document.getElementById('btn-av-stop-all');
  try{
    if(allBtn){ allBtn.disabled = true; allBtn.textContent = '⏳ 中止中...'; }
    const r = await fetch('/api/auto-video/stop-all', {method:'POST'});
    const d = await r.json().catch(()=>({}));
    if(!r.ok){
      alert('中止失敗：' + (d.error || r.status));
    }
  }catch(e){
    alert('中止失敗：' + e);
  }finally{
    if(allBtn){
      setTimeout(()=>{
        allBtn.disabled = false;
        allBtn.textContent = '⏹ 一鍵中止';
      }, 1500);
    }
  }
}

// 追蹤當前畫面上所有 task 卡片（running + finished），供「一鍵下載全部」判斷批次是否全部完成
const _avAllCards = new Set();

function _avUpdateBatchDownloadBtn(){
  const btn = document.getElementById('btn-av-download-all');
  if(!btn) return;
  if(_avAllCards.size === 0){ btn.style.display = 'none'; return; }
  let allDone = true, successCount = 0;
  for(const tc of _avAllCards){
    if(!tc._finished){ allDone = false; }
    else if(tc._downloadUrl){ successCount++; }
  }
  if(allDone && successCount > 0){
    btn.style.display = '';
    btn.disabled = false;
    btn.textContent = '⬇ 一鍵下載全部 (' + successCount + ')';
  }else{
    btn.style.display = 'none';
  }
}

async function _avDownloadAllAndClear(){
  const btn = document.getElementById('btn-av-download-all');
  const cards = Array.from(_avAllCards).filter(tc => tc._finished && tc._downloadUrl);
  if(cards.length === 0){
    if(btn) btn.style.display = 'none';
    return;
  }
  if(btn){ btn.disabled = true; btn.textContent = '⏳ 下載中 0/' + cards.length; }
  for(let i = 0; i < cards.length; i++){
    const tc = cards[i];
    if(btn){ btn.textContent = '⏳ 下載中 ' + (i+1) + '/' + cards.length; }
    try{
      const a = document.createElement('a');
      a.href = tc._downloadUrl;
      if(tc._downloadName){ a.download = tc._downloadName; }
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }catch(_){ }
    // 間隔避免瀏覽器阻擋多重下載
    await new Promise(r => setTimeout(r, 500));
  }
  // 給瀏覽器一點時間啟動最後一個下載，再清除卡片
  await new Promise(r => setTimeout(r, 600));
  for(const tc of cards){
    const card = tc.card;
    if(card){
      card.style.transition = 'opacity .3s, transform .3s';
      card.style.opacity = '0';
      card.style.transform = 'translateX(30px)';
    }
    _avAllCards.delete(tc);
  }
  setTimeout(() => {
    for(const tc of cards){
      if(tc.card && tc.card.parentNode) tc.card.parentNode.removeChild(tc.card);
    }
    _avUpdateBatchDownloadBtn();
  }, 350);
}

function _avStartTimer(tc){
  if(tc._timerInterval) return;
  tc._startAt = Date.now();
  const _tick = () => {
    if(!tc._startAt || tc._timerStopped) return;
    const el = tc.timer;
    if(el){ el.textContent = '⏱ ' + _avFormatElapsed((Date.now() - tc._startAt)/1000); }
  };
  _tick();
  tc._timerInterval = setInterval(_tick, 1000);
}

function _avStopTimer(tc){
  tc._timerStopped = true;
  if(tc._timerInterval){ clearInterval(tc._timerInterval); tc._timerInterval = null; }
}

function _avCreateTaskCard(label){
  _avTaskCounter++;
  const id = 'av-task-'+_avTaskCounter;
  const card = document.createElement('div');
  card.className = 'card';
  card.id = id;
  card.style.cssText = 'margin-bottom:12px;position:relative';
  card.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;gap:8px;flex-wrap:wrap">'
    +'<h3 class="av-task-title" style="margin:0;flex:1;min-width:200px">⏳ '+label+'</h3>'
    +'<span class="av-task-timer" style="font-family:Consolas,monospace;font-size:.88em;color:var(--txt2);padding:2px 8px;background:rgba(100,150,255,0.10);border-radius:6px">⏱ 00:00</span>'
    +'<button class="av-task-stop btn" style="font-size:.78em;padding:4px 10px;background:#fee2e2;color:#b91c1c;border:1px solid #fca5a5;border-radius:6px;cursor:pointer" title="中止此任務">⏹ 中止</button>'
    +'<button class="av-task-close" '
    +'style="background:none;border:none;font-size:1.3em;cursor:pointer;color:var(--txt2);padding:0 4px;line-height:1" title="關閉（不會中止後端任務）">&times;</button></div>'
    +'<div class="progress-wrap"><div class="progress-bar av-task-bar" style="width:0%"></div>'
    +'<span class="progress-text av-task-pct">0%</span></div>'
    +'<div class="av-task-msg" style="font-size:.88em;color:var(--txt2);margin-top:8px">啟動中...</div>'
    +'<div class="av-task-log" style="max-height:150px;overflow-y:auto;font-family:Consolas,monospace;font-size:.75em;line-height:1.7;padding:6px;background:#F9FAFB;border-radius:8px;margin-top:8px"></div>'
    +'<div class="av-task-result" style="display:none;margin-top:10px"></div>';
  const tc = {
    card: card,
    title: card.querySelector('.av-task-title'),
    bar: card.querySelector('.av-task-bar'),
    pct: card.querySelector('.av-task-pct'),
    msg: card.querySelector('.av-task-msg'),
    log: card.querySelector('.av-task-log'),
    result: card.querySelector('.av-task-result'),
    timer: card.querySelector('.av-task-timer'),
    stopBtn: card.querySelector('.av-task-stop'),
    closeBtn: card.querySelector('.av-task-close'),
    _taskId: '',
    _startAt: 0,
    _timerInterval: null,
    _timerStopped: false,
    _finished: false,
  };
  // 中止按鈕：呼叫後端 stop API（task_id 在 _avListenSSE 拿到時綁上）
  tc.stopBtn.onclick = () => { _avStopOne(tc._taskId, tc.stopBtn); };
  // 關閉按鈕：若還在跑，先問要不要中止；否則直接隱藏卡片 + 通知後端清掉歷史
  tc.closeBtn.onclick = () => {
    if(!tc._finished && tc._taskId){
      if(confirm('任務還在跑，要中止並關閉嗎？\n（按取消只會關 UI 卡片，後端仍會繼續跑）')){
        _avStopOne(tc._taskId, tc.stopBtn);
        // 後端回 cancelled 後 SSE 會 mark finished，這時再讓使用者手動關閉
        return;
      }
    }
    // 已完成 / 失敗：叫後端把任務從歷史刪掉，刷新後才不會又跑出來
    if(tc._finished && tc._taskId){
      fetch('/api/auto-video/dismiss/'+tc._taskId, {method:'POST'}).catch(()=>{});
    }
    _avAllCards.delete(tc);
    card.style.transition = 'opacity .3s,transform .3s';
    card.style.opacity = '0';
    card.style.transform = 'translateX(30px)';
    setTimeout(() => { card.remove(); _avUpdateBatchDownloadBtn(); }, 300);
  };
  _avStartTimer(tc);
  _avAllCards.add(tc);
  _avUpdateBatchDownloadBtn();
  return tc;
}

function _avListenSSE(taskId, tc, taskLabel, onFinish){
  let lastPhase = '';
  tc._taskId = taskId;
  const es = new EventSource('/api/auto-video/progress/'+taskId);
  let _finished = false;
  const _finish = (result)=>{
    if(_finished) return;
    _finished = true;
    tc._finished = true;
    _avStopTimer(tc);
    if(tc.stopBtn){ tc.stopBtn.style.display = 'none'; }
    if(typeof onFinish === 'function'){
      try{ onFinish(result); }catch(_){}
    }
    try{ _avUpdateBatchDownloadBtn(); }catch(_){}
  };
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
        _avNotify('🎉 影片完成', taskLabel + ' — ' + (d.filename||''), d.file_url);
        const _dlUrl = _avBuildDownloadUrl(d.file_url, tc.person, d.filename);
        // 顯示漂亮的下載檔名；點檔名連結也觸發下載（走 _dlUrl）
        const _prettyName = (() => {
          try{
            const u = new URL(_dlUrl, window.location.origin);
            return u.searchParams.get('dl') || d.filename;
          }catch(_){ return d.filename; }
        })();
        // 存到卡片上，供「⬇ 一鍵下載全部」使用
        tc._downloadUrl = _dlUrl;
        tc._downloadName = _prettyName;
        // HTML 屬性用的 escape（替換 " 和 &）
        const _prettyAttr = _prettyName.replace(/&/g,'&amp;').replace(/"/g,'&quot;');
        tc.msg.innerHTML =
          '✅ <a href="'+_dlUrl+'" download="'+_prettyAttr+'" target="_blank" style="color:var(--pri);font-weight:600">'+_prettyName+'</a>'
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
          +'<a href="'+_dlUrl+'" download="'+_prettyAttr+'" class="btn btn-pri" style="font-size:.85em">⬇️ 下載影片</a>'
          +'<button class="btn" style="font-size:.85em;background:#fee2e2;color:#dc2626;border:1px solid #fca5a5" '
          +'onclick="_avDeleteOutput(this,\''+d.rel_path+'\')">🗑️ 刪除影片</button>'
          +'<button class="btn btn-sec" style="font-size:.85em" '
          +'onclick="document.getElementById(\'tab-autovid\').scrollIntoView({behavior:\'smooth\'})">⬆️ 回到設定</button></div>';
        const dline = document.createElement('div');
        dline.style.color = 'var(--ok)';
        dline.style.fontWeight = '600';
        dline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ✅ 完成！'+d.filename;
        tc.log.appendChild(dline);
        _finish({ok:true, filename:d.filename, file_url:d.file_url});
        break;
      case 'cancelled':
        es.close();
        tc.title.textContent = '⏹ 已中止 — '+taskLabel;
        tc.title.style.color = 'var(--warn)';
        tc.msg.textContent = '⏹ ' + (d.message || '使用者中止');
        tc.msg.style.color = 'var(--warn)';
        const cline = document.createElement('div');
        cline.style.color = 'var(--warn)';
        cline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ⏹ '+(d.message||'使用者中止');
        tc.log.appendChild(cline);
        _finish({ok:false, cancelled:true});
        break;
      case 'error':
        es.close();
        // 中止時後端會先送 cancelled 再送 error，避免 UI 顯示紅色失敗
        if(tc._finished){ break; }
        tc.title.textContent = '❌ 失敗 — '+taskLabel;
        tc.title.style.color = 'var(--err)';
        tc.msg.textContent = '❌ '+d.message;
        tc.msg.style.color = 'var(--err)';
        const eline = document.createElement('div');
        eline.style.color = 'var(--err)';
        eline.textContent = '['+new Date().toTimeString().slice(0,8)+'] ❌ '+d.message;
        tc.log.appendChild(eline);
        _finish({ok:false, error:d.message});
        break;
      case 'heartbeat': break;
    }
  };
  es.onerror = ()=>{
    es.close();
    tc.title.textContent = '⚠️ 連線中斷 — '+taskLabel;
    _finish({ok:false, error:'SSE 連線中斷'});
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
  bgmInjectOpts('cb', opts);
  _injectAdvOpts('cb', opts);

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
    # log 跟其他 runtime 資料一起放在 APP_DATA_DIR（預設就是 source 旁邊）
    _log_path = os.environ.get("LOG_PATH", os.path.join(APP_DATA_DIR, "web_app.log"))
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
