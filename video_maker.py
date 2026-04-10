# -*- coding: utf-8 -*-
"""
影片模板生成器 — 6 種 2025/2026 TikTok 潮流模板
使用 Pillow 合成畫面 + FFmpeg 編碼
"""

import os, sys, re, math, random, subprocess, threading, tempfile, shutil
import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps, ImageChops, ImageEnhance

try:
    import cv2
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

try:
    import imageio_ffmpeg
    FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
except ImportError:
    FFMPEG = "ffmpeg"

# ── 人臉偵測 ────────────────────────────────────────────
_face_cascade = None


def _get_face_cascade():
    global _face_cascade
    if _face_cascade is None and HAS_CV2:
        cascade_path = os.path.join(
            os.path.dirname(cv2.__file__), "data",
            "haarcascade_frontalface_default.xml"
        )
        if os.path.isfile(cascade_path):
            _face_cascade = cv2.CascadeClassifier(cascade_path)
    return _face_cascade


def _detect_face_center(pil_img):
    cascade = _get_face_cascade()
    if cascade is None:
        return None
    w, h = pil_img.size
    scale = min(1.0, 600 / max(w, h))
    sw, sh = int(w * scale), int(h * scale)
    small = pil_img.resize((sw, sh), Image.LANCZOS)
    gray = cv2.cvtColor(np.array(small), cv2.COLOR_RGB2GRAY)
    faces = cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
    if len(faces) == 0:
        return None
    areas = [fw * fh for (_, _, fw, fh) in faces]
    idx = int(np.argmax(areas))
    fx, fy, fw, fh = faces[idx]
    cx = (fx + fw / 2) / scale
    cy = (fy + fh / 2) / scale
    return (cx, cy)


# ── 畫面設定 ────────────────────────────────────────────
W, H = 1080, 1920  # 9:16
FPS = 30

# ── Windows 字型路徑 ────────────────────────────────────
_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\msjh.ttc",
    r"C:\Windows\Fonts\msyh.ttc",
    r"C:\Windows\Fonts\arial.ttf",
    r"C:\Windows\Fonts\segoeui.ttf",
]
_FONT_SERIF_CANDIDATES = [
    r"C:\Windows\Fonts\georgia.ttf",
    r"C:\Windows\Fonts\times.ttf",
    r"C:\Windows\Fonts\timesbd.ttf",
]
_FONT_BOLD_CANDIDATES = [
    r"C:\Windows\Fonts\impact.ttf",
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\msjhbd.ttc",
    r"C:\Windows\Fonts\msyhbd.ttc",
]


def _find_font(candidates):
    for p in candidates:
        if os.path.isfile(p):
            return p
    return None


FONT_PATH = _find_font(_FONT_CANDIDATES)
FONT_SERIF_PATH = _find_font(_FONT_SERIF_CANDIDATES) or FONT_PATH
FONT_BOLD_PATH = _find_font(_FONT_BOLD_CANDIDATES) or FONT_PATH


def _font(size, bold=False, serif=False):
    path = FONT_SERIF_PATH if serif else (FONT_BOLD_PATH if bold else FONT_PATH)
    if path:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            pass
    return ImageFont.load_default()


# ── 模板清單 ────────────────────────────────────────────
TEMPLATES = {
    "velocity": {
        "id": "velocity", "name": "Velocity 節拍變速", "icon": "🔥",
        "desc": "慢推→瞬間閃切＋動態模糊，TikTok fan edit 經典風格",
        "dur_per_photo": 1.5, "min_photos": 6,
    },
    "parallax_3d": {
        "id": "parallax_3d", "name": "3D 視差效果", "icon": "📐",
        "desc": "前景人物浮起＋背景反向移動，CapCut 爆紅 3D Zoom",
        "dur_per_photo": 2.5, "min_photos": 3,
    },
    "film_vhs": {
        "id": "film_vhs", "name": "底片 / VHS 復古", "icon": "🎞️",
        "desc": "膠片顆粒＋光暈漏光＋掃描線＋復古色調",
        "dur_per_photo": 2.5, "min_photos": 3,
    },
    "rgb_glitch": {
        "id": "rgb_glitch", "name": "RGB 色散故障", "icon": "💫",
        "desc": "色版偏移閃爍＋半色調＋高對比，時尚雜誌質感",
        "dur_per_photo": 1.2, "min_photos": 5,
    },
    "cinema": {
        "id": "cinema", "name": "電影預告片", "icon": "🎬",
        "desc": "寬銀幕黑邊＋青橙調色＋打字字幕，電影主角感",
        "dur_per_photo": 3.0, "min_photos": 3,
    },
    "heartbeat": {
        "id": "heartbeat", "name": "心跳脈動", "icon": "❤️",
        "desc": "照片跟節奏放大縮小＋暗角呼吸＋閃白切換",
        "dur_per_photo": 1.5, "min_photos": 5,
    },
}


# ══════════════════════════════════════════════════════════
#  工具函式
# ══════════════════════════════════════════════════════════
def _collect_photos(photo_dir, max_photos=12, shuffle=True):
    MIN_DIM = 400
    exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}
    candidates = []
    for f in sorted(os.listdir(photo_dir)):
        if f.startswith("__"):
            continue
        fp = os.path.join(photo_dir, f)
        if not (os.path.isfile(fp) and os.path.splitext(f)[1].lower() in exts):
            continue
        try:
            with Image.open(fp) as img:
                w, h = img.size
        except Exception:
            continue
        if w < MIN_DIM or h < MIN_DIM:
            continue
        candidates.append({"path": fp, "w": w, "h": h})

    if not candidates:
        return []

    if len(candidates) > max_photos and HAS_CV2:
        cascade = _get_face_cascade()
        if cascade is not None:
            for c in candidates:
                try:
                    img = Image.open(c["path"]).convert("RGB")
                    scale = min(1.0, 400 / max(img.size))
                    small = img.resize(
                        (int(img.size[0] * scale), int(img.size[1] * scale)),
                        Image.LANCZOS,
                    )
                    gray = cv2.cvtColor(np.array(small), cv2.COLOR_RGB2GRAY)
                    faces = cascade.detectMultiScale(
                        gray, scaleFactor=1.1, minNeighbors=5, minSize=(25, 25)
                    )
                    c["has_face"] = len(faces) > 0
                    if len(faces) > 0:
                        areas = [fw * fh for (_, _, fw, fh) in faces]
                        c["face_area"] = max(areas) / (small.size[0] * small.size[1])
                    else:
                        c["face_area"] = 0
                except Exception:
                    c["has_face"] = False
                    c["face_area"] = 0
        else:
            for c in candidates:
                c["has_face"] = False
                c["face_area"] = 0
    else:
        for c in candidates:
            c["has_face"] = False
            c["face_area"] = 0

    for c in candidates:
        pixels = c["w"] * c["h"]
        score = 0
        if c["has_face"]:
            score += 100
            score += c["face_area"] * 50
        score += min(pixels / 1_000_000, 30)
        ratio = c["h"] / max(c["w"], 1)
        if 1.2 < ratio < 2.2:
            score += 10
        c["score"] = score

    candidates.sort(key=lambda c: c["score"], reverse=True)
    top = candidates[:max_photos * 2]
    if shuffle:
        random.shuffle(top)
    selected = top[:max_photos]
    return [c["path"] for c in selected]


def _load_image(path, max_dim=2400):
    img = Image.open(path).convert("RGB")
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim), Image.LANCZOS)
    return img


def _load_with_face(path, max_dim=2400):
    img = _load_image(path, max_dim)
    fc = _detect_face_center(img)
    return img, fc


def _fit_cover(img, w, h, face_center=None):
    iw, ih = img.size
    target_ratio = w / h
    img_ratio = iw / ih
    if img_ratio > target_ratio:
        new_h = ih
        new_w = int(ih * target_ratio)
        if face_center:
            cx = face_center[0]
            left = int(cx - new_w / 2)
            left = max(0, min(left, iw - new_w))
        else:
            left = (iw - new_w) // 2
        top = 0
    else:
        new_w = iw
        new_h = int(iw / target_ratio)
        if face_center:
            cy = face_center[1]
            top = int(cy - new_h / 2)
            top = max(0, min(top, ih - new_h))
        else:
            top = (ih - new_h) // 2
        left = 0
    cropped = img.crop((left, top, left + new_w, top + new_h))
    return cropped.resize((w, h), Image.LANCZOS)


def _fit_contain(img, w, h):
    return ImageOps.contain(img, (w, h), Image.LANCZOS)


def _ease_out_cubic(t):
    return 1 - (1 - t) ** 3


def _ease_in_out_cubic(t):
    if t < 0.5:
        return 4 * t * t * t
    else:
        return 1 - (-2 * t + 2) ** 3 / 2


def _lerp(a, b, t):
    return a + (b - a) * t


# ── 通用特效工具 ─────────────────────────────────────────

def _add_vignette(img, strength=0.6):
    """加暗角"""
    w, h = img.size
    vig = Image.new("L", (w, h), 0)
    draw = ImageDraw.Draw(vig)
    cx, cy = w // 2, h // 2
    max_r = math.sqrt(cx * cx + cy * cy)
    steps = 30
    for i in range(steps):
        r = max_r * (1 - i / steps)
        alpha = int(255 * (1 - (i / steps) ** 1.5) * strength)
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=alpha)
    black = Image.new("RGB", (w, h), (0, 0, 0))
    vig_inv = ImageOps.invert(vig)
    return Image.composite(img, black, vig_inv)


def _add_film_grain(img, intensity=25):
    """加膠片顆粒雜訊"""
    arr = np.array(img, dtype=np.int16)
    noise = np.random.normal(0, intensity, arr.shape).astype(np.int16)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    return Image.fromarray(arr)


def _add_scanlines(img, spacing=3, opacity=40):
    """加掃描線"""
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    for y in range(0, img.size[1], spacing):
        draw.line([(0, y), (img.size[0], y)], fill=(0, 0, 0, opacity))
    return Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")


def _teal_orange_grade(img, strength=0.5):
    """青橙調色（電影感）"""
    arr = np.array(img, dtype=np.float32)
    # 暗部偏青 (加 B, 減 R)
    dark_mask = (arr.mean(axis=2, keepdims=True) < 100).astype(np.float32)
    arr[:, :, 0] -= 15 * strength * dark_mask[:, :, 0]  # R -
    arr[:, :, 2] += 20 * strength * dark_mask[:, :, 0]  # B +
    # 亮部偏暖 (加 R, 減 B)
    light_mask = (arr.mean(axis=2, keepdims=True) > 150).astype(np.float32)
    arr[:, :, 0] += 15 * strength * light_mask[:, :, 0]  # R +
    arr[:, :, 2] -= 10 * strength * light_mask[:, :, 0]  # B -
    arr = np.clip(arr, 0, 255).astype(np.uint8)
    return Image.fromarray(arr)


def _motion_blur_h(img, radius=30):
    """水平動態模糊"""
    kernel_size = radius * 2 + 1
    kernel = [1.0 / kernel_size] * kernel_size
    k = ImageFilter.Kernel(
        size=(kernel_size, 1), kernel=kernel, scale=1, offset=0
    )
    return img.filter(k)


def _rgb_split(img, offset_x=8, offset_y=4):
    """RGB 色版偏移"""
    r, g, b = img.split()
    r = ImageChops.offset(r, offset_x, offset_y)
    b = ImageChops.offset(b, -offset_x, -offset_y)
    return Image.merge("RGB", (r, g, b))


def _make_light_leak(w, h):
    """產生隨機光暈漏光"""
    leak = Image.new("RGB", (w, h), (0, 0, 0))
    draw = ImageDraw.Draw(leak)
    # 隨機幾個暖色光斑
    for _ in range(random.randint(2, 4)):
        cx = random.randint(-w // 4, w + w // 4)
        cy = random.randint(-h // 4, h + h // 4)
        radius = random.randint(w // 4, w // 2)
        r = random.randint(200, 255)
        g = random.randint(100, 200)
        b = random.randint(20, 80)
        for i in range(20):
            rr = int(radius * (1 - i / 20))
            alpha_val = int(40 * (1 - i / 20))
            color = (min(r, 255), min(g, 255), min(b, 255))
            draw.ellipse([cx - rr, cy - rr, cx + rr, cy + rr], fill=color)
    leak = leak.filter(ImageFilter.GaussianBlur(radius=80))
    return leak


# ══════════════════════════════════════════════════════════
#  FFmpeg 管線編碼器
# ══════════════════════════════════════════════════════════
class FrameEncoder:
    def __init__(self, output_path, width=W, height=H, fps=FPS):
        self.output_path = output_path
        self.width = width
        self.height = height
        self.fps = fps
        self.proc = None
        self._stderr_lines = []
        self._stderr_thread = None

    def start(self):
        cmd = [
            FFMPEG, "-y",
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{self.width}x{self.height}",
            "-r", str(self.fps),
            "-i", "pipe:0",
            "-c:v", "libx264", "-preset", "medium", "-crf", "20",
            "-pix_fmt", "yuv420p", "-r", str(self.fps),
            "-movflags", "+faststart",
            self.output_path,
        ]
        self.proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self._stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self._stderr_thread.start()

    def _read_stderr(self):
        for line in self.proc.stderr:
            self._stderr_lines.append(line.decode("utf-8", errors="replace").strip())

    def write_frame(self, img):
        if img.size != (self.width, self.height):
            img = img.resize((self.width, self.height), Image.LANCZOS)
        self.proc.stdin.write(img.tobytes())

    def finish(self):
        self.proc.stdin.close()
        self.proc.wait()
        if self._stderr_thread:
            self._stderr_thread.join(timeout=5)
        return self.proc.returncode == 0


# ══════════════════════════════════════════════════════════
#  模板 1: 🔥 Velocity 節拍變速
#  慢推 → 白閃瞬切 → 動態模糊過渡
# ══════════════════════════════════════════════════════════
def _tpl_velocity(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 1.5)
    # 每張照片：慢推階段 + 快閃切換階段
    slow_ratio = 0.75  # 75% 時間慢推
    fast_ratio = 0.25  # 25% 時間快速切換
    slow_frames = max(int(dur * slow_ratio * FPS), 6)
    fast_frames = max(int(dur * fast_ratio * FPS), 3)
    total_frames = (slow_frames + fast_frames) * len(photos)

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    # 預載圖片
    prepped = []
    for p in photos:
        img, fc = _load_with_face(p)
        big = _fit_cover(img, int(W * 1.3), int(H * 1.3), face_center=fc)
        prepped.append(big)

    for i, big in enumerate(prepped):
        bw, bh = big.size

        # ── 慢推階段：緩慢 zoom in，帶暗角 ──
        for f in range(slow_frames):
            t = f / max(slow_frames - 1, 1)
            t_ease = _ease_in_out_cubic(t)
            # 緩慢 zoom 1.0 → 1.15
            scale = _lerp(1.0, 1.15, t_ease)
            cw = int(W / scale)
            ch = int(H / scale)
            cx = (bw - cw) // 2
            cy = (bh - ch) // 2
            frame = big.crop((cx, cy, cx + cw, cy + ch)).resize((W, H), Image.LANCZOS)
            # 漸進暗角 (越接近切換越重)
            vig_str = _lerp(0.2, 0.55, t)
            frame = _add_vignette(frame, vig_str)
            enc.write_frame(frame)
            frame_idx += 1

        # ── 快閃切換階段：白閃 + 動態模糊 ──
        if i < len(prepped) - 1:
            next_big = prepped[i + 1]
            nbw, nbh = next_big.size
            for f in range(fast_frames):
                t = f / max(fast_frames - 1, 1)
                if t < 0.35:
                    # 白閃 (從當前圖片 blend 到白色)
                    alpha = t / 0.35
                    curr_frame = _fit_cover(big, W, H)
                    white = Image.new("RGB", (W, H), (255, 255, 255))
                    frame = Image.blend(curr_frame, white, min(alpha * 0.8, 0.8))
                elif t < 0.55:
                    # 純白 / 高亮過渡
                    frame = Image.new("RGB", (W, H), (245, 245, 250))
                else:
                    # 下一張淡入 + 動態模糊逐漸消失
                    alpha = (t - 0.55) / 0.45
                    next_frame = _fit_cover(next_big, W, H)
                    # 動態模糊 (越後面越清晰)
                    blur_r = max(int(20 * (1 - alpha)), 1)
                    try:
                        blurred = _motion_blur_h(next_frame, blur_r)
                    except Exception:
                        blurred = next_frame
                    frame = Image.blend(
                        Image.new("RGB", (W, H), (245, 245, 250)),
                        blurred,
                        _ease_out_cubic(alpha)
                    )
                enc.write_frame(frame)
                frame_idx += 1
        else:
            # 最後一張：fade out
            for f in range(fast_frames):
                t = f / max(fast_frames - 1, 1)
                curr_frame = _fit_cover(big, W, H)
                black = Image.new("RGB", (W, H), (0, 0, 0))
                frame = Image.blend(curr_frame, black, _ease_out_cubic(t))
                enc.write_frame(frame)
                frame_idx += 1

        if progress_cb:
            progress_cb(5 + frame_idx / total_frames * 85, f"Velocity: {i+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  模板 2: 📐 3D 視差效果
#  前景人物微放大 + 背景反向平移 + 景深模糊
# ══════════════════════════════════════════════════════════
def _tpl_parallax_3d(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 2.5)
    hold_frames = int(dur * FPS)
    trans_frames = int(0.4 * FPS)  # 轉場
    total_frames = hold_frames * len(photos)

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    for pi, p in enumerate(photos):
        img, fc = _load_with_face(p)
        # 背景層：模糊 + 稍大
        bg_base = _fit_cover(img, int(W * 1.25), int(H * 1.25), face_center=fc)
        bg_blur = bg_base.filter(ImageFilter.GaussianBlur(radius=12))

        # 前景層：正常裁切
        fg = _fit_cover(img, W, H, face_center=fc)

        # 方向：交替左右
        direction = 1 if pi % 2 == 0 else -1

        for f in range(hold_frames):
            t = f / max(hold_frames - 1, 1)
            t_ease = _ease_in_out_cubic(t)

            # 背景反向平移
            bg_shift_x = int(direction * _lerp(-30, 30, t_ease))
            bbw, bbh = bg_blur.size
            crop_x = (bbw - W) // 2 + bg_shift_x
            crop_y = (bbh - H) // 2
            crop_x = max(0, min(crop_x, bbw - W))
            crop_y = max(0, min(crop_y, bbh - H))
            bg_frame = bg_blur.crop((crop_x, crop_y, crop_x + W, crop_y + H))
            # 背景暗化
            bg_frame = Image.blend(bg_frame, Image.new("RGB", (W, H), (0, 0, 0)), 0.3)

            # 前景微放大 (1.0 → 1.06)
            fg_scale = _lerp(1.0, 1.06, t_ease)
            fg_w = int(W / fg_scale)
            fg_h = int(H / fg_scale)
            fg_cx = (W - fg_w) // 2
            fg_cy = (H - fg_h) // 2
            fg_crop = fg.crop((fg_cx, fg_cy, fg_cx + fg_w, fg_cy + fg_h))
            fg_resized = fg_crop.resize((W, H), Image.LANCZOS)

            # 合成：前景覆蓋在背景上 (帶微妙的混合)
            frame = Image.blend(bg_frame, fg_resized, 0.88)
            # 加暗角增加深度感
            frame = _add_vignette(frame, 0.4)

            # 轉場淡入淡出
            if pi > 0 and f < trans_frames:
                alpha = f / trans_frames
                frame_arr = np.array(frame, dtype=np.float32)
                frame_arr *= alpha
                frame = Image.fromarray(frame_arr.clip(0, 255).astype(np.uint8))

            enc.write_frame(frame)
            frame_idx += 1
            if progress_cb and frame_idx % 10 == 0:
                progress_cb(5 + frame_idx / total_frames * 85, f"3D 視差: {pi+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  模板 3: 🎞️ 底片 / VHS 復古
#  膠片顆粒 + 光暈漏光 + 掃描線 + 色偏
# ══════════════════════════════════════════════════════════
def _tpl_film_vhs(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 2.5)
    hold_frames = int(dur * FPS)
    fade_frames = int(0.5 * FPS)
    total_frames = hold_frames * len(photos)

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    # 預先產生幾個光暈（共用）
    leaks = [_make_light_leak(W, H) for _ in range(3)]

    for pi, p in enumerate(photos):
        img, fc = _load_with_face(p)
        base = _fit_cover(img, W, H, face_center=fc)

        # 復古色調：降飽和 + 暖色偏移
        enhancer = ImageEnhance.Color(base)
        base_desat = enhancer.enhance(0.6)
        arr = np.array(base_desat, dtype=np.float32)
        # 暖色偏移
        arr[:, :, 0] = np.clip(arr[:, :, 0] + 12, 0, 255)  # R+
        arr[:, :, 1] = np.clip(arr[:, :, 1] + 5, 0, 255)   # G+
        arr[:, :, 2] = np.clip(arr[:, :, 2] - 15, 0, 255)   # B-
        base_graded = Image.fromarray(arr.astype(np.uint8))

        # 選一個光暈
        leak = leaks[pi % len(leaks)]

        for f in range(hold_frames):
            t = f / max(hold_frames - 1, 1)
            frame = base_graded.copy()

            # 輕微 zoom (呼吸感)
            breath = 1.0 + 0.025 * math.sin(t * math.pi * 2)
            if breath != 1.0:
                bw = int(W / breath)
                bh = int(H / breath)
                cx = (W - bw) // 2
                cy = (H - bh) // 2
                frame = frame.crop((cx, cy, cx + bw, cy + bh)).resize((W, H), Image.LANCZOS)

            # 膠片顆粒
            frame = _add_film_grain(frame, intensity=18)

            # 光暈漏光（時間推移效果）
            leak_alpha = 0.08 + 0.06 * math.sin(t * math.pi)
            frame = Image.blend(frame, leak, leak_alpha)

            # 掃描線
            frame = _add_scanlines(frame, spacing=3, opacity=30)

            # 暗角
            frame = _add_vignette(frame, 0.5)

            # 底部時間戳
            draw = ImageDraw.Draw(frame)
            ts_font = _font(28)
            timestamp = f"2026.04  #{pi+1:02d}"
            draw.text((W - 40, H - 60), timestamp, font=ts_font, anchor="rm",
                       fill=(200, 180, 140))

            # 偶爾的水平偏移抖動 (VHS 效果)
            if random.random() < 0.06:
                shift = random.randint(-6, 6)
                frame = ImageChops.offset(frame, shift, 0)

            # 轉場
            if pi > 0 and f < fade_frames:
                alpha = f / fade_frames
                bright = Image.new("RGB", (W, H), (255, 220, 180))
                # 膠片燒灼轉場
                if alpha < 0.5:
                    frame = Image.blend(frame, bright, (0.5 - alpha) * 1.2)
                frame_arr = np.array(frame, dtype=np.float32) * alpha
                frame = Image.fromarray(frame_arr.clip(0, 255).astype(np.uint8))

            enc.write_frame(frame)
            frame_idx += 1
            if progress_cb and frame_idx % 10 == 0:
                progress_cb(5 + frame_idx / total_frames * 85, f"底片復古: {pi+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  模板 4: 💫 RGB 色散故障
#  色版偏移 + 閃爍 + 高對比 + 故障條紋
# ══════════════════════════════════════════════════════════
def _tpl_rgb_glitch(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 1.2)
    hold_frames = int(dur * FPS)
    total_frames = hold_frames * len(photos)

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    for pi, p in enumerate(photos):
        img, fc = _load_with_face(p)
        base = _fit_cover(img, W, H, face_center=fc)
        # 高對比 + 稍微提亮
        base = ImageEnhance.Contrast(base).enhance(1.4)
        base = ImageEnhance.Brightness(base).enhance(1.05)

        for f in range(hold_frames):
            t = f / max(hold_frames - 1, 1)
            frame = base.copy()

            # 持續的輕微 RGB 偏移
            base_offset = 3 + int(3 * math.sin(t * math.pi * 4))
            frame = _rgb_split(frame, base_offset, base_offset // 2)

            # 節拍感 zoom pulse
            pulse = 1.0 + 0.03 * abs(math.sin(t * math.pi * 3))
            if pulse != 1.0:
                pw, ph = int(W / pulse), int(H / pulse)
                cx, cy = (W - pw) // 2, (H - ph) // 2
                frame = frame.crop((cx, cy, cx + pw, cy + ph)).resize((W, H), Image.LANCZOS)

            # 隨機故障條紋
            if random.random() < 0.12:
                arr = np.array(frame)
                n_stripes = random.randint(2, 6)
                for _ in range(n_stripes):
                    y_start = random.randint(0, H - 20)
                    h_stripe = random.randint(3, 20)
                    shift = random.randint(-40, 40)
                    stripe = arr[y_start:y_start + h_stripe, :, :]
                    arr[y_start:y_start + h_stripe, :, :] = np.roll(stripe, shift, axis=1)
                frame = Image.fromarray(arr)

            # 大故障瞬間 (每張照片中間來一次)
            mid_frame = hold_frames // 2
            if abs(f - mid_frame) < 2:
                frame = _rgb_split(frame, 20, 12)
                # 亮度閃爍
                frame = ImageEnhance.Brightness(frame).enhance(1.4)

            # 暗角
            frame = _add_vignette(frame, 0.35)

            # 切換閃白
            if f < 2 and pi > 0:
                white = Image.new("RGB", (W, H), (255, 255, 255))
                alpha = 1.0 - f / 2
                frame = Image.blend(frame, white, alpha * 0.7)

            # 名字浮水印 (帶 RGB 偏移效果)
            if t > 0.3 and t < 0.7:
                draw = ImageDraw.Draw(frame)
                name_font = _font(36, bold=True)
                text_alpha = min((t - 0.3) / 0.1, 1.0, (0.7 - t) / 0.1)
                color_val = int(255 * text_alpha)
                draw.text((W // 2 + 2, H - 160 + 1), celebrity.upper(),
                          font=name_font, anchor="mm", fill=(color_val, 0, 0))
                draw.text((W // 2 - 2, H - 160 - 1), celebrity.upper(),
                          font=name_font, anchor="mm", fill=(0, 0, color_val))
                draw.text((W // 2, H - 160), celebrity.upper(),
                          font=name_font, anchor="mm", fill=(color_val, color_val, color_val))

            enc.write_frame(frame)
            frame_idx += 1
            if progress_cb and frame_idx % 8 == 0:
                progress_cb(5 + frame_idx / total_frames * 85, f"RGB 故障: {pi+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  模板 5: 🎬 電影預告片
#  寬銀幕黑邊 + 青橙調色 + 慢 zoom + 打字字幕
# ══════════════════════════════════════════════════════════
_CINEMA_LINES = [
    "EVERY LEGEND", "HAS A BEGINNING",
    "THE WORLD", "IS WATCHING",
    "BEYOND", "ALL LIMITS",
    "ONE NAME", "ONE ICON",
    "RISE", "ABOVE",
    "UNSTOPPABLE", "UNFORGETTABLE",
]


def _tpl_cinema(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 3.0)
    hold_frames = int(dur * FPS)
    bar_h = 130  # 上下黑邊高度
    fade_frames = int(0.6 * FPS)
    total_frames = hold_frames * len(photos)

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    for pi, p in enumerate(photos):
        img, fc = _load_with_face(p)
        # 填滿中間區域 (扣除上下黑邊)
        content_h = H - bar_h * 2
        base = _fit_cover(img, W, content_h, face_center=fc)
        # 青橙調色
        base = _teal_orange_grade(base, 0.7)
        # 提高對比
        base = ImageEnhance.Contrast(base).enhance(1.25)

        # 準備字幕文字
        line_idx = (pi * 2) % len(_CINEMA_LINES)
        text_line1 = _CINEMA_LINES[line_idx]
        text_line2 = _CINEMA_LINES[(line_idx + 1) % len(_CINEMA_LINES)]
        # 最後一張用明星名
        if pi == len(photos) - 1:
            text_line1 = celebrity.upper()
            text_line2 = ""

        # 放大一些以便 zoom
        big = _fit_cover(img, int(W * 1.2), int(content_h * 1.2), face_center=fc)
        big = _teal_orange_grade(big, 0.7)
        big = ImageEnhance.Contrast(big).enhance(1.25)

        for f in range(hold_frames):
            t = f / max(hold_frames - 1, 1)
            t_ease = _ease_in_out_cubic(t)

            # 慢 zoom
            scale = _lerp(1.0, 1.12, t_ease)
            bw, bh = big.size
            cw = int(W / scale)
            ch = int(content_h / scale)
            cx = (bw - cw) // 2
            cy = (bh - ch) // 2
            cx = max(0, min(cx, bw - cw))
            cy = max(0, min(cy, bh - ch))
            content = big.crop((cx, cy, cx + cw, cy + ch)).resize((W, content_h), Image.LANCZOS)

            # 組合畫面 (上下黑邊)
            canvas = Image.new("RGB", (W, H), (0, 0, 0))
            canvas.paste(content, (0, bar_h))

            # 暗角
            canvas = _add_vignette(canvas, 0.45)

            # 打字機字幕效果
            draw = ImageDraw.Draw(canvas)
            title_font = _font(52, bold=True)
            sub_font = _font(34)

            # 文字出現時機
            char_appear_rate = 0.06  # 每個字元多少 t 出現
            if text_line1:
                n_chars1 = min(int(t / char_appear_rate) + 1, len(text_line1)) if t > 0.1 else 0
                visible1 = text_line1[:n_chars1]
                if visible1:
                    # 文字位置：下方黑邊區域
                    ty = H - bar_h + bar_h // 2 - 20
                    draw.text((W // 2, ty), visible1, font=title_font,
                              anchor="mm", fill=(255, 255, 255))
                    # 打字游標
                    if n_chars1 < len(text_line1) and int(t * 8) % 2 == 0:
                        bbox = draw.textbbox((W // 2, ty), visible1, font=title_font, anchor="mm")
                        cursor_x = bbox[2] + 4
                        draw.line([(cursor_x, ty - 18), (cursor_x, ty + 18)],
                                  fill=(255, 255, 255), width=3)

            if text_line2:
                delay = len(text_line1) * char_appear_rate + 0.15
                if t > delay:
                    t2 = t - delay
                    n_chars2 = min(int(t2 / char_appear_rate) + 1, len(text_line2))
                    visible2 = text_line2[:n_chars2]
                    if visible2:
                        ty2 = H - bar_h + bar_h // 2 + 25
                        draw.text((W // 2, ty2), visible2, font=sub_font,
                                  anchor="mm", fill=(180, 180, 180))

            # 上方黑邊裝飾線
            line_w = int(W * 0.6 * min(t * 4, 1.0))
            if line_w > 0:
                draw.line([(W // 2 - line_w // 2, bar_h - 2),
                           (W // 2 + line_w // 2, bar_h - 2)],
                          fill=(200, 160, 60), width=2)

            # 跨圖淡入
            if pi > 0 and f < fade_frames:
                alpha = f / fade_frames
                arr = np.array(canvas, dtype=np.float32) * _ease_out_cubic(alpha)
                canvas = Image.fromarray(arr.clip(0, 255).astype(np.uint8))

            # 最後 fade out
            if pi == len(photos) - 1 and f > hold_frames - fade_frames:
                alpha = (hold_frames - f) / fade_frames
                arr = np.array(canvas, dtype=np.float32) * alpha
                canvas = Image.fromarray(arr.clip(0, 255).astype(np.uint8))

            enc.write_frame(canvas)
            frame_idx += 1
            if progress_cb and frame_idx % 10 == 0:
                progress_cb(5 + frame_idx / total_frames * 85, f"電影預告: {pi+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  模板 6: ❤️ 心跳脈動
#  照片跟節奏放大縮小 + 暗角呼吸 + 白閃切換
# ══════════════════════════════════════════════════════════
def _tpl_heartbeat(photos, output_path, celebrity, options, progress_cb):
    dur = options.get("dur_per_photo", 1.5)
    hold_frames = int(dur * FPS)
    flash_frames = 2  # 切換白閃幀數
    total_frames = (hold_frames + flash_frames) * len(photos)

    # BPM 感 — 每秒 2 次脈動 = 120 BPM
    bpm = 120
    pulse_freq = bpm / 60.0  # Hz

    enc = FrameEncoder(output_path)
    enc.start()
    frame_idx = 0

    for pi, p in enumerate(photos):
        img, fc = _load_with_face(p)
        base = _fit_cover(img, W, H, face_center=fc)
        # 稍微降飽和 + 提對比
        base = ImageEnhance.Color(base).enhance(0.85)
        base = ImageEnhance.Contrast(base).enhance(1.2)

        for f in range(hold_frames):
            t = f / FPS  # 秒
            frame = base.copy()

            # 心跳脈動 zoom (正弦波)
            # 使用 abs(sin) 讓脈動像心跳（每次 beat 是一次放大）
            beat_phase = math.sin(2 * math.pi * pulse_freq * t)
            pulse = 1.0 + 0.04 * abs(beat_phase)
            pw = int(W / pulse)
            ph = int(H / pulse)
            cx = (W - pw) // 2
            cy = (H - ph) // 2
            frame = frame.crop((cx, cy, cx + pw, cy + ph)).resize((W, H), Image.LANCZOS)

            # 暗角脈動 (與 zoom 同步)
            vig_str = 0.3 + 0.25 * abs(beat_phase)
            frame = _add_vignette(frame, vig_str)

            # 節拍亮度微閃
            if beat_phase > 0.9:
                frame = ImageEnhance.Brightness(frame).enhance(1.08)

            enc.write_frame(frame)
            frame_idx += 1

        # 切換白閃
        if pi < len(photos) - 1:
            for f in range(flash_frames):
                alpha = 1.0 - f / max(flash_frames, 1)
                white = Image.new("RGB", (W, H), (255, 255, 255))
                curr_frame = _fit_cover(img, W, H, face_center=fc)
                frame = Image.blend(curr_frame, white, alpha * 0.85)
                enc.write_frame(frame)
                frame_idx += 1

        if progress_cb:
            progress_cb(5 + frame_idx / total_frames * 85, f"心跳脈動: {pi+1}/{len(photos)}")

    enc.finish()


# ══════════════════════════════════════════════════════════
#  主入口
# ══════════════════════════════════════════════════════════
_DISPATCH = {
    "velocity": _tpl_velocity,
    "parallax_3d": _tpl_parallax_3d,
    "film_vhs": _tpl_film_vhs,
    "rgb_glitch": _tpl_rgb_glitch,
    "cinema": _tpl_cinema,
    "heartbeat": _tpl_heartbeat,
}


def generate_video(celebrity, template_id, photo_dir, output_path,
                   options=None, progress_cb=None):
    """
    生成影片。
    Returns:
        dict: {success, output_path, duration, file_size, error}
    """
    options = options or {}
    tpl = TEMPLATES.get(template_id)
    if not tpl:
        return {"success": False, "error": f"未知模板: {template_id}"}

    func = _DISPATCH.get(template_id)
    if not func:
        return {"success": False, "error": f"模板未實作: {template_id}"}

    # 收集照片（若前端已選好照片就直接用）
    if options.get("selected_photos"):
        photos = [p for p in options["selected_photos"] if os.path.isfile(p)]
    else:
        max_photos = int(options.get("max_photos", 12))
        shuffle = options.get("shuffle", True)
        photos = _collect_photos(photo_dir, max_photos, shuffle)

    min_p = tpl.get("min_photos", 3)
    if len(photos) < min_p:
        return {"success": False, "error": f"照片數量不足，至少需要 {min_p} 張（找到 {len(photos)} 張）"}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if progress_cb:
        progress_cb(5, f"載入 {len(photos)} 張照片，使用模板: {tpl['name']}")

    try:
        func(photos, output_path, celebrity, options, progress_cb)
    except Exception as e:
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except Exception:
                pass
        return {"success": False, "error": str(e)}

    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
        return {"success": False, "error": "FFmpeg 編碼失敗，輸出檔案異常"}

    file_size = os.path.getsize(output_path)
    dur_per = options.get("dur_per_photo", tpl.get("dur_per_photo", 2.0))
    duration = len(photos) * dur_per

    if progress_cb:
        progress_cb(100, "影片生成完成！")

    return {
        "success": True,
        "output_path": output_path,
        "duration": round(duration, 1),
        "file_size": file_size,
    }
