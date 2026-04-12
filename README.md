# Media Downloader

All-in-one media downloader with celebrity photo bulk download, YouTube/TikTok video download, AI face recognition extraction, and one-click highlight reel generation.

## Features

### Photo Download
- **Multiple image sources** - Bing, Google, DuckDuckGo, Pinterest - select one or more
- **3-tier deduplication** - Prevents downloading the same photo twice:
  1. **URL check** - Skips already-downloaded URLs (instant, database lookup)
  2. **MD5 hash** - Catches exact binary duplicates across different URLs
  3. **Perceptual hash (pHash)** - Catches resized/recompressed variants (hamming distance < 8)
- **Batch download** - Queue multiple celebrities in one go
- **Size filtering** - Filter by image size (any / large / extra-large)
- **Auto-organized folders** - Each person gets their own subfolder
- **Smart keywords** - Automatically generates search keyword variations
- **Celebrity alias system** - Merge different names/languages for the same person
- **Photo usage tracking** - Avoids re-selecting previously used photos

### Video Download & Processing
- **YouTube & TikTok search** - Search and download videos from both platforms
- **TikTok scraping** - Uses DrissionPage + persistent Chrome for TikTok keyword search (bypasses login/CAPTCHA issues)
- **Batch download** - Select multiple videos to download at once
- **Quality selection** - 480p / 720p / 1080p / best quality / audio only

### AI Face Recognition & Extraction
- **Face detection** - YuNet ONNX model for real-time face detection
- **Face recognition** - SFace ONNX model for face embedding & matching
- **Video person extraction** - Automatically scan videos and extract segments containing a specific person
- **Reference photo matching** - Uses downloaded photos as reference to identify the person in videos
- **Alias system** - Map alternate names to canonical photo folders

### One-Click Video Generation
- **Automated pipeline** - Input a person name, get a highlight reel:
  1. Search TikTok/YouTube automatically
  2. Download top videos
  3. AI face recognition to extract person segments
  4. Score highlights and compile final video
- **Vertical video support** - 9:16 format ready for TikTok/Reels/Shorts
- **Highlight strategies** - Balanced / Close-up priority / Dynamic priority / Random
- **Crossfade transitions** - Smooth transitions between clips

### TikTok-Style Video Maker
- 6 modern video templates:
  - Velocity (slow zoom + white flash + motion blur)
  - Parallax 3D (foreground zoom + background pan + depth blur)
  - Film VHS (grain + light leaks + scan lines + warm color shift)
  - RGB Glitch (channel split + glitch stripes + high contrast)
  - Cinema (2.39:1 letterbox + teal-orange grading + typewriter subtitles)
  - Heartbeat (120 BPM sine-wave zoom pulse + vignette breathing)

### General
- **Web-based UI** - Access from any device on the local network
- **Real-time progress** - Server-Sent Events (SSE) for live updates
- **Desktop GUI** - tkinter-based alternative (`celebrity_downloader.py`)
- **SSH tunnel** - Remote access through SSH port forwarding

## Quick Start

### Prerequisites

- Python 3.8+
- FFmpeg (required for video features)
- Chrome (required for TikTok search)

### Installation

```bash
git clone https://github.com/walterfan1322/celebrity-photo-downloader.git
cd celebrity-photo-downloader
pip install -r requirements.txt
```

Or on Windows, run `setup.bat`.

### Download Face Models (for video extraction)

Place these ONNX models in a `models/` directory:
- [YuNet](https://github.com/opencv/opencv_zoo/tree/main/models/face_detection_yunet) - `yunet.onnx`
- [SFace](https://github.com/opencv/opencv_zoo/tree/main/models/face_recognition_sface) - `sface.onnx`

### Usage

**Web version** (recommended):
```bash
python web_app.py
```
Then open `http://localhost:5000`. Other devices on the same network can access it via your machine's IP.

**Desktop version**:
```bash
python celebrity_downloader.py
```

### TikTok Search Setup

TikTok keyword search requires a persistent Chrome instance with debug port:
```bash
chrome --remote-debugging-port=9222 --user-data-dir="./chrome_profile"
```
Log into TikTok once in this Chrome instance. See `startup.bat.example` for auto-start configuration.

## Configuration

All settings can be overridden via environment variables. See `.env.example` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_ROOT` | `./Photos` | Where photos are saved |
| `YT_ROOT` | `./YouTube` | Where YouTube downloads are saved |
| `PORT` | `5000` | Web server port |
| `CHROME_DEBUG_PORT` | `9222` | Chrome DevTools port for TikTok search |

## Remote Access via SSH Tunnel

If the server's firewall blocks the web port, you can use `connect.py` to create an SSH tunnel:

```bash
# Set connection info
export SSH_HOST=your.server.ip
export SSH_USER=your_username
export SSH_PASS=your_password

python connect.py
```

This forwards `localhost:5000` to the remote server, and also allows other LAN devices to connect through your machine.

## Project Structure

```
celebrity-photo-downloader/
├── web_app.py               # Web GUI (Flask + SSE) - main application
├── celebrity_downloader.py  # Desktop GUI (tkinter)
├── connect.py               # SSH tunnel client
├── requirements.txt         # Python dependencies
├── setup.bat                # Windows installer script
├── run_web.bat              # Launch web version
├── run.bat                  # Launch desktop version
├── connect.bat              # Launch SSH tunnel
├── deploy.bat               # Deploy to remote server
├── startup.bat.example      # Auto-start on boot (example)
├── .env.example             # Environment variable template
├── .gitignore
├── models/                  # Face recognition models (not tracked)
│   ├── yunet.onnx
│   └── sface.onnx
├── Photos/                  # Downloaded photos (not tracked)
├── YouTube/                 # Downloaded/extracted videos (not tracked)
│   ├── downloads/
│   └── extracts/
└── data/                    # SQLite database (not tracked)
```

## Tech Stack

- **Backend**: Python, Flask, Waitress (production WSGI)
- **Frontend**: Vanilla HTML/CSS/JS (embedded single-file)
- **Video**: yt-dlp, FFmpeg, DrissionPage (TikTok scraping)
- **AI/CV**: OpenCV, YuNet (face detection), SFace (face recognition)
- **Image Processing**: Pillow, imagehash
- **Database**: SQLite
- **Remote**: paramiko (SSH tunneling)

## Changelog

### v2.0 (2026-04-12)
- **TikTok keyword search** - DrissionPage + persistent Chrome scraping (bypasses CAPTCHA)
- **One-click video generation** - Full automated pipeline: search -> download -> face extract -> highlight reel
- **Vertical video (9:16)** - TikTok/Reels/Shorts ready output format
- **Highlight reel generator** - Score video highlights, select best clips, compile with crossfade transitions
- **Alias & folder mapping modal** - Interactive folder selection when photo folder not found
- **Batch download + extract** - Select multiple search results, download and extract in sequence
- **Video rotation** - In-place FFmpeg rotation (90/180/270)
- **Auto-start support** - Chrome + web app auto-launch on boot via scheduled task
- **Sanitized for open source** - All paths relative, credentials via env vars, no hardcoded sensitive data

### v1.2 (2026-04-10)
- AI face recognition video extraction (YuNet + SFace ONNX models)
- YouTube folder reorganization: `downloads/{person}/` and `extracts/{person}/`
- Batch download fix: KeyError and early stop issues

### v1.1 (2026-04-10)
- TikTok-style video maker with 6 templates
- YouTube video search & download (yt-dlp)
- Celebrity alias system
- Photo usage tracking
- Google, DuckDuckGo, Pinterest image sources
- Security cleanup: remove credentials, hardcoded paths

### v1.0 (2026-04-10)
- Initial release
- Bing image search with 3-tier deduplication (URL / MD5 / pHash)
- Web GUI (Flask + SSE) and Desktop GUI (tkinter)
- Batch download, size filtering, auto-organized folders
- SSH tunnel remote access

## License

MIT
