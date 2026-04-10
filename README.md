# Celebrity Photo Downloader

Bulk download celebrity photos from Bing Image Search with intelligent deduplication. Includes both a desktop GUI (tkinter) and a web-based interface (Flask) accessible from any device on the local network.

## Features

- **Multiple image sources** - Bing, Google, DuckDuckGo, Pinterest (experimental) - select one or more at once
- **3-tier deduplication** - Prevents downloading the same photo twice:
  1. **URL check** - Skips already-downloaded URLs (instant, database lookup)
  2. **MD5 hash** - Catches exact binary duplicates across different URLs
  3. **Perceptual hash (pHash)** - Catches resized/recompressed variants (hamming distance < 8)
- **Batch download** - Queue multiple celebrities in one go
- **Size filtering** - Filter by image size (any / large >500px / extra-large >1024px)
- **Auto-organized folders** - Each celebrity gets their own subfolder
- **Download history** - SQLite database tracks all downloads with metadata
- **Two interfaces**:
  - **Desktop GUI** (`celebrity_downloader.py`) - tkinter-based, runs locally
  - **Web GUI** (`web_app.py`) - Flask-based, accessible from any device on the LAN

## Screenshots

### Web Interface
The web version features a responsive UI with:
- Real-time download progress via Server-Sent Events (SSE)
- Image gallery with lightbox viewer
- Download statistics dashboard
- Batch download support

### Desktop Interface
The desktop version provides:
- Threaded downloads (non-blocking UI)
- Scrollable log output
- Settings persistence

## Quick Start

### Prerequisites

- Python 3.8+
- Windows (batch scripts are Windows-only, but Python scripts work cross-platform)

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/celebrity-photo-downloader.git
cd celebrity-photo-downloader
pip install -r requirements.txt
```

Or on Windows, just run `setup.bat`.

### Usage

**Web version** (recommended):
```bash
python web_app.py
```
Then open `http://localhost:5000` in your browser. Other devices on the same network can access it via your machine's IP.

**Desktop version**:
```bash
python celebrity_downloader.py
```

Or on Windows, use `run_web.bat` / `run.bat`.

## Configuration

Configuration is done via environment variables (or just edit the defaults in the source):

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_ROOT` | `D:\CelebrityPhotos` | Where photos are saved |
| `PORT` | `5000` | Web server port |

See `.env.example` for all available variables.

## Remote Access via SSH Tunnel

If the server's firewall blocks the web port, you can use `connect.py` to create an SSH tunnel:

```bash
# Set connection info
export SSH_HOST=192.168.100.xxx
export SSH_USER=your_username
export SSH_PASS=your_password

python connect.py
```

This forwards `localhost:5000` to the remote server, and also allows other LAN devices to connect through your machine.

## Project Structure

```
celebrity-photo-downloader/
├── celebrity_downloader.py  # Desktop GUI (tkinter)
├── web_app.py               # Web GUI (Flask + SSE)
├── connect.py               # SSH tunnel client
├── requirements.txt         # Python dependencies
├── setup.bat                # Windows installer script
├── run.bat                  # Launch desktop version
├── run_web.bat              # Launch web version
├── connect.bat              # Launch SSH tunnel
├── deploy.bat               # Deploy to remote server
└── .env.example             # Environment variable template
```

## Tech Stack

- **Backend**: Python, Flask, Waitress (production WSGI server)
- **Frontend**: Vanilla HTML/CSS/JS (embedded in web_app.py)
- **Image Processing**: Pillow, imagehash
- **Database**: SQLite
- **Scraping**: requests + regex parsing (Bing, Google, DuckDuckGo, Pinterest)
- **Remote Access**: paramiko (SSH tunneling)

## License

MIT
