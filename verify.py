
import sys, os
print("Python:", sys.version.split()[0])

import py_compile
script_dir = os.path.dirname(os.path.abspath(__file__))
py_compile.compile(os.path.join(script_dir, "celebrity_downloader.py"), doraise=True)
print("Syntax: OK")

import requests, PIL, imagehash
print(f"requests={requests.__version__}, Pillow={PIL.__version__}, imagehash={imagehash.__version__}")

download_root = os.environ.get("DOWNLOAD_ROOT", os.path.join(script_dir, "Photos"))
print(f"Download root ({download_root}) exists:", os.path.isdir(download_root))
print("All checks passed!")
