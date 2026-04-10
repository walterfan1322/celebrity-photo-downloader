
import sys
print("Python:", sys.version.split()[0])

import py_compile
py_compile.compile(r"D:/CelebrityPhotoDownloader/celebrity_downloader.py", doraise=True)
print("Syntax: OK")

import requests, PIL, imagehash
print(f"requests={requests.__version__}, Pillow={PIL.__version__}, imagehash={imagehash.__version__}")

import os
print("D:/CelebrityPhotos exists:", os.path.isdir(r"D:/CelebrityPhotos"))
print("All checks passed!")
