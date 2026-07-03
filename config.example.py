import os

from anyio import Path

from model import SekaiServerRegion

# Proxy for fetching restricted content
PROXY_URL = None

# Server region
REGION = SekaiServerRegion.JP

# Fallback unity version, replace with the correct version if needed
UNITY_VERSION = "2022.3.21f1"
# User agent for requests, replace with the correct user agent if needed
USER_AGENT = None
# HTTP request timeout in seconds; set to 0 or None to disable
REQUEST_TIMEOUT = 180
# Number of download retry attempts on timeout or connection errors
DOWNLOAD_MAX_RETRIES = 3
# Minimum free bytes to keep on the download filesystem before starting a new download
MIN_FREE_DISK_BYTES = 1024 * 1024 * 1024
# How often blocked downloads recheck free disk space
DOWNLOAD_DISK_SPACE_CHECK_INTERVAL = 5

# Concurrency settings, default to the number of CPU cores
MAX_CONCURRENCY = os.cpu_count()
# Pipeline stage concurrency. Defaults preserve the previous MAX_CONCURRENCY behavior
# for download/extract while upload uses one bundle-level worker.
MAX_CONCURRENCY_DOWNLOADS = MAX_CONCURRENCY
MAX_CONCURRENCY_EXTRACTS = MAX_CONCURRENCY
MAX_CONCURRENCY_UPLOAD_STAGE = 1
MAX_CONCURRENCY_MOTION_BASE_FILES = max(1, (os.cpu_count() or 1) // 2)
# Maximum queued artifacts between stages.
PIPELINE_STAGE_QUEUE_SIZE = MAX_CONCURRENCY
# Maximum number of concurrent uploads
MAX_CONCURRENCY_UPLOADS = 10

# Crypto settings
AES_KEY = bytes("AES_KEY")
AES_IV = bytes("AES_IV")

# JSON URL for fetching game version information
GAME_VERSION_JSON_URL = None
# URL for fetching game cookies
GAME_COOKIE_URL = None
# URL for fetching in-game version information
GAME_VERSION_URL = None
# URL for fetching asset version
ASSET_VER_URL = None
# URL for fetching asset bundle info
ASSET_BUNDLE_INFO_URL = None
# URL for downloading asset bundle
ASSET_BUNDLE_URL = None
APP_VERSION_OVERRIDE = None

# Cache information for downloading, must set!
DL_LIST_CACHE_PATH = Path("cache", "jp", "json", "dl_list.json")
ASSET_BUNDLE_INFO_CACHE_PATH = Path("cache", "jp", "json", "asset_bundle_info.json")
GAME_VERSION_JSON_CACHE_PATH = Path("cache", "jp", "json", "version.json")

# Local asset directories, must set!
ASSET_LOCAL_EXTRACTED_DIR = None  # Example: Path("cache", "jp", "extracted")
ASSET_LOCAL_BUNDLE_CACHE_DIR = None  # Example: Path("cache", "jp", "bundle")

# Remote storage settings
ASSET_REMOTE_STORAGE = [
    {
        "type": "live2d",
        "base": "remote:example-bucket/",
        "program": "rclone",
        "args": ["copy", "src", "dst"]
    },
]

DL_INCLUDE_LIST = [
    r"^live2d/.*"
]
DL_EXCLUDE_LIST = None
DL_PRIORITY_LIST = None
