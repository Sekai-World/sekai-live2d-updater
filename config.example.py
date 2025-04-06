import os
import platform

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

# Concurrency settings, default to the number of CPU cores
MAX_CONCURRENCY = os.cpu_count()
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
# URL for fetching asset bundle info
ASSET_BUNDLE_INFO_URL = None
# URL for downloading asset bundle
ASSET_BUNDLE_URL = None

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
        "base": "remote:example-assets/",
        "cmd": "rclone copy {src} {dst}",
    },
]