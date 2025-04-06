import asyncio
import base64
import orjson as json
import logging
import time
from logging.handlers import QueueHandler
from queue import SimpleQueue
from typing import Dict, List, Tuple

import aiohttp
from anyio import Path, open_file

logger = logging.getLogger("live2d")


class LocalQueueHandler(QueueHandler):
    def emit(self, record: logging.LogRecord) -> None:
        # Removed the call to self.prepare(), handle task cancellation
        try:
            self.enqueue(record)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.handleError(record)


def setup_logging_queue() -> None:
    """Move log handlers to a separate thread.

    Replace handlers on the root logger with a LocalQueueHandler,
    and start a logging.QueueListener holding the original
    handlers.

    """
    queue = SimpleQueue()
    root = logging.getLogger()

    handlers: List[logging.Handler] = []

    handler = LocalQueueHandler(queue)
    root.addHandler(handler)
    for h in root.handlers[:]:
        if h is not handler:
            root.removeHandler(h)
            handlers.append(h)

    listener = logging.handlers.QueueListener(
        queue, *handlers, respect_handler_level=True
    )
    listener.start()


async def ensure_dir_exists(dir_path: Path):
    """Ensure the directory exists, create it if not."""
    if not await dir_path.exists():
        await dir_path.mkdir(parents=True, exist_ok=True)

    if not await dir_path.is_dir():
        raise NotADirectoryError(
            f"Failed to create directory {dir_path}, path exists but is not a directory"
        )


async def get_download_list(
    asset_bundle_info: Dict,
    game_version_json: Dict,
    config=None,
    assetbundle_host_hash: str = None,
) -> List[Tuple[str, Dict]]:
    """Generate the download list for the live2d asset bundles.

    Args:
        asset_bundle_info (Dict): current asset bundle info
        game_version_json (Dict): current game version json
        config (Module, optional): configurations. Defaults to None.
        assetver (str, optional): asset ver used by nuverse servers. Defaults to None.
        assetbundle_host_hash (str, optional): host hash used by colorful palette servers. Defaults to None.

    Returns:
        List[Tuple[str, Dict]]: download list of asset bundles
    """

    cached_asset_bundle_info = None
    cached_game_version_json = None
    if await config.ASSET_BUNDLE_INFO_CACHE_PATH.exists():
        async with await open_file(config.ASSET_BUNDLE_INFO_CACHE_PATH) as f:
            cached_asset_bundle_info = json.loads(await f.read())
    if await config.GAME_VERSION_JSON_CACHE_PATH.exists():
        async with await open_file(config.GAME_VERSION_JSON_CACHE_PATH) as f:
            cached_game_version_json = json.loads(await f.read())

    download_list = None
    if cached_asset_bundle_info and cached_game_version_json:
        # Colorful Palette servers
        cached_bundles: Dict = cached_asset_bundle_info.get("bundles")
        current_bundles: Dict = asset_bundle_info.get("bundles")

        # compare hash of each bundle, if not equal, it should be included in the download list
        # it also includes the new bundles
        changed_bundles = [
            bundle
            for bundle in current_bundles.values()
            if bundle.get("hash")
            != cached_bundles.get(bundle.get("bundleName"), {}).get("hash")
        ]

        # Generate the download list from changed bundles
        version = asset_bundle_info.get("version")
        asset_hash: str = game_version_json.get("assetHash")
        download_list = [
            (
                config.ASSET_BUNDLE_URL.format(
                    assetbundleHostHash=assetbundle_host_hash,
                    version=version,
                    assetHash=asset_hash,
                    bundleName=bundle.get("bundleName"),
                ),
                bundle,
            )
            for bundle in changed_bundles
            if bundle.get("bundleName").startswith("live2d/")
        ]

    else:
        # Get the download list for a full download
        version = asset_bundle_info.get("version")
        asset_hash: str = game_version_json.get("assetHash")
        bundles: Dict = asset_bundle_info.get("bundles")

        download_list = [
            (
                config.ASSET_BUNDLE_URL.format(
                    assetbundleHostHash=assetbundle_host_hash,
                    version=version,
                    assetHash=asset_hash,
                    bundleName=bundle.get("bundleName"),
                ),
                bundle,
            )
            for bundle in bundles.values()
            if bundle.get("bundleName").startswith("live2d/")
        ]

    # Cache the download list
    if download_list:
        async with await open_file(config.DL_LIST_CACHE_PATH, "wb") as f:
            await f.write(json.dumps(download_list, option=json.OPT_INDENT_2))

    # Cache the asset bundle info
    async with await open_file(config.ASSET_BUNDLE_INFO_CACHE_PATH, "wb") as f:
        await f.write(json.dumps(asset_bundle_info, option=json.OPT_INDENT_2))

    # Cache the game version json
    async with await open_file(config.GAME_VERSION_JSON_CACHE_PATH, "wb") as f:
        await f.write(json.dumps(game_version_json, option=json.OPT_INDENT_2))

    return download_list


async def refresh_cookie(
    config, headers: Dict[str, str], cookie: str = None
) -> Tuple[Dict[str, str], str]:
    """Refresh the cookie using the GAME_COOKIE_URL."""
    if cookie:
        # Extract the expire time from the cookie
        cookie_expire_time = json.loads(
            base64.b64decode(cookie.split(";")[0].split("=")[1] + "=").decode("utf-8")
        )["Statement"][0]["Condition"]["DateLessThan"]["AWS:EpochTime"]
        # Check if the cookie is expired
        if cookie_expire_time > int(time.time()) + 3600:
            return headers, cookie

    # If the cookie is expired or not set, fetch a new one
    if config.GAME_COOKIE_URL:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.GAME_COOKIE_URL, headers=headers
            ) as response:
                if response.status == 200:
                    cookie = response.headers.get("Set-Cookie")
                    headers["Cookie"] = cookie
                else:
                    raise RuntimeError(
                        f"Failed to fetch cookie from {config.GAME_COOKIE_URL}"
                    )
    else:
        raise ValueError("GAME_COOKIE_URL is not set in the config")

    return headers, cookie


async def deobfuscate(data: bytes) -> bytes:
    """Deobfuscate the bundle data"""
    if data[:4] == b"\x20\x00\x00\x00":
        data = data[4:]
    elif data[:4] == b"\x10\x00\x00\x00":
        data = data[4:]
        header = bytes(
            a ^ b for a, b in zip(data[:128], (b"\xff" * 5 + b"\x00" * 3) * 16)
        )
        data = header + data[128:]
    return data
