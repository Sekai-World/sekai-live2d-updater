import asyncio
import orjson as json
import logging
from typing import Dict, List, Tuple

import aiohttp
from anyio import Path, open_file

from crypto import unpack
from helpers import (
    ensure_dir_exists,
    get_download_list,
    refresh_cookie,
    setup_logging_queue,
)
from utils.live2d import restore_live2d_motions
from worker import worker

logger = logging.getLogger("live2d")


async def do_download(dl_list: List[Tuple], config, headers, cookie):
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def download_task(url, bundle):
        async with semaphore:
            await worker(
                f"download_worker-{url}",
                (url, bundle),
                config,
                headers,
                cookie=cookie,
            )

    # Create and gather download tasks
    tasks = [download_task(url, bundle) for url, bundle in dl_list]
    await asyncio.gather(*tasks)

    logger.info("Download completed, restoring live2d motions...")

    await restore_live2d_motions(
        config.ASSET_LOCAL_BUNDLE_CACHE_DIR / "live2d" / "motion",
        config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "motion",
        config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "model",
        config.UNITY_VERSION,
    )

    logger.info("Restoring completed, uploading...")

    for remote_storage in config.ASSET_REMOTE_STORAGE:
        if remote_storage["type"] == "live2d":
            remote_base = remote_storage["base"]

            # Construct the remote path
            remote_path = Path(remote_base) / "live2d"

            # Construct the upload command
            src_path: Path = config.ASSET_LOCAL_BUNDLE_CACHE_DIR / "live2d"
            program: str = remote_storage["program"]
            args: list[str] = remote_storage["args"][:]
            args[args.index("src")] = str(src_path)
            args[args.index("dst")] = str(remote_path)
            logger.debug(
                "Uploading %s to %s using command: %s %s",
                src_path,
                remote_path,
                program,
                " ".join(args),
            )

            # Execute the command
            upload_process = await asyncio.create_subprocess_exec(program, *args)
            await upload_process.wait()
            if upload_process.returncode != 0:
                logger.error("Failed to upload %s to %s", src_path, remote_path)
                raise RuntimeError(
                    f"Failed to upload {src_path} to {remote_path} using command: {program} {' '.join(args)}"
                )
            else:
                logger.info("Successfully uploaded %s to %s", src_path, remote_path)


async def main():
    # Check if the config module is loaded
    if "config" not in globals():
        raise ImportError(
            "Config module not loaded. Please run the script with the config argument."
        )
    # load the config module
    global config

    # ensure required directories exist
    await ensure_dir_exists(config.DL_LIST_CACHE_PATH.parent)
    await ensure_dir_exists(config.ASSET_BUNDLE_INFO_CACHE_PATH.parent)
    await ensure_dir_exists(config.GAME_VERSION_JSON_CACHE_PATH.parent)

    headers: Dict[str, str] = {
        "Accept": "*/*",
        "User-Agent": config.USER_AGENT,
        "X-Unity-Version": config.UNITY_VERSION,
    }

    cookie = None
    # Cookie must be filled if GAME_COOKIE_URL is set in the config
    if config.GAME_COOKIE_URL:
        headers, cookie = await refresh_cookie(config, headers)

    if await config.DL_LIST_CACHE_PATH.exists():
        logger.info(
            "Cache file %s exists, loading from cache", config.DL_LIST_CACHE_PATH
        )
        # Load the dl_list from the cache and start downloading
        async with await open_file(config.DL_LIST_CACHE_PATH, "r") as f:
            dl_list = json.loads(await f.read())
            logger.info("%d items to download", len(dl_list))
            await do_download(dl_list, config=config, headers=headers, cookie=cookie)

        # remove the cache file
        await config.DL_LIST_CACHE_PATH.unlink()
        return

    game_version_json = None
    # Download, parse and cache the game version json from GAME_VERSION_JSON_URL
    if config.GAME_VERSION_JSON_URL:
        async with aiohttp.ClientSession() as session:
            async with session.get(config.GAME_VERSION_JSON_URL) as response:
                if response.status == 200:
                    game_version_json = await response.json(content_type="text/plain")
                    # Check if the json is valid
                    if (
                        not isinstance(game_version_json, dict)
                        or "appVersion" not in game_version_json
                        or "appHash" not in game_version_json
                    ):
                        raise Exception(
                            f"Invalid json from {config.GAME_VERSION_JSON_URL}"
                        )
                else:
                    raise Exception(
                        f"Failed to fetch game version json from {config.GAME_VERSION_JSON_URL}"
                    )
    else:
        raise Exception("GAME_VERSION_JSON_URL is not set in the config")
    logger.debug(
        f"Current appVersion: {game_version_json['appVersion']}, dataVersion: {game_version_json['dataVersion']}, assetVersion: {game_version_json['assetVersion']}"
    )

    assetbundle_host_hash = None
    # Format GAME_VERSION_URL using the appVersion and appHash from the game version json
    if config.GAME_VERSION_URL:
        game_version_url = config.GAME_VERSION_URL.format(
            appVersion=game_version_json["appVersion"],
            appHash=game_version_json["appHash"],
        )
        # This request needs to be proxied
        async with aiohttp.ClientSession(proxy=config.PROXY_URL) as session:
            async with session.get(game_version_url, headers=headers) as response:
                if response.status == 200:
                    result = await response.read()
                    json_result = unpack(config.AES_KEY, config.AES_IV, result)
                    # Check if the json is valid
                    if (
                        not isinstance(json_result, dict)
                        or "assetbundleHostHash" not in json_result
                    ):
                        raise Exception(f"Invalid result from {game_version_url}")
                    assetbundle_host_hash = json_result["assetbundleHostHash"]
                else:
                    raise Exception(
                        f"Failed to fetch assetbundle host hash from {game_version_url}"
                    )
    else:
        raise Exception("GAME_VERSION_URL is not set in the config")
    logger.debug(
        f"Current assetbundleHostHash: {assetbundle_host_hash}, assetHash: {game_version_json['assetHash']}"
    )

    asset_bundle_info = None
    # Format ASSET_BUNDLE_INFO_URL using the information above
    if config.ASSET_BUNDLE_INFO_URL:
        asset_bundle_info_url = config.ASSET_BUNDLE_INFO_URL.format(
            assetbundleHostHash=assetbundle_host_hash,
            assetVersion=game_version_json["assetVersion"],
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(asset_bundle_info_url, headers=headers) as response:
                if response.status == 200:
                    result = await response.read()
                    asset_bundle_info = unpack(config.AES_KEY, config.AES_IV, result)
                    # Check if the json is valid
                    if not isinstance(asset_bundle_info, dict):
                        raise Exception(f"Invalid json from {asset_bundle_info_url}")
                else:
                    raise Exception(
                        f"Failed to fetch asset bundle info from {asset_bundle_info_url}"
                    )
    else:
        raise Exception("ASSET_BUNDLE_INFO_URL is not set in the config")
    logger.debug(
        f"Current assetBundleInfoVersion: {asset_bundle_info['version']}, bundles length: {len(asset_bundle_info['bundles'])}"
    )

    # Generate the download list
    download_list = await get_download_list(
        asset_bundle_info,
        game_version_json,
        config=config,
        assetbundle_host_hash=assetbundle_host_hash,
    )
    logger.info("Download list generated, %d items to download", len(download_list))

    await do_download(download_list, config=config, headers=headers, cookie=cookie)

    # remove the cached download list
    if await config.DL_LIST_CACHE_PATH.exists():
        await config.DL_LIST_CACHE_PATH.unlink()


def cli():
    # Accept command line arguments
    import argparse

    parser = argparse.ArgumentParser(
        description="Start the asset updater with given config."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Path to the config python file.",
        required=True,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging."
    )
    args = parser.parse_args()

    # Load the config python file as dynamic module
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("config", args.config)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    sys.modules["config"] = config
    # Set the config as a global variable
    globals()["config"] = config

    # Set the logging level
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    setup_logging_queue()

    # Run the main function
    asyncio.run(main())


if __name__ == "__main__":
    cli()
