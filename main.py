import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import orjson as json
from anyio import Path, open_file

from asset_bundle_info import build_request_headers, fetch_asset_bundle_info
from helpers import (
    build_download_disk_space_gate,
    ensure_dir_exists,
    filter_bundles,
    get_download_list,
    setup_logging_queue,
)
from utils.live2d import restore_live2d_motions
from worker import run_pipeline

logger = logging.getLogger("live2d")

DownloadItem = Tuple[str, Dict[str, Any]]
config: Optional[Any] = None


def require_config():
    if config is None:
        raise ImportError(
            "Config module not loaded. Please run the script with the config argument."
        )
    return config


async def do_download(dl_list: List[DownloadItem], config, headers, cookie) -> bool:
    logger.info("RUN | step=4/4 | action=pipeline_start | items=%d", len(dl_list))
    download_disk_space_gate = build_download_disk_space_gate(config)
    if download_disk_space_gate is not None:
        logger.debug(
            "Download disk space gate enabled for %s with min free bytes=%d",
            download_disk_space_gate.target_path,
            download_disk_space_gate.min_free_bytes,
        )

    try:
        failed_tasks = await run_pipeline(
            dl_list,
            config,
            headers,
            cookie=cookie,
            download_disk_space_gate=download_disk_space_gate,
        )
    except Exception:
        logger.exception(
            "ERROR | stage=pipeline | action=crash | preserve_pending=true | items=%d",
            len(dl_list),
        )
        failed_tasks = dl_list

    if failed_tasks:
        failed_path = config.DL_LIST_CACHE_PATH
        async with await open_file(failed_path, "wb") as f:
            await f.write(json.dumps(failed_tasks, option=json.OPT_INDENT_2))
        logger.warning(
            "RUN | result=partial_failure | failed=%d | retry_list=%s",
            len(failed_tasks),
            failed_path,
        )
        return False
    else:
        logger.info("RUN | result=success | completed=%d", len(dl_list))

    logger.info("Download completed, restoring live2d motions...")

    if len(dl_list):
        await restore_live2d_motions(
            config.ASSET_LOCAL_BUNDLE_CACHE_DIR / "live2d" / "motion",
            config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "motion",
            config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "model",
            config.UNITY_VERSION,
        )

    logger.info("Restoring completed, generating model list...")
    
    # Glob for all model files
    model_dir: Path = config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "model"
    model_list = []
    async for model_file in model_dir.glob("**/*.model3.json"):
        model_name = model_file.name.replace(".model3.json", "")
        model_path = model_file.parent.relative_to(model_dir)
        
        model_list.append({
            "modelName": model_name,
            "modelBase": str(model_file.parent.name),
            "modelPath": str(model_path),
            "modelFile": model_file.name,
        })
        
    logger.debug("Model list generated, %s", model_list)
    # Save the model list to a json file
    model_list_path = config.ASSET_LOCAL_EXTRACTED_DIR / "live2d" / "model_list.json"
    async with await open_file(model_list_path, "wb") as f:
        await f.write(json.dumps(model_list, option=json.OPT_INDENT_2))
        
    logger.info("Model list saved to %s", model_list_path)
    
    if config.ASSET_REMOTE_STORAGE:
        logger.info("Uploading live2d assets...")

        for remote_storage in config.ASSET_REMOTE_STORAGE:
            if remote_storage["type"] == "live2d":
                remote_base = remote_storage["base"]

                # Construct the remote path
                remote_path = Path(remote_base) / "live2d"

                # Construct the upload command
                src_path: Path = config.ASSET_LOCAL_EXTRACTED_DIR / "live2d"
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

    return True


async def main(
    update_asset_bundle_info_only: bool = False,
    force_full_download: bool = False,
):
    cfg = require_config()
    start_time = time.monotonic()

    run_mode = "metadata-only" if update_asset_bundle_info_only else "full-pipeline"
    logger.info(
        "RUN | status=start | mode=%s | force_full_download=%s",
        run_mode,
        force_full_download,
    )

    await ensure_dir_exists(cfg.DL_LIST_CACHE_PATH.parent)
    await ensure_dir_exists(cfg.ASSET_BUNDLE_INFO_CACHE_PATH.parent)
    await ensure_dir_exists(cfg.GAME_VERSION_JSON_CACHE_PATH.parent)
    headers, cookie = await build_request_headers(cfg)

    if force_full_download:
        logger.info(
            "RUN | option=force_full_download | cache_metadata=false | cache_pending=false"
        )

    logger.info("RUN | step=1/4 | action=fetch_metadata")
    fetch_result = await fetch_asset_bundle_info(cfg, headers=headers, cookie=cookie)
    headers = fetch_result.headers
    cookie = fetch_result.cookie
    game_version_json = fetch_result.game_version_json
    asset_ver = fetch_result.asset_ver
    assetbundle_host_hash = fetch_result.assetbundle_host_hash
    asset_bundle_info = fetch_result.asset_bundle_info

    logger.info(
        "RUN | action=metadata_fetched | asset_ver=%s | bundle_count=%d",
        asset_ver,
        len(asset_bundle_info.get("bundles", {})),
    )

    if update_asset_bundle_info_only:
        logger.info("RUN | step=2/2 | action=write_metadata_cache")
        current_bundles: Dict[str, Dict] = asset_bundle_info.get("bundles", {})
        if not current_bundles:
            raise ValueError("bundles must be set in asset bundle info")

        current_bundles = {
            key: value
            for key, value in current_bundles.items()
            if (value.get("bundleName") or "").startswith("live2d/")
        }
        current_bundles = await filter_bundles(
            current_bundles,
            include_list=getattr(cfg, "DL_INCLUDE_LIST", None),
            exclude_list=getattr(cfg, "DL_EXCLUDE_LIST", None),
        )
        if not current_bundles:
            raise ValueError("No bundles found after filtering")

        async with await open_file(cfg.ASSET_BUNDLE_INFO_CACHE_PATH, "wb") as f:
            await f.write(
                json.dumps(
                    {
                        "version": asset_bundle_info.get("version", ""),
                        "os": asset_bundle_info.get("os", ""),
                        "bundles": current_bundles,
                    },
                    option=json.OPT_INDENT_2,
                )
            )
        async with await open_file(cfg.GAME_VERSION_JSON_CACHE_PATH, "wb") as f:
            await f.write(json.dumps(game_version_json, option=json.OPT_INDENT_2))
        logger.info(
            "RUN | result=metadata_updated | path=%s | filtered_bundles=%d",
            cfg.ASSET_BUNDLE_INFO_CACHE_PATH,
            len(current_bundles),
        )
        logger.info(
            "RUN | status=completed | duration_sec=%.2f",
            time.monotonic() - start_time,
        )
        return

    logger.info("RUN | step=2/4 | action=build_download_list")
    new_download_list: List[DownloadItem] = await get_download_list(
        asset_bundle_info,
        game_version_json,
        config=cfg,
        assetver=asset_ver,
        assetbundle_host_hash=assetbundle_host_hash,
        include_list=getattr(cfg, "DL_INCLUDE_LIST", None),
        exclude_list=getattr(cfg, "DL_EXCLUDE_LIST", None),
        priority_list=getattr(cfg, "DL_PRIORITY_LIST", None),
        force_full_download=force_full_download,
    )
    logger.debug("New download candidates: %d item(s)", len(new_download_list))

    pending_list: List[DownloadItem] = []
    if (not force_full_download) and await cfg.DL_LIST_CACHE_PATH.exists():
        async with await open_file(cfg.DL_LIST_CACHE_PATH, "r") as f:
            pending_list = json.loads(await f.read())
        logger.info(
            "RUN | action=load_pending | count=%d | path=%s",
            len(pending_list),
            cfg.DL_LIST_CACHE_PATH,
        )

    if pending_list and new_download_list:
        pending_bundle_names = {
            bundle.get("bundleName") for _, bundle in pending_list
        }
        deduped_new = [
            item for item in new_download_list
            if item[1].get("bundleName") not in pending_bundle_names
        ]
        download_list: List[DownloadItem] = pending_list + deduped_new
        logger.info(
            "RUN | action=merge_download_list | pending=%d | new=%d | total=%d",
            len(pending_list),
            len(deduped_new),
            len(download_list),
        )
    elif pending_list:
        download_list = pending_list
        logger.info("RUN | action=retry_pending_only | count=%d", len(pending_list))
    else:
        download_list = new_download_list

    if not download_list:
        logger.info("RUN | result=noop | reason=no_items")
        logger.info(
            "RUN | status=completed | duration_sec=%.2f",
            time.monotonic() - start_time,
        )
        return

    logger.info("RUN | action=download_list_ready | count=%d", len(download_list))
    logger.info("RUN | step=3/4 | action=persist_queue | path=%s", cfg.DL_LIST_CACHE_PATH)
    async with await open_file(cfg.DL_LIST_CACHE_PATH, "wb") as f:
        await f.write(json.dumps(download_list, option=json.OPT_INDENT_2))

    is_success = await do_download(
        download_list,
        config=cfg,
        headers=headers,
        cookie=cookie,
    )

    if is_success and len(download_list) > 0 and await cfg.DL_LIST_CACHE_PATH.exists():
        await cfg.DL_LIST_CACHE_PATH.unlink()
        logger.debug(
            "Cleanup complete: removed pending list cache %s",
            cfg.DL_LIST_CACHE_PATH,
        )

    logger.info(
        "RUN | status=completed | duration_sec=%.2f",
        time.monotonic() - start_time,
    )


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
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only output warnings and errors.",
    )
    parser.add_argument(
        "--update-asset-bundle-info-only",
        action="store_true",
        help="Fetch and update asset_bundle_info.json only; do not download bundles.",
    )
    parser.add_argument(
        "--force-full-download",
        action="store_true",
        help="Ignore cached metadata and pending downloads, then download all matched bundles.",
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
    if args.quiet:
        log_level = logging.WARNING
    elif args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    setup_logging_queue()
    region_name = getattr(getattr(config, "REGION", None), "name", "unknown")
    logger.info(
        "RUN | action=start | config=%s | region=%s | verbose=%s",
        args.config,
        region_name,
        args.verbose,
    )

    start_time = time.perf_counter()
    try:
        asyncio.run(
            main(
                update_asset_bundle_info_only=args.update_asset_bundle_info_only,
                force_full_download=args.force_full_download,
            )
        )
    finally:
        logger.info(
            "RUN | action=completed | duration_sec=%.2f",
            time.perf_counter() - start_time,
        )


if __name__ == "__main__":
    cli()
