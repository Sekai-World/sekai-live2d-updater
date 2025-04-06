import logging
import tempfile
from typing import Dict, Tuple, Union

from anyio import Path

from bundle import download_deobfuscate_bundle, extract_asset_bundle
from helpers import refresh_cookie

logger = logging.getLogger("live2d")


async def worker(
    name: str,
    dl_info: Tuple[str, Dict],
    config,
    headers: Dict[str, str],
    cookie: str = None,
):
    url, bundle = dl_info

    logger.debug("worker %s processing %s", name, bundle.get("bundleName", url))

    headers, cookie = await refresh_cookie(config, headers, cookie)

    bundle_save_path: Union[Path, None] = None
    tmp_bundle_save_file = None
    if isinstance(config.ASSET_LOCAL_BUNDLE_CACHE_DIR, Path):
        # Save the bundle to the local directory
        bundle_save_path: Path = (
            config.ASSET_LOCAL_BUNDLE_CACHE_DIR / bundle["bundleName"]
        )
        # Create the directory if it doesn't exist
        await bundle_save_path.parent.mkdir(parents=True, exist_ok=True)

        # Download the bundle
        await download_deobfuscate_bundle(
            url,
            bundle_save_path,
            headers=headers,
        )
    else:
        # Save the bundle to the temp directory
        tmp_bundle_save_file = tempfile.NamedTemporaryFile()
        bundle_save_path = Path(tmp_bundle_save_file.name)

        # Download the bundle
        await download_deobfuscate_bundle(
            url,
            bundle_save_path,
            headers=headers,
        )

    # Get the extracted save path
    extracted_save_path: Union[Path, None] = None
    if isinstance(config.ASSET_LOCAL_EXTRACTED_DIR, Path):
        # Save the extracted assets to the local directory
        extracted_save_path = config.ASSET_LOCAL_EXTRACTED_DIR
        # Create the directory if it doesn't exist
        await extracted_save_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        raise ValueError("ASSET_LOCAL_EXTRACTED_DIR must be set to a local directory.")

    try:
        # Skip live2d motions
        if bundle["bundleName"].startswith("live2d/motion"):
            logger.debug("Skipping live2d motion %s", bundle["bundleName"])
            return
        # Extract the bundle
        exported_list = await extract_asset_bundle(
            bundle_save_path,
            bundle,
            extracted_save_path,
            unity_version=config.UNITY_VERSION,
            config=config,
        )
        logger.debug("Extracted %s to %s", bundle["bundleName"], exported_list)

    finally:
        # Clean up the temporary bundle file
        if tmp_bundle_save_file:
            tmp_bundle_save_file.close()
