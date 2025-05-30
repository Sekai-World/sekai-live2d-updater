"""This module contains functions to download, deobfuscate, and extract asset bundles."""

import orjson as json
import logging
from typing import Dict, List, Tuple

import aiohttp
import UnityPy
import UnityPy.classes
import UnityPy.config
from anyio import Path, open_file

from constants import UNITY_FS_CONTAINER_BASE
from helpers import deobfuscate
from utils.live2d import (
    correct_param_ids,
    extract_params_ids_from_moc3,
    restore_unity_object_to_motion3,
)

logger = logging.getLogger("live2d")


async def download_deobfuscate_bundle(
    url: str, bundle_save_path: Path, headers: Dict[str, str]
) -> Tuple[str, Dict]:
    """Download and deobfuscate the bundle."""
    # Download the bundle
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                # Read the response data
                data = await response.read()
                # Deobfuscate the data
                deobfuscated_data = await deobfuscate(data)
                # Save the deobfuscated data to the file
                async with await open_file(bundle_save_path, "wb") as f:
                    await f.write(deobfuscated_data)
            else:
                raise aiohttp.ClientError(f"Failed to download {url}")


async def extract_asset_bundle(
    bundle_save_path: Path,
    bundle: Dict[str, str],
    extracted_save_path: Path,
    unity_version: str = None,
    config=None,
) -> List[Path]:
    """Extract the asset bundle to the specified directory.

    Args:
        bundle_save_path (Path): _description_
        bundle (Dict[str, str]): _description_
        extracted_save_path (Path): _description_
        unity_version (str, optional): _description_. Defaults to None.
        config (_type_, optional): _description_. Defaults to None.

    Raises:
        ValueError: _description_
        TypeError: _description_
        TypeError: _description_
        TypeError: _description_
        RuntimeError: _description_

    Returns:
        List[Path]: _description_
    """
    UnityPy.config.FALLBACK_UNITY_VERSION = unity_version

    # Load the bundle
    _unity_file = UnityPy.load(bundle_save_path.as_posix())
    # Check if the bundle is valid
    if not _unity_file:
        raise ValueError(f"Failed to load {bundle_save_path}")

    logger.debug("Loaded bundle %s from %s", bundle.get("bundleName"), bundle_save_path)

    exported_files: List[Path] = []
    post_process_addtional_motions: List[Tuple[UnityPy.classes.MonoBehaviour, Path]] = (
        []
    )
    param_id_map: Dict[str, str] = {}
    for unityfs_path, unityfs_obj in _unity_file.container.items():
        relpath = Path(unityfs_path).relative_to(UNITY_FS_CONTAINER_BASE)

        save_path = extracted_save_path / relpath.relative_to(*relpath.parts[:1])

        save_dir = save_path.parent
        if "motion" in save_dir.parts[-1]:
            # Skip the motions for now
            logger.debug("Skipping motion %s", unityfs_path)
            continue
        # Create the directory if it doesn't exist
        await save_dir.mkdir(parents=True, exist_ok=True)

        try:
            match unityfs_obj.type.name:
                case "MonoBehaviour":
                    tree = None
                    try:
                        if unityfs_obj.serialized_type.node:
                            tree = unityfs_obj.read_typetree()
                    except AttributeError:
                        tree = unityfs_obj.read_typetree()
                    logger.debug(
                        "Saving MonoBehaviour %s to %s", unityfs_path, save_path
                    )
                    # Save the typetree to a json file
                    async with await open_file(save_path, "wb") as f:
                        await f.write(json.dumps(tree, option=json.OPT_INDENT_2))
                    exported_files.append(save_path)

                    if (
                        "AdditionalMotionData" in tree
                        and len(tree["AdditionalMotionData"]) > 0
                    ):
                        post_process_addtional_motions.append(
                            (unityfs_obj.read(), save_dir)
                        )
                        logger.debug(
                            "Found AdditionalMotionData in %s: %s",
                            unityfs_path,
                            tree["AdditionalMotionData"],
                        )
                case "TextAsset":
                    data = unityfs_obj.read()
                    if isinstance(data, UnityPy.classes.TextAsset):
                        if save_path.suffix == ".bytes":
                            save_path = save_path.with_suffix("")
                        async with await open_file(save_path, "wb") as f:
                            data_bytes = data.m_Script.encode(
                                "utf-8", "surrogateescape"
                            )
                            await f.write(data_bytes)
                            if save_path.suffix == ".moc3":
                                param_id_map.update(
                                    extract_params_ids_from_moc3(data_bytes)
                                )
                        exported_files.append(save_path)
                    else:
                        raise TypeError(
                            f"Expected TextAsset, got {type(data)} for {unityfs_path}"
                        )
                case "Texture2D" | "Sprite":
                    data = unityfs_obj.read()
                    if isinstance(data, UnityPy.classes.Texture2D) or isinstance(
                        data, UnityPy.classes.Sprite
                    ):
                        # save as png
                        logger.debug(
                            "Saving texture %s to %s",
                            unityfs_path,
                            save_path.with_suffix(".png"),
                        )
                        data.image.save(save_path.with_suffix(".png"))
                        exported_files.append(save_path.with_suffix(".png"))

                        # save as webp
                        logger.debug(
                            "Saving texture %s to %s",
                            unityfs_path,
                            save_path.with_suffix(".png"),
                        )
                        data.image.save(save_path.with_suffix(".webp"))
                        exported_files.append(save_path.with_suffix(".webp"))
                    else:
                        raise TypeError(
                            f"Expected Texture2D or Sprite, got {type(data)} for {unityfs_path}"
                        )
                case "AudioClip":
                    data = unityfs_obj.read()
                    if isinstance(data, UnityPy.classes.AudioClip):
                        for filename, sample_data in data.samples.items():
                            logger.debug(
                                "Saving audio clip %s to %s",
                                filename,
                                save_path.with_name(filename),
                            )
                            async with await open_file(
                                save_path.with_name(filename), "wb"
                            ) as f:
                                await f.write(sample_data)
                            exported_files.append(save_path.with_name(filename))
                    else:
                        raise TypeError(
                            f"Expected AudioClip, got {type(data)} for {unityfs_path}"
                        )
                case "AnimationClip":
                    # export the typetree
                    tree = unityfs_obj.read_typetree()
                    logger.debug(
                        "Saving AnimationClip %s to %s", unityfs_path, save_path
                    )
                    async with await open_file(save_path, "wb") as f:
                        await f.write(json.dumps(tree, option=json.OPT_INDENT_2))
                    exported_files.append(save_path)
                case _:
                    logger.warning(
                        "Unknowen type %s of %s, extracting typetree",
                        unityfs_obj.type.name,
                        unityfs_path,
                    )
                    tree = unityfs_obj.read_typetree()
                    async with await open_file(save_path, "wb") as f:
                        await f.write(json.dumps(tree, option=json.OPT_INDENT_2))
                    exported_files.append(save_path)
        except (ValueError, TypeError, AttributeError, OSError) as e:
            logger.error("Failed to extract %s: %s", unityfs_path, e)
            continue

    # Post process the additional motions
    if len(post_process_addtional_motions) > 0:
        logger.debug("Post processing additional motions")
        for mono_behav, save_dir in post_process_addtional_motions:
            addtional_motions = mono_behav.AdditionalMotionData

            motions = [
                restore_unity_object_to_motion3(motion) for motion in addtional_motions
            ]
            # filter out empty motions
            motions = [motion for motion in motions if motion is not None]
            correct_param_ids(motions, param_id_map)

            # Save the motions to the directory
            all_motion_names = {
                "motions": [name for name, _ in motions],
            }
            all_motion_path = save_dir / "motions" / "BuildMotionData.json"
            await all_motion_path.parent.mkdir(parents=True, exist_ok=True)
            async with await open_file(all_motion_path, "wb") as f:
                await f.write(json.dumps(all_motion_names, option=json.OPT_INDENT_2))
            exported_files.append(all_motion_path)

            for motion_name, motion in motions:
                motion_path = save_dir / "motions" / f"{motion_name}.motion3.json"
                async with await open_file(motion_path, "wb") as f:
                    await f.write(json.dumps(motion, option=json.OPT_INDENT_2))
                exported_files.append(motion_path)
                logger.debug(
                    "Saved additional motion %s to %s", motion_name, motion_path
                )

    return exported_files
