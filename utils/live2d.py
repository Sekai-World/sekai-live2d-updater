import logging
import struct
from io import BytesIO
from typing import Dict, List, Tuple
from zlib import crc32

import orjson as json
import UnityPy
import UnityPy.classes
import UnityPy.config
from anyio import Path, open_file

from constants import UNITY_FS_CONTAINER_BASE

from .binary import BinaryStream

logger = logging.getLogger("live2d")

live2d_target_map = {
    "CubismParameter": ("Parameter", None),
    "CubismPart": ("PartOpacity", None),
    "CubismRenderController": ("Model", "Opacity"),
    "CubismEyeBlinkController": ("Model", "EyeBlink"),
    "CubismMouthController": ("Model", "LipSync"),
}


def format_float(num):
    if isinstance(num, float) and int(num) == num:
        return int(num)
    elif isinstance(num, float):
        return float("{:.3f}".format(num))
    return num


class StreamedCurveKey(object):

    def __init__(self, bs):
        super().__init__()

        self.index: int = bs.readUInt32()
        self.coeff: List[float] = [bs.readFloat() for i in range(3)]

        self.outSlope: float = self.coeff[2]
        self.value: float = bs.readFloat()
        self.inSlope: float = 0.0

    def __repr__(self) -> str:
        return str(
            {
                "index": self.index,
                "coeff": self.coeff,
                "inSlope": self.inSlope,
                "outSlope": self.outSlope,
                "value": self.value,
            }
        )

    def calc_next_in_slope(self, dx, rhs):
        if self.coeff[0] == 0 and self.coeff[1] == 0 and self.coeff[2] == 0:
            return float("Inf")

        dx = max(dx, 0.0001)
        dy = rhs.value - self.value
        length = 1.0 / (dx * dx)
        d1 = self.outSlope * dx
        d2 = dy + dy + dy - d1 - d1 - self.coeff[1] / length

        return d2 / dx


def find_binding(generic_bindings: List[UnityPy.classes.GenericBinding], index: int):
    curves = 0
    for b in generic_bindings:
        if b.typeID == UnityPy.enums.ClassIDType.Transform:
            switch = b.attribute

            if switch in [1, 3, 4]:
                # case 1: #kBindTransformPosition
                # case 3: #kBindTransformScale
                # case 4: #kBindTransformEuler
                curves += 3
            elif switch == 2:  # kBindTransformRotation
                curves += 4
            else:
                curves += 1
        else:
            curves += 1
        if curves > index:
            return b
    return None


def process_streamed_clip(streamed_clip: List[int]) -> List:
    _b = struct.pack("I" * len(streamed_clip), *streamed_clip)
    bs = BinaryStream(BytesIO(_b))

    ret = []
    # key_list = []
    while bs.base_stream.tell() < len(_b):
        time = bs.readFloat()

        num_keys = bs.readUInt32()
        key_list = []

        for _ in range(num_keys):
            key_list.append(StreamedCurveKey(bs))

        assert len(key_list) == num_keys
        if time < 0:
            continue
        ret.append({"time": time, "keyList": key_list})

    for k, v in enumerate(ret):
        if k < 2 or k == len(ret) - 1:
            continue

        for ck in v["keyList"]:
            for fI in range(k - 1, 0, -1):
                pre_frame = ret[fI]
                pre_curve_key = next(
                    (x for x in pre_frame["keyList"] if x.index == ck.index), None
                )

                if pre_curve_key:
                    ck.inSlope = pre_curve_key.calc_next_in_slope(
                        v["time"] - pre_frame["time"], ck
                    )
                    break

    return ret


def read_streamed_data(
    motion: Dict,
    clip_binding_constant: UnityPy.classes.AnimationClipBindingConstant,
    time: float,
    curve_key: StreamedCurveKey,
):
    idx = curve_key.index
    binding_constant = find_binding(clip_binding_constant.genericBindings, idx)
    if binding_constant is None:
        raise RuntimeError(
            f"Failed to find binding constant for {idx} in {clip_binding_constant}"
        )
    mono_script = binding_constant.script.deref().read()
    target, bone_name = live2d_target_map[mono_script.m_Name]
    if not bone_name:
        bone_name = str(binding_constant.path)
    if bone_name:
        track = next((x for x in motion["TrackList"] if x["Name"] == bone_name), None)
        if not track:
            track = {
                "Name": bone_name,
                "Target": target,
                "Curve": [
                    {
                        "time": time,
                        "value": curve_key.value,
                        "inSlope": curve_key.inSlope,
                        "outSlope": curve_key.outSlope,
                        "coeff": curve_key.coeff,
                    }
                ],
            }
            motion["TrackList"].append(track)
        else:
            # track["Target"] = target
            track["Curve"].append(
                {
                    "time": time,
                    "value": curve_key.value,
                    "inSlope": curve_key.inSlope,
                    "outSlope": curve_key.outSlope,
                    "coeff": curve_key.coeff,
                }
            )


def read_curve_data(
    motion: Dict,
    clip_binding_constant: UnityPy.classes.AnimationClipBindingConstant,
    idx: int,
    time: float,
    sample_list: List[float],
    curve_idx: int,
):
    binding_constant = find_binding(clip_binding_constant.genericBindings, idx)
    if binding_constant is None:
        raise RuntimeError(
            f"Failed to find binding constant for {idx} in {clip_binding_constant}"
        )
    mono_script = binding_constant.script.deref().read()
    target, bone_name = live2d_target_map[mono_script.m_Name]
    if not bone_name:
        bone_name = str(binding_constant.path)
    if bone_name:
        track = next((x for x in motion["TrackList"] if x["Name"] == bone_name), None)
        if not track:
            track = {
                "Name": bone_name,
                "Target": target,
                "Curve": [
                    {
                        "time": time,
                        "value": sample_list[curve_idx],
                        "inSlope": 0,
                        "outSlope": 0,
                        "coeff": None,
                    }
                ],
            }
            motion["TrackList"].append(track)
        else:
            # track["Target"] = target
            track["Curve"].append(
                {
                    "time": time,
                    "value": sample_list[curve_idx],
                    "inSlope": 0,
                    "outSlope": 0,
                    "coeff": None,
                }
            )


def restore_unity_object_to_motion3(unity_object) -> Tuple | None:
    """Restore unity game object to motion3 json format"""
    asset_name = unity_object.ClipAssetName

    # Read the animation clip
    # Only allow in-file animation clips
    if unity_object.Clip.m_PathID != 0 and unity_object.Clip.m_FileID == 0:
        animation_clip: UnityPy.classes.AnimationClip = unity_object.Clip.deref().read()
        if not isinstance(animation_clip, UnityPy.classes.AnimationClip):
            raise RuntimeError(
                f"Failed to read animation clip {asset_name}, expected AnimationClip, got {type(animation_clip)}"
            )
    else:
        logger.warning(
            "Clip path id is empty or file id is not 0, reading %s for %s",
            unity_object.Clip,
            asset_name,
        )
        return

    # Read meta data from facial_anim
    name = animation_clip.m_Name
    sample_rate = animation_clip.m_SampleRate
    duration = format_float(animation_clip.m_MuscleClip.m_StopTime)
    motion = {
        "Name": name,
        "SampleRate": sample_rate,
        "Duration": duration,
        "TrackList": [],
        "Events": [],
    }

    assert (
        name == animation_clip.m_Name
    ), f"Name mismatch {name} != {animation_clip.m_Name}"

    logger.debug(
        "Restoring %s with sample rate %s and duration %s", name, sample_rate, duration
    )

    # Read streamed frames
    streamed_frames = process_streamed_clip(
        animation_clip.m_MuscleClip.m_Clip.data.m_StreamedClip.data
    )
    # Read the clip binding constant
    clip_binding_constant = animation_clip.m_ClipBindingConstant
    if not clip_binding_constant:
        raise RuntimeError(f"Failed to read clip binding constant {asset_name}")

    # Fill streamed frames
    for frame in streamed_frames:
        time = frame["time"]
        for curve_key in frame["keyList"]:
            read_streamed_data(motion, clip_binding_constant, time, curve_key)

    # Read dense clip
    dense_clip = animation_clip.m_MuscleClip.m_Clip.data.m_DenseClip
    # Read streamed clip count
    stream_count = animation_clip.m_MuscleClip.m_Clip.data.m_StreamedClip.curveCount

    # Fill curve data
    for frame_idx in range(dense_clip.m_FrameCount):
        time = dense_clip.m_BeginTime + frame_idx / dense_clip.m_SampleRate
        for curve_idx in range(dense_clip.m_CurveCount):
            idx = stream_count + curve_idx
            read_curve_data(
                motion,
                clip_binding_constant,
                idx,
                time,
                dense_clip.m_SampleArray,
                curve_idx,
            )

    # Read constant clip
    constant_clip = animation_clip.m_MuscleClip.m_Clip.data.m_ConstantClip
    # Read dense clip count
    dense_count = dense_clip.m_CurveCount
    # Time correction
    time2 = 0.0
    for _ in range(2):
        for curve_idx in range(len(constant_clip.data)):
            idx = stream_count + dense_count + curve_idx
            read_curve_data(
                motion, clip_binding_constant, idx, time2, constant_clip.data, curve_idx
            )
        time2 = animation_clip.m_MuscleClip.m_StopTime

    # Fill events
    for ev in animation_clip.m_Events:
        motion["Events"].append({"time": ev.time, "value": ev.data})

    # Base motion3 structure
    restored_motion3 = {
        "Version": 3,
        "Meta": {
            "Duration": duration,
            "Fps": sample_rate,
            "Loop": True,
            "AreBeziersRestricted": True,
            "CurveCount": len(motion["TrackList"]),
            "UserDataCount": len(motion["Events"]),
        },
        "Curves": [None] * len(motion["TrackList"]),
        "UserData": [None] * len(motion["Events"]),
    }

    total_segment_count = 0
    total_point_count = 0

    for idx, track in enumerate(motion["TrackList"]):
        restored_motion3["Curves"][idx] = {
            "Target": track["Target"],
            "Id": track["Name"],
            "Segments": [0, format_float(track["Curve"][0]["value"])],
        }
        total_segment_count += 1
        total_point_count += 1

        for j in range(1, len(track["Curve"])):
            curve = track["Curve"][j]
            pre_curve = track["Curve"][j - 1]

            if (
                j + 1 < len(track["Curve"])
                and abs(curve["time"] - pre_curve["time"] - 0.01) < 0.0001
            ):
                next_curve = track["Curve"][j + 1]
                if next_curve["value"] == curve["value"]:
                    restored_motion3["Curves"][idx]["Segments"].extend(
                        [
                            3,
                            format_float(next_curve["time"]),
                            format_float(next_curve["value"]),
                        ]
                    )
                    total_point_count += 1
                    total_segment_count += 1
                    continue

            if curve["inSlope"] == float("inf"):
                restored_motion3["Curves"][idx]["Segments"].extend(
                    [2, format_float(curve["time"]), format_float(curve["value"])]
                )
            elif pre_curve["outSlope"] == 0.0 and abs(curve["inSlope"]) < 0.0001:
                restored_motion3["Curves"][idx]["Segments"].extend(
                    [0, format_float(curve["time"]), format_float(curve["value"])]
                )
            else:
                tangent_len = (curve["time"] - pre_curve["time"]) / 3.0
                restored_motion3["Curves"][idx]["Segments"].extend(
                    [
                        1,
                        format_float(pre_curve["time"] + tangent_len),
                        format_float(
                            pre_curve["outSlope"] * tangent_len + pre_curve["value"]
                        ),
                        format_float(curve["time"] - tangent_len),
                        format_float(curve["value"] - curve["inSlope"] * tangent_len),
                        format_float(curve["time"]),
                        format_float(curve["value"]),
                    ]
                )
                total_point_count += 2

            total_point_count += 1
            total_segment_count += 1

    restored_motion3["Meta"]["TotalSegmentCount"] = total_segment_count
    restored_motion3["Meta"]["TotalPointCount"] = total_point_count

    total_user_data_size = sum(len(ev["value"]) for ev in motion["Events"])
    for idx, ev in enumerate(motion["Events"]):
        restored_motion3["UserData"][idx] = {
            "Time": format_float(ev["time"]),
            "Value": ev["value"],
        }

    restored_motion3["Meta"]["TotalUserDataSize"] = total_user_data_size

    return name, restored_motion3


def correct_param_ids(motions: List[Tuple[str, Dict]], param_id_map: Dict[str, str]):
    """Correct the parameter IDs in the motions"""
    for name, motion in motions:
        for curve in motion["Curves"]:
            try:
                num_id = curve["Id"]
                curve["Id"] = param_id_map[num_id]
            except KeyError:
                logger.warning("unable to find key %s in file %s", curve["Id"], name)


def extract_params_ids_from_moc3(moc3: bytes) -> Dict[str, str]:
    """Extract parameter IDs from moc3 file"""
    bs = BinaryStream(BytesIO(moc3))
    bs.base_stream.seek(0x4C)
    part_base_addr = bs.readUInt32()
    part_end_addr = bs.readUInt32()

    cursor = part_base_addr
    param_id_map = {}

    while part_end_addr - cursor > 64:
        bs.base_stream.seek(cursor)
        param_id = bs.readStringToNull()
        crc = str(crc32(param_id))
        param_id_map[crc] = param_id.decode()
        crc = str(crc32(b"Parts/" + param_id))
        param_id_map[crc] = param_id.decode()

        cursor += 64

    bs.base_stream.seek(0x108)
    param_base_addr = bs.readUInt32()
    param_end_addr = bs.readUInt32()

    cursor = param_base_addr

    while param_end_addr - cursor > 64:
        bs.base_stream.seek(cursor)
        param_id = bs.readStringToNull()
        crc = str(crc32(param_id))
        param_id_map[crc] = param_id.decode()
        crc = str(crc32(b"Parameters/" + param_id))
        param_id_map[crc] = param_id.decode()

        cursor += 64

    return param_id_map


class Live2DBuildMotion:
    ClipAssetName: str
    Clip: UnityPy.classes.PPtr[UnityPy.classes.AnimationClip]

    def __init__(
        self,
        clip_asset_name: str,
        clip: UnityPy.classes.PPtr[UnityPy.classes.AnimationClip],
    ):
        self.ClipAssetName = clip_asset_name
        self.Clip = clip

    def __repr__(self) -> str:
        return str(
            {
                "ClipAssetName": self.ClipAssetName,
                "Clip": self.Clip,
            }
        )


async def restore_live2d_motions(
    local_live2d_motion_bundle_cache_dir: Path,
    local_live2d_motion_extracted_dir: Path,
    local_live2d_model_extracted_dir: Path,
    unity_version: str,
):
    UnityPy.config.FALLBACK_UNITY_VERSION = unity_version

    if not await local_live2d_motion_bundle_cache_dir.exists():
        raise FileNotFoundError(
            f"Motion bundle dir {local_live2d_motion_bundle_cache_dir} does not exist"
        )
    if not await local_live2d_model_extracted_dir.exists():
        raise FileNotFoundError(
            f"Model extracted dir {local_live2d_model_extracted_dir} does not exist"
        )

    # Gather param ID map
    param_id_map: Dict[str, str] = {}
    async for moc3_path in local_live2d_model_extracted_dir.glob("**/*.moc3"):
        async with await open_file(moc3_path, "rb") as f:
            moc3 = await f.read()
            param_id_map.update(extract_params_ids_from_moc3(moc3))
    logger.debug("Param ID map: %s", param_id_map)

    # Process all motion bundles
    async for motion_base_bundle_path in local_live2d_motion_bundle_cache_dir.glob("*"):
        montion_base = UnityPy.load(motion_base_bundle_path.as_posix())
        if not montion_base:
            raise RuntimeError(
                f"Failed to load motion bundle {motion_base_bundle_path}"
            )

        container_items = montion_base.container.items()
        # Find the buildmotiondata
        buildmotiondata_path, buildmotiondata = next(
            (
                (i[0], i[1].read())
                for i in container_items
                if i[1].type.name == "MonoBehaviour"
                and "buildmotiondata" in i[0].lower()
            ),
            None,
        )
        if not buildmotiondata:
            raise RuntimeError(
                f"Failed to find buildmotiondata in {motion_base_bundle_path}"
            )

        facials = [
            restore_unity_object_to_motion3(facial)
            for facial in buildmotiondata.Facials
        ]
        if not facials and not buildmotiondata.Motions:
            logger.warning(
                "No facials found in %s, try searching container items",
                motion_base_bundle_path,
            )
            # Try to find facials in container items
            container_facials = [
                Live2DBuildMotion(Path(asset_path).stem, pptr)
                for asset_path, pptr in container_items
                if Path(asset_path).parent.name == "facial"
                and Path(asset_path).suffix == ".anim"
            ]
            if not container_facials:
                logger.exception(
                    "Failed to find facials in %s after searching container items",
                    motion_base_bundle_path,
                )
                raise RuntimeError(
                    f"Failed to find facials in {motion_base_bundle_path}"
                )
            facials = [
                restore_unity_object_to_motion3(facial) for facial in container_facials
            ]
        # filter out empty facials
        facials = [facial for facial in facials if facial is not None]
        correct_param_ids(facials, param_id_map)

        motions = [
            restore_unity_object_to_motion3(motion)
            for motion in buildmotiondata.Motions
        ]
        if not motions and not buildmotiondata.Motions:
            logger.warning(
                "No motions found in %s, try searching container items",
                motion_base_bundle_path,
            )
            # Try to find motions in container items
            container_motions = [
                Live2DBuildMotion(Path(asset_path).stem, pptr)
                for asset_path, pptr in container_items
                if Path(asset_path).parent.name == "motion"
                and Path(asset_path).suffix == ".anim"
            ]
            if not container_motions:
                logger.exception(
                    "Failed to find motions in %s after searching container items",
                    motion_base_bundle_path,
                )
                raise RuntimeError(
                    f"Failed to find motions in {motion_base_bundle_path}"
                )
            motions = [
                restore_unity_object_to_motion3(motion) for motion in container_motions
            ]
        # filter out empty motions
        motions = [motion for motion in motions if motion is not None]
        correct_param_ids(motions, param_id_map)

        _reldir = Path(buildmotiondata_path).relative_to(UNITY_FS_CONTAINER_BASE).parent
        save_dir = local_live2d_motion_extracted_dir / _reldir.relative_to(
            *_reldir.parts[:1]
        ).relative_to("live2d/motion")
        # Collect and write all motion names
        all_motion_names = {
            "expressions": [name for name, _ in facials],
            "motions": [name for name, _ in motions],
        }
        all_motion_path = save_dir / "BuildMotionData.json"
        await all_motion_path.parent.mkdir(parents=True, exist_ok=True)
        async with await open_file(all_motion_path, "wb") as f:
            await f.write(json.dumps(all_motion_names, option=json.OPT_INDENT_2))

        # Write all facial expressions
        facial_save_dir = save_dir / "facial"
        await facial_save_dir.mkdir(parents=True, exist_ok=True)
        for name, motion in facials:
            async with await open_file(
                facial_save_dir / f"{name}.motion3.json", "wb"
            ) as f:
                await f.write(json.dumps(motion, option=json.OPT_INDENT_2))

        # Write all motions
        motion_save_dir = save_dir / "motion"
        await motion_save_dir.mkdir(parents=True, exist_ok=True)
        for name, motion in motions:
            async with await open_file(
                motion_save_dir / f"{name}.motion3.json", "wb"
            ) as f:
                await f.write(json.dumps(motion, option=json.OPT_INDENT_2))

        logger.info(
            "Restored %s motion data to %s",
            motion_base_bundle_path,
            save_dir,
        )
