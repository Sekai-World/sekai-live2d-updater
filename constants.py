from anyio import Path

from model import SekaiServerRegion

UNITY_FS_CONTAINER_BASE = Path('assets/sekai/assetbundle/resources/')

NUVERSE_REGIONS = [SekaiServerRegion.TW, SekaiServerRegion.KR, SekaiServerRegion.CN]
