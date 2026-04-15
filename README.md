To run:

```python
# /// script
# dependencies = [
#     "aind_session",
#     "codeocean",
#     "npc_lims",
#     "pydantic-settings",
# ]
# ///

import datetime
import json
import logging
import pathlib
import time

import aind_session
import codeocean.computation
import codeocean.data_asset
import npc_lims
import pydantic
import pydantic_settings

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

client = aind_session.get_codeocean_client()

CAPSULE_ID = "2218efbd-63e0-475b-a140-0d70f2367869"
# https://codeocean.allenneuraldynamics.org/capsule/4240578/tree

DEFAULT_SESSION_IDS: list[str] = ['664851_2023-11-15', '668755_2023-08-31', '759434_2025-02-04', '713655_2024-08-09', '742903_2024-10-22']

# Process names passed to npc_lims.get_session_capsule_pipeline_data_asset.
# Each entry is (process_name, required). Optional assets are skipped if not found.
ASSET_PROCESS_NAMES: list[tuple[str, bool]] = [
    ("sorted", True),
    ("facemap", False),
    ("LPFaceParts", False),
    ("dlc_eye", False),
]


class Settings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(
        cli_parse_args=True,
        cli_kebab_case="all",
        cli_implicit_flags=True,
    )

    session_id: str | None = None
    regenerate: bool = False
    zarr: bool = False
    test: bool = False
    merge_processing: bool = True
    merge_legacy: bool = True
    outpath: pathlib.Path = pydantic.Field(
        default_factory=lambda: pathlib.Path(
            f"nwb_computations_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
        )
    )
    dryrun: bool = False


def get_lp_faceparts_asset(session_id: str) -> codeocean.data_asset.DataAsset | None:
    """Return the latest gamma-encoded lp_faceparts asset, falling back to any lp_faceparts asset."""
    try:
        all_lp_assets = [
            asset
            for asset in npc_lims.get_session_data_assets(session_id)
            if "lp_faceparts" in asset.name
        ]
    except FileNotFoundError:
        return None
    if not all_lp_assets:
        return None

    gamma_assets = [a for a in all_lp_assets if npc_lims.is_lighting_pose_gamma_encoded(a)]
    candidates = gamma_assets or all_lp_assets
    if gamma_assets:
        logger.info(f"{session_id}: using gamma-encoded lp_faceparts asset")
    else:
        logger.warning(f"{session_id}: no gamma-encoded lp_faceparts found, using latest lp_faceparts")
    return npc_lims.get_latest_data_asset(candidates)


def get_data_assets_for_session(session_id: str) -> list[codeocean.computation.DataAssetsRunParam]:
    """Collect raw + pipeline data assets for a session, logging any that are missing."""
    session = aind_session.get_sessions(*session_id.split("_"))[0]
    raw_asset = session.raw_data_asset
    logger.info(f"{session_id}: attaching 'raw' asset {raw_asset.name!r} (id={raw_asset.id!r})")
    assets: list[codeocean.data_asset.DataAsset] = [raw_asset]

    for process_name, required in ASSET_PROCESS_NAMES:
        if process_name == "sorted":
            try:
                asset = npc_lims.get_session_sorted_data_asset(session_id)
            except (ValueError, FileNotFoundError):
                msg = f"{session_id}: no sorted asset found"
                if required:
                    raise
                logger.warning(msg)
                continue
        else:
            try:
                asset = npc_lims.get_session_capsule_pipeline_data_asset(session_id, process_name)
            except FileNotFoundError:
                msg = f"{session_id}: no {process_name!r} asset found"
                if required:
                    raise
                logger.warning(msg)
                continue

        logger.info(f"{session_id}: attaching {process_name!r} asset {asset.name!r} (id={asset.id!r})")
        assets.append(asset)

    return [
        codeocean.computation.DataAssetsRunParam(id=a.id, mount=a.mount)
        for a in assets
    ]


def run_session(session_id: str, settings: Settings) -> dict | None:
    logger.info(f"Processing session_id={session_id!r}")
    try:
        data_asset_params = get_data_assets_for_session(session_id)
    except Exception:
        logger.exception(f"{session_id}: failed to collect assets, skipping")
        return None

    run_params = codeocean.computation.RunParams(
        capsule_id=CAPSULE_ID,
        data_assets=data_asset_params,
        named_parameters=[
            codeocean.computation.NamedRunParam(param_name="session_id", value=session_id),
            codeocean.computation.NamedRunParam(param_name="regenerate", value=str(settings.regenerate).lower()),
            codeocean.computation.NamedRunParam(param_name="zarr", value=str(settings.zarr).lower()),
            codeocean.computation.NamedRunParam(param_name="test", value=str(settings.test).lower()),
            codeocean.computation.NamedRunParam(param_name="merge_processing", value=str(settings.merge_processing).lower()),
            codeocean.computation.NamedRunParam(param_name="merge_legacy", value=str(settings.merge_legacy).lower()),
        ],
    )

    if settings.dryrun:
        logger.info(f"Dry run: skipping run_capsule for session_id={session_id!r}")
        return None

    computation = client.computations.run_capsule(run_params)
    logger.info(f"Started computation {computation.id} for session_id={session_id!r}")
    return {"session_id": session_id, "computation_id": computation.id}


def main(settings: Settings) -> None:
    session_ids = [settings.session_id] if settings.session_id else DEFAULT_SESSION_IDS
    records: list[dict] = []
    for session_id in session_ids:
        record = run_session(session_id, settings)
        if record is not None:
            records.append(record)
            time.sleep(2)

    if not settings.dryrun:
        with settings.outpath.open("w") as f:
            json.dump(records, f, indent=2)
        logger.info(f"Wrote {len(records)} computation records to {settings.outpath}")


if __name__ == "__main__":
    settings = Settings(
        session_id=DEFAULT_SESSION_IDS[0],
        dryrun=False, 
        test=True, 
    )  # type: ignore[call-arg]
    logger.info(f"{settings=}")
    main(settings)
```