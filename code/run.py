import datetime
import json
import logging
import pathlib

import aind_session
import lazynwb
import npc_lims
import npc_sessions
import npc_sessions.aind_data_schema
import pydantic_settings
import upath
from aind_data_schema.components.identifiers import Code
from aind_data_schema.core.processing import DataProcess, Processing, ProcessStage
from aind_data_schema_models.process_names import ProcessName
from aind_metadata_upgrader.processing.v1v2 import ProcessingV1V2

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path("/root/capsule/data")
RESULTS_DIR = pathlib.Path("/root/capsule/results")

class Settings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(
        cli_parse_args=True,
    )

    session_id: str
    regenerate: bool = False # copies from cache if exists
    zarr: bool = False
    test: bool = False # small metadata-only version of NWB for testing
    merge_processing: bool = True  # merge data_processes from input asset processing.json files
    merge_legacy: bool = True  # coerce old/invalid-schema processing.json files into current schema (ignored if merge_processing=False)
    logging_level: str = "INFO"
    
def _copy(src: pathlib.Path | upath.UPath | str, dest_dir: pathlib.Path | upath.UPath | str) -> None:
    src = upath.UPath(src)
    dest = upath.UPath(dest_dir) / src.name
    logger.info(f"Copying {src.as_posix()} to {dest.parent.as_posix()}")
    dest.parent.mkdir(parents=True, exist_ok=True)

    chunk_size = 10 * 1024 * 1024
    with src.open("rb") as fsrc, dest.open("wb") as fdest:
        while True:
            chunk = fsrc.read(chunk_size)
            if not chunk:
                break
            fdest.write(chunk)
    logger.info(f"Done copying {src.name}: {dest.stat().st_size / 1024 / 1024:.0f} MB")


def _get_processing_model(
    start_date_time: datetime.datetime,
    end_date_time: datetime.datetime,
) -> Processing:
    """Build the aind-data-schema Processing model for NWB file generation."""
    data_process = DataProcess(
        process_type=ProcessName.FILE_FORMAT_CONVERSION,
        stage=ProcessStage.PROCESSING,
        code=Code(
            url="https://github.com/AllenInstitute/npc_sessions",
            version=npc_sessions.__version__,
        ),
        experimenters=["Ben Hardcastle"], # experimenter actually means person doing this processing
        start_date_time=start_date_time,
        end_date_time=end_date_time,
    )
    return Processing(data_processes=[data_process])


def _write_processing(
    processing: Processing,
    dest_dir: pathlib.Path,
    merge_processing: bool = True,
    merge_legacy: bool = True,
) -> None:
    """Write processing.json, merging data_processes from any input asset processing.json files.

    Valid Processing models (matching schema version) are merged via __add__.
    Invalid or mismatched-version models are upgraded via ProcessingV1V2 and merged
    via __add__ if merge_legacy=True; assets that fail upgrading are skipped.
    If merge_processing=False, writes only this capsule's DataProcess.
    """
    if not merge_processing:
        processing.write_standard_file(dest_dir)
        return

    for asset_dir in DATA_DIR.iterdir():
        path = asset_dir / "processing.json"
        if not path.exists():
            continue
        text = path.read_text()
        try:
            prior = Processing.model_validate_json(text)
        except Exception:
            pass
        else:
            processing = prior + processing
            logger.info(f"Merged valid Processing model from {asset_dir.name}")
            continue
        if not merge_legacy:
            continue
        raw = json.loads(text)
        try:
            v2_data = ProcessingV1V2().upgrade(raw, Processing.model_fields["schema_version"].default)
        except Exception as e:
            logger.warning(f"Could not upgrade Processing from {asset_dir.name}: {e}")
            continue
        try:
            prior = Processing(**v2_data)
        except Exception as e:
            logger.warning(f"Could not validate upgraded Processing from {asset_dir.name}: {e}")
            continue
        processing = prior + processing
        logger.info(f"Upgraded and merged legacy Processing from {asset_dir.name}")

    processing.write_standard_file(dest_dir)


def main(settings: Settings) -> None:
    logger.info(f"Running with settings: {settings}")

    session = npc_sessions.Session(settings.session_id)
    
    # _______________________ write NWB _____________________
    logger.info(f"Writing NWB file")
    start_date_time = npc_sessions.get_aware_dt(datetime.datetime.now())
    nwb_path = RESULTS_DIR / f"{settings.session_id}.nwb"
    if settings.regenerate or not (existing := npc_lims.get_nwb_path(settings.session_id, version=npc_sessions.__version__, zarr=settings.zarr)).exists():
        logger.info("Generating new NWB")
        if not nwb_path.exists():
            session.write_nwb(
                path=nwb_path,
                metadata_only=settings.test,
                zarr=settings.zarr,
                force=True
            )
    else:
        logger.info("Reusing existing NWB")
        _copy(existing, nwb_path)
    end_date_time = npc_sessions.get_aware_dt(datetime.datetime.now())

    # __________________ write NWB contents __________________
    logger.info(f"Writing contents of NWB file internal paths")
    internal_paths = lazynwb.get_internal_paths(nwb_path, include_arrays=False)
    (RESULTS_DIR / "nwb_contents.json").write_text(json.dumps(list(internal_paths.keys()), indent=2))

    # __________ copy metadata files from raw asset _________
    raw_data_dir = [p for p in DATA_DIR.iterdir() if p.is_dir() and p.name == aind_session.Session(p).id][0]
    logger.info(f"Identified raw data asset dir: {raw_data_dir.as_posix()}")
    for name in ("subject", "procedures", ):
        src = raw_data_dir / f"{name}.json"
        if src.exists():
            logger.info(f"Copying {name}.json")
            _copy(src, RESULTS_DIR)

    # __________________ write AIND metadata __________________
    logger.info(f"Writing instrument.json")
    npc_sessions.aind_data_schema.get_instrument_model(session).write_standard_file(RESULTS_DIR)
    logger.info(f"Writing acquisition.json")
    npc_sessions.aind_data_schema.get_acquisition_model(session).write_standard_file(RESULTS_DIR)
    logger.info(f"Writing data_description.json")
    (
        npc_sessions.aind_data_schema.get_data_description_model(session)
        .model_copy(
            update=dict(
                name=f"{raw_data_dir.name}_nwb_{end_date_time.strftime('%Y-%m-%d_%H-%M-%S')}",
                creation_time=end_date_time,
                data_level="derived",
            )
        )
        .write_standard_file(RESULTS_DIR)
    )
    # ______________ write processing metadata  ______________
    logger.info(f"Writing processing.json")
    _write_processing(
        processing=_get_processing_model(start_date_time, end_date_time),
        dest_dir=RESULTS_DIR,
        merge_processing=settings.merge_processing,
        merge_legacy=settings.merge_legacy,
    )

    
if __name__ == "__main__":
    settings = Settings()
    logging.basicConfig(level=settings.logging_level, force=True)
    main(settings)
