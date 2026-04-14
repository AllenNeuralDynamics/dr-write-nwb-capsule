import datetime
import json
import logging
import pathlib
import re

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

logger = logging.getLogger(__name__)

DATA_DIR = pathlib.Path("/root/capsule/data")
RESULTS_DIR = pathlib.Path("/root/capsule/results")

class Settings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(
        cli_parse_args=True,
        cli_implicit_flags=True,
    )

    session_id: str
    regenerate: bool = False # copies from cache if exists
    zarr: bool = False
    logging_level: str = 'INFO'
    test: bool = False # small metadata-only version of NWB for testing
    merge_processing: bool = True  # merge data_processes from input asset processing.json files
    merge_legacy: bool = True  # coerce old/invalid-schema processing.json files into current schema (ignored if merge_processing=False)

def _copy(src: upath.UPath | str, dest_dir: str | upath.UPath) -> None:
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


def _parse_datetime_from_dirname(name: str) -> datetime.datetime:
    """Extract the last YYYY-MM-DD_HH-MM-SS timestamp from an asset directory name."""
    matches = re.findall(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", name)
    if matches:
        return datetime.datetime.strptime(matches[-1], "%Y-%m-%d_%H-%M-%S").replace(
            tzinfo=datetime.timezone.utc
        )
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _coerce_legacy_data_process(dp: dict, experimenter: str | None) -> DataProcess:
    """Map an old-schema DataProcess dict to the current DataProcess model.

    Old required fields that have no new equivalent (input_location, output_location)
    are dropped. Missing required new fields are filled with defaults.
    """
    return DataProcess(
        process_type=dp["name"],  # ProcessName accepts string values directly
        stage=ProcessStage.PROCESSING,
        code=Code(
            url=dp.get("code_url") or "unknown",
            version=dp.get("software_version") or dp.get("code_version"),
            parameters=dp.get("parameters") or None,
        ),
        experimenters=[experimenter] if experimenter else ["unknown"],
        start_date_time=dp["start_date_time"],
        end_date_time=dp.get("end_date_time"),
        output_parameters=dp.get("outputs") or None,
        notes=dp.get("notes"),
    )


def _write_processing(
    processing: Processing,
    dest_dir: pathlib.Path,
    merge_processing: bool = True,
    merge_legacy: bool = True,
) -> None:
    """Write processing.json, merging data_processes from any input asset processing.json files.

    Valid Processing models (matching schema version) are merged via __add__.
    Invalid or mismatched-version models are coerced to the current schema and merged
    via __add__ if merge_legacy=True; individual items that fail coercion are skipped.
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
            processing = prior + processing
            logger.info(f"Merged valid Processing model from {asset_dir.name}")
            continue
        except Exception:
            pass
        if not merge_legacy:
            continue
        raw = json.loads(text)
        experimenter = raw.get("processing_pipeline", {}).get("processor_full_name")
        dps = (raw.get("data_processes")
               or raw.get("processing_pipeline", {}).get("data_processes")
               or [])
        if not isinstance(dps, list):
            # data_processes is a bare dict (e.g. just {"parameters": {...}}):
            # synthesise a single DataProcess from whatever is available
            dt = _parse_datetime_from_dirname(asset_dir.name)
            try:
                coerced = [DataProcess(
                    process_type=ProcessName.OTHER,
                    stage=ProcessStage.PROCESSING,
                    code=Code(
                        url="unknown",
                        parameters=dps.get("parameters") or None,
                    ),
                    experimenters=[experimenter] if experimenter else ["unknown"],
                    start_date_time=dt,
                    notes=asset_dir.name,
                )]
                prior = Processing(data_processes=coerced)
                processing = prior + processing
                logger.info(f"Synthesised DataProcess from bare parameters dict in {asset_dir.name}")
            except Exception as e:
                logger.warning(f"Could not synthesise DataProcess from {asset_dir.name}: {e}")
            continue
        coerced: list[DataProcess] = []
        for dp in dps:
            items = dp if isinstance(dp, list) else [dp]
            for item in items:
                try:
                    coerced.append(_coerce_legacy_data_process(item, experimenter))
                except Exception as e:
                    logger.warning(f"Could not coerce DataProcess from {asset_dir.name}: {e}")
        if coerced:
            prior = Processing(data_processes=coerced)
            processing = prior + processing
            logger.info(f"Coerced and merged {len(coerced)} legacy data_processes from {asset_dir.name}")

    processing.write_standard_file(dest_dir)


def main(settings: Settings) -> None:
    logger.info(f"Running with settings: {settings}")

    session = npc_sessions.Session(settings.session_id)

    # _______________________ write NWB _____________________
    start_date_time = datetime.datetime.now(tz=datetime.timezone.utc)
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
    end_date_time = datetime.datetime.now(tz=datetime.timezone.utc)

    # __________________ write NWB contents __________________
    internal_paths = lazynwb.get_internal_paths(nwb_path, include_arrays=False)
    (RESULTS_DIR / "nwb_contents.json").write_text(json.dumps(list(internal_paths.keys()), indent=2))

    # __________________ write AIND metadata __________________
    npc_sessions.aind_data_schema.get_instrument_model(session).write_standard_file(RESULTS_DIR)
    npc_sessions.aind_data_schema.get_data_description_model(session).write_standard_file(RESULTS_DIR)
    npc_sessions.aind_data_schema.get_acquisition_model(session).write_standard_file(RESULTS_DIR)

    # ______________ write processing metadata  ______________
    _write_processing(
        processing=_get_processing_model(start_date_time, end_date_time),
        dest_dir=RESULTS_DIR,
        merge_processing=settings.merge_processing,
        merge_legacy=settings.merge_legacy,
    )


if __name__ == "__main__":
    settings = Settings()
    logging.basicConfig(level=settings.logging_level)
    main(settings)
