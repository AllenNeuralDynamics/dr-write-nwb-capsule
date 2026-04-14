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
    test: bool = False

logging.basicConfig(level=Settings().logging_level)
logger = logging.getLogger(__name__)


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


def main(settings: Settings | None = None) -> None:
    if settings is None:
        settings = Settings()
    logger.info(f"Running with settings: {settings}")

    session = npc_sessions.Session(settings.session_id)
    
    # _______________________ write NWB _____________________
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

    # __________________ write NWB contents __________________
    internal_paths = lazynwb.get_internal_paths(nwb_path, include_arrays=False)
    (RESULTS_DIR / "nwb_contents.json").write_text(json.dumps(list(internal_paths.keys()), indent=2))

    # __________________ write AIND metadata __________________
    npc_sessions.aind_data_schema.get_instrument_model(session).write_standard_file(RESULTS_DIR)
    npc_sessions.aind_data_schema.get_data_description_model(session).write_standard_file(RESULTS_DIR)
    npc_sessions.aind_data_schema.get_acquisition_model(session).write_standard_file(RESULTS_DIR)
    
    # ______________ write processing metadata  ______________
if __name__ == "__main__":
    main()