import json
import logging
import pathlib

import requests
from pydantic import parse_file_as

from data.schemas.data_sources import DataSources

from .utils import camelcase_to_snakecase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Data Sources")

WORK_DIR = pathlib.Path("./data")

DATASOURCE_URI = "https://verifier.globalnames.org/api/v1/data_sources"
DATA_SOURCES_FILE_PATH = WORK_DIR / "Oceans1876" / "data_sources.json"


def update_data_sources() -> None:
    """
    Update the data sources file (`DATASOURCE_FILE_PATH`)
    from Global Names API (`DATASOURCE_URI`).
    """
    data_sources = parse_file_as(DataSources, DATA_SOURCES_FILE_PATH)

    resp = requests.get(DATASOURCE_URI)
    if resp.status_code == 200:
        data_sources_dict = resp.json()
        for ds in data_sources_dict:
            if ds["id"] in data_sources:
                for k, v in ds.items():
                    setattr(data_sources[ds["id"]], camelcase_to_snakecase(k), v)

        with open(DATA_SOURCES_FILE_PATH, "w") as f:
            json.dump(
                dict(map(lambda d: (d[0], d[1].dict()), data_sources.items())),
                f,
                indent=2,
            )
    else:
        logger.warning(f"Received Status code: {resp.status_code} from the GNAMES API")


if __name__ == "__main__":
    update_data_sources()
