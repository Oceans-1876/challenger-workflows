import json
import logging
import pathlib
from typing import Dict, Optional

import requests
from pydantic import BaseModel, parse_file_as

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Data Sources")

WORK_DIR = pathlib.Path("../data")

DATASOURCE_URI = "https://verifier.globalnames.org/api/v1/data_sources"
DATASOURCE_FILE_PATH = WORK_DIR / "Oceans1876" / "data_sources.json"


class DataSourceUrls(BaseModel):
    aphiaRecordsByID: Optional[str]
    aphiaRecordsByMatchNames: Optional[str]
    aphiaSynonymsByID: Optional[str]
    endpoint: Optional[str]
    matchedCanonicalFull: Optional[str]
    matchedCanonicalSimple: Optional[str]
    matchedName: Optional[str]
    web: Optional[str]


class DataSource(BaseModel):
    id: str
    title: str
    titleShort: str
    curation: str
    recordCount: int
    updatedAt: str
    urls: DataSourceUrls = {}
    homeURL: Optional[str] = None
    isOutlinkReady: bool = False


def update_data_sources() -> None:
    resp = requests.get(DATASOURCE_URI)

    data_sources = parse_file_as(Dict[str, DataSource], DATASOURCE_FILE_PATH)

    if resp.status_code == 200:
        data_sources_dict = resp.json()
        for ds in data_sources_dict:
            if ds["id"] in data_sources:
                for k, v in ds.items():
                    setattr(data_sources[ds["id"]], k, v)

        with open(DATASOURCE_FILE_PATH, "w") as f:
            json.dump(
                dict(map(lambda d: (d[0], d[1].dict()), data_sources.items())),
                f,
                indent=2,
            )

    else:
        logger.warning(
            f"Received Status code: {data_sources.status_code} from the GNAMES API"
        )


if __name__ == "__main__":
    update_data_sources()
