import pathlib
from typing import Dict, List, Union
import json

import requests
from utils import export_json, import_json


WORK_DIR: pathlib.Path = pathlib.Path("../data")

all_species_json: pathlib.Path = WORK_DIR / "Oceans1876" / "index_species_verified.json"

index_species_status_json: pathlib.Path = (
    WORK_DIR / "Oceans1876" / "index_species_status.json"
)

datasources_json: pathlib.Path = WORK_DIR / "Oceans1876" / "data_sources.json"

output_json: pathlib.Path = WORK_DIR / "Oceans1876" / "op.json"


# Data = Union[dict, list[dict]]

idx_spcs: dict = import_json(all_species_json)[
    "species"
]  # main verified file to fetch Datasource Id

idx_spcs_stat: dict = import_json(index_species_status_json)[
    "species"
]  # index against null values to fetch

data_sources: List[dict] = import_json(datasources_json)

id_indexed_datasources: dict = dict(
    [(datasource["id"], datasource) for datasource in data_sources]
)

output: Dict[str, dict] = dict()

for spcs_id, spcs_val in idx_spcs_stat.items():
    if spcs_val is None:
        output[spcs_id] = id_indexed_datasources[idx_spcs[spcs_id]["dataSourceId"]]

print(len(output))

export_json(output_json, output)
