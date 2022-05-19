from urllib import response
from collections import defaultdict, deque
import requests
from utils import export_json, import_json
from typing import Union
import pathlib
from tqdm import tqdm

WORK_DIR = pathlib.Path("../data")
all_spcs_json = WORK_DIR / "Oceans1876" / "index_species_verified.json"
output_json = WORK_DIR / "Oceans1876" / "idx_spcs_status_col.json"
irmng_data_json: pathlib.Path = (
    WORK_DIR / "Oceans1876" / "index_species_status_irmng.json"
)


# Define return type for json files
Data = Union[dict, list[dict]]

idx_spcs: Data = import_json(all_spcs_json)["species"]
irmng_data: Data = import_json(irmng_data_json)["species"]


res: str = "https://api.catalogueoflife.org/name/matching?"

query_parameter = "q={}"


def get_species_status(species: dict, idx_species: dict) -> dict:
    species_id_list = deque()
    total_null_count = 0
    sp_ids = [key for key in species if species[key] is None]
    species_status = {}
    print(f"Searching {len(sp_ids)} null entries of {len(idx_spcs)} entries")
    for i, s in tqdm(enumerate(sp_ids)):
        species_id_list.append(s)

        new_URI = res + query_parameter.format(idx_species[s]["matchedName"])
        resp = requests.get(new_URI)
        data = resp.json()
        if resp.status_code == 200 and data["type"] != "none":
            species_status[s] = {
                "name": idx_species[s]["matchedName"],
                "type": data["type"],
                "alternatives": data.get("alternatives", None),
                "nameKey": data["nameKey"],
            }
        else:
            species_status[s] = None
            total_null_count += 1

    species_status = {
        "metadata": {
            "null_entries": total_null_count,
        },
        "species": species_status,
    }
    return species_status


if __name__ == "__main__":
    species_status = get_species_status(irmng_data, idx_spcs)
    export_json(output_json, species_status)
