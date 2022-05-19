import pathlib
from collections import defaultdict, deque
from typing import Union, List, Dict

import requests
from utils import export_json, import_json

WORK_DIR = pathlib.Path("../data")
all_species_json = WORK_DIR / "Oceans1876" / "index_species_verified.json"
index_species_status_json = WORK_DIR / "Oceans1876" / "index_species_status.json"
missing_worms_species_from_irmng = (
    WORK_DIR / "Oceans1876" / "missing_worms_species.json"
)

# Define return type for json files
Data = Union[dict, List[dict]]

index_species: Data = import_json(all_species_json)["species"]

API_URIS: Dict[str, Dict[str, str]] = {
    "worms": {
        "records_by_matched_names": "https://marinespecies.org/rest/AphiaRecordsByMatchNames?",
        "records_by_id": "https://marinespecies.org/rest/AphiaRecordByAphiaID/{}",
        "synonym_records_by_id": "https://marinespecies.org/rest/AphiaSynonymsByAphiaID/{}",
        "vernacular_records_by_id": "https://marinespecies.org/rest/AphiaVernacularsByAphiaID/{}",
    },
    "irmng": {
        "records_by_matched_names": "https://irmng.org/rest/AphiaRecordsByMatchNames?",
        "records_by_id": "https://irmng.org/rest/AphiaRecordByIRMNG_ID/{}",
        "synonym_records_by_id": "https://irmng.org/rest/AphiaSynonymsByIRMNG_ID/{}",
        "vernacular_records_by_id": "https://irmng.org/rest/AphiaVernacularsByIRMNG_ID/{}",
    },
}

API_INCOMING_KEY_NAMES: Dict[str, Dict[str, dict]] = {
    "worms": {
        "id": "AphiaID",
        "valid_id": "valid_AphiaID",
        "current_id": "currrentAphiaID",
    },
    "irmng": {
        "id": "IRMNG_ID",
        "valid_id": "valid_IRMNG_ID",
        "current_id": "currrentIRMNG_ID",
    },
}

API_OUTPUT_PATH: Dict[str, pathlib.Path] = {
    "worms": index_species_status_json,
    "irmng": missing_worms_species_from_irmng,
}


# Only used for WORMS API because we can batch our requests together.
query_param = "scientificnames[]={}"

BATCH_SIZE = (
    50  # set to 50, 50 is the maximum response length that the WoRMS API allows.
)
LIMIT = 0  # development testing

print(f"Total Species Count: {len(index_species.keys())}")


def get_species_status(species: dict, limit: int, URI: str, params: dict) -> dict:
    species_batch_list = deque()
    species_id_list = deque()
    status_types_count = defaultdict(int)
    null_count = 0
    sp_ids = list(species.keys())
    species_status = {}
    for i, s in enumerate(sp_ids):
        species_batch_list.append(query_param.format(species[s]["matchedName"]))
        species_id_list.append(s)
        if ((i != 0) and ((i + 1) % BATCH_SIZE == 0)) or (i == (len(sp_ids) - 1)):
            new_URI = URI + "&".join(list(species_batch_list))
            print("Number of species being requested now: ", len(species_batch_list))
            print(f"Progress: {i + 1 - BATCH_SIZE} species processed so far.")
            species_batch_list.clear()

            data = requests.get(new_URI).json()
            print(f"Response list length: {len(data)}")

            print("#" * 40)
            for id, sp in zip(species_id_list, data):
                print(f"Entries in response list object: {len(sp)}")
                if len(sp) >= 1:
                    species_status[id] = {
                        "name": species[id]["matchedName"],
                        params["current_id"]: sp[0][params["id"]],
                        params["valid_id"]: sp[0][params["valid_id"]],
                        "status": sp[0]["status"],
                        "unacceptreason": sp[0]["unacceptreason"],
                        "outUrl": sp[0]["url"],
                        "scientificname": sp[0]["scientificname"],
                        "isBrackish": sp[0]["isBrackish"]
                        if sp[0]["isBrackish"] is not None
                        else 0,
                        "isExtinct": sp[0]["isExtinct"]
                        if sp[0]["isExtinct"] is not None
                        else 0,
                        "isFreshwater": sp[0]["isFreshwater"]
                        if sp[0]["isFreshwater"] is not None
                        else 0,
                        "isMarine": sp[0]["isMarine"]
                        if sp[0]["isMarine"] is not None
                        else 0,
                        "isTerrestrial": sp[0]["isTerrestrial"]
                        if sp[0]["isTerrestrial"] is not None
                        else 0,
                        "multipleResultsexisted": True if len(sp) > 1 else False,
                    }
                    status_types_count[sp[0]["status"]] += 1
                else:
                    species_status[id] = None
                    null_count += 1
            species_id_list.clear()
            print(f"Progress: {i + 1} species processed.")
            print(f"Total entries in species_status: {len(species_status)}")

        if (limit != 0) and (i + 1) == limit:
            break

    species_status = {
        "metadata": {
            "status_type_count": status_types_count,
            "null_entries": null_count,
        },
        "species": species_status,
    }
    return species_status


def get_accepted_species(species_status: dict, URI: str, params: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp and sp["status"] != "accepted":
            print(f"Getting data for species number: {i + 1}")
            data = requests.get(URI.format(sp[params["valid_id"]]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                continue
            print(f"Data received length: {len(data)}")

            valid_data = {
                params["current_id"]: data[params["id"]],
                "url": data["url"],
                "scientificname": data["scientificname"],
                "authority": data["authority"],
                "valid_name": data["valid_name"],
                "valid_authority": data["valid_authority"],
            }
            sp["validSpeciesData"] = valid_data
        elif sp:
            sp["validSpeciesData"] = None


def get_species_synonyms(species_status: dict, URI: str, params: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp:
            print(f"Getting Species {i + 1} Synonyms: ID: {sp[params['current_id']]}")
            data = requests.get(URI.format(sp[params["current_id"]]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                sp["synonyms"] = []
                continue

            print(f"Data received length: {len(data)}")
            synonyms = [
                {
                    params["id"]: syn[params["id"]],
                    "url": syn["url"],
                    "scientificname": syn["scientificname"],
                    "authority": syn["authority"],
                }
                for syn in data
            ]
            sp["synonyms"] = synonyms
            sp["moreSynonyms"] = True if len(data) == 50 else False


def get_vernaculars(species_status: dict, URI: str, params: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp:
            print(
                f"Getting Species number {i + 1}'s common names. \
                    ID:{sp[params['valid_id']]}"
            )
            data = requests.get(URI.format(sp[params["valid_id"]]))

            sp["commonNames"] = data.json() if data.status_code == 200 else []


if __name__ == "__main__":
    working_on: str = "irmng"
    work_on_missing = True
    # work_on_missing = False

    URI: str = API_URIS[working_on]["records_by_matched_names"]
    accepted_URI: str = API_URIS[working_on]["records_by_id"]
    synonyms_URI: str = API_URIS[working_on]["synonym_records_by_id"]
    vernaculars_URI: str = API_URIS[working_on]["vernacular_records_by_id"]

    params: dict = API_INCOMING_KEY_NAMES[working_on]
    # output_file_path: pathlib.Path = API_OUTPUT_PATH[working_on]
    output_file_path: pathlib.Path = (
        WORK_DIR / "Oceans1876" / "index_species_status_irmng.json"
    )
    # constructing data
    if work_on_missing:
        index_species_status = import_json(index_species_status_json)["species"]
        index_species = dict(
            [
                (key, index_species[key])
                for key in index_species_status.keys()
                if index_species_status[key] == None
            ]
        )
    print(
        f"Working on missing:{work_on_missing} with {working_on} datasource, with {len(index_species)} number of species."
    )
    species_status = get_species_status(index_species, LIMIT, URI, params)

    # species_status = import_json(output_file_path)

    get_accepted_species(species_status["species"], accepted_URI, params)

    get_species_synonyms(species_status["species"], synonyms_URI, params)

    get_vernaculars(species_status["species"], vernaculars_URI, params)

    export_json(output_file_path, species_status)
