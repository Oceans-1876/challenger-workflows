import pathlib
from collections import defaultdict, deque
from typing import Union

import requests
from utils import export_json, import_json

WORK_DIR = pathlib.Path("../data")
all_spcs_json = WORK_DIR / "Oceans1876" / "index_species_verified.json"
idx_spcs_status_json = WORK_DIR / "Oceans1876" / "index_species_status.json"
output_json = WORK_DIR / "Oceans1876" / "idx_spcs_status.json"

# Define return type for json files
Data = Union[dict, list[dict]]

idx_spcs: Data = import_json(all_spcs_json)["species"]

aphiaSynonymsByIRMNG_ID_URI: str = "https://irmng.org/rest/AphiaSynonymsByIRMNG_ID/{}"
aphiaRecordByIRMNG_ID: str = "https://irmng.org/rest/AphiaRecordByIRMNG_ID/{}"
aphiaRecordsByMatchNames: str = "https://irmng.org/rest/AphiaRecordsByMatchNames?"
aphiaVernacularsByIRMNG_ID: str = "https://irmng.org/rest/AphiaVernacularsByIRMNG_ID/{}"

query_parameter = "scientificnames[]={}"

BATCH_SIZE = (
    50  # set to 50, 50 is the maximum response length that the WoRMS API allows.
)
LIMIT = 0  # development testing

print(f"Total Species Count: {len(idx_spcs.keys())}")

def get_species_status(species: dict, limit: int) -> dict:
    species_batch_list = deque()
    species_id_list = deque()
    status_types_count = defaultdict(int)
    total_null_count = 0
    sp_ids = list(species.keys())
    species_status = {}
    for i, s in enumerate(sp_ids):
        species_batch_list.append(query_parameter.format(species[s]["matchedName"]))
        species_id_list.append(s)
        if ((i != 0) and ((i + 1) % BATCH_SIZE == 0)) or (i == (len(sp_ids) - 1)):
            new_URI = aphiaRecordsByMatchNames + "&".join(list(species_batch_list))
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
                        "currentIRMNG_ID": sp[0]["IRMNG_ID"],
                        "valid_IRMNG_ID": sp[0]["valid_IRMNG_ID"],
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
                    total_null_count += 1
            species_id_list.clear()
            print(f"Progress: {i + 1} species processed.")
            print(f"Total entries in species_status: {len(species_status)}")

        if (limit != 0) and (i + 1) == limit:
            break

    species_status = {
        "metadata": {
            "status_type_count": status_types_count,
            "null_entries": total_null_count,
        },
        "species": species_status,
    }
    return species_status


def get_accepted_species(species_status: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp and sp["status"] != "accepted":
            print(f"Getting data for species number: {i + 1}")
            data = requests.get(aphiaRecordByIRMNG_ID.format(sp["valid_IRMNG_ID"]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                continue
            print(f"Data received length: {len(data)}")

            valid_data = {
                "currentIRMNG_ID": data["IRMNG_ID"],
                "url": data["url"],
                "scientificname": data["scientificname"],
                "authority": data["authority"],
                "valid_name": data["valid_name"],
                "valid_authority": data["valid_authority"],
            }
            sp["validSpeciesData"] = valid_data
        elif sp:
            sp["validSpeciesData"] = None

def get_species_synonyms(species_status: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp:
            print(f"Getting Species {i + 1} Synonyms: ID: {sp['currentIRMNG_ID']}")
            data = requests.get(aphiaSynonymsByIRMNG_ID_URI.format(sp["currentIRMNG_ID"]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                sp["synonyms"] = []
                continue

            print(f"Data received length: {len(data)}")
            synonyms = [
                {
                    "IRMNG_ID": syn["IRMNG_ID"],
                    "url": syn["url"],
                    "scientificname": syn["scientificname"],
                    "authority": syn["authority"],
                }
                for syn in data
            ]
            sp["synonyms"] = synonyms
            sp["moreSynonyms"] = True if len(data) == 50 else False

def get_vernaculars(species_status: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp:
            print(
                f"Getting Species number {i + 1}'s common names. \
                    ID:{sp['valid_IRMNG_ID']}"
            )
            data = requests.get(aphiaVernacularsByIRMNG_ID.format(sp["valid_IRMNG_ID"]))

            sp["commonNames"] = data.json() if data.status_code == 200 else []


if __name__== "__main__":
    species_status = get_species_status(idx_spcs, LIMIT)
    #species_status = import_json(output_json)
    get_accepted_species(species_status["species"])
    get_species_synonyms(species_status["species"])
    get_vernaculars(species_status["species"])
    export_json(output_json, species_status)
