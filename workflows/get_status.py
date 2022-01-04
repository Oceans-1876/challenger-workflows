import pathlib
from collections import defaultdict, deque
from typing import Union

import requests
from utils import export_json, import_json

WORK_DIR = pathlib.Path("../data")
all_species_json = WORK_DIR / "Oceans1876" / "index_species_verified.json"
index_species_status_json = WORK_DIR / "Oceans1876" / "index_species_status.json"

# Define return type for json files
Data = Union[dict, list[dict]]

index_species: Data = import_json(all_species_json)["species"]

aphiaSynonymsByID_URI: str = "https://marinespecies.org/rest/AphiaSynonymsByAphiaID/{}"
aphiaRecordsByID_URI: str = "https://marinespecies.org/rest/AphiaRecordByAphiaID/{}"
aphiaRecordsByMatchNames: str = (
    "https://marinespecies.org/rest/AphiaRecordsByMatchNames?"
)
vernacularsByAphiaID: str = (
    "https://marinespecies.org/rest/AphiaVernacularsByAphiaID/{}"
)

query_param = "scientificnames[]={}"

BATCH_SIZE = (
    50  # set to 50, 50 is the maximum response length that the WoRMS API allows.
)
LIMIT = 0  # development testing

print(f"Total Species Count: {len(index_species.keys())}")


def get_species_status(species: dict, limit: int) -> dict:
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
                        "currentAphiaID": sp[0]["AphiaID"],
                        "valid_AphiaID": sp[0]["valid_AphiaID"],
                        "status": sp[0]["status"],
                        "unacceptreason": sp[0]["unacceptreason"],
                        "outUrl": sp[0]["url"],
                        "scientificname": sp[0]["scientificname"],
                        "isBrackish": sp[0]["isBrackish"],
                        "isExtinct": sp[0]["isExtinct"],
                        "isFreshwater": sp[0]["isFreshwater"],
                        "isMarine": sp[0]["isMarine"],
                        "isTerrestrial": sp[0]["isTerrestrial"],
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


def get_accepted_species(species_status: dict) -> None:
    for i, sp in enumerate(species_status.values()):
        if sp and sp["status"] != "accepted":
            print(f"Getting data for species number: {i + 1}")
            data = requests.get(aphiaRecordsByID_URI.format(sp["valid_AphiaID"]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                continue
            print(f"Data received length: {len(data)}")

            valid_data = {
                "AphiaID": data["AphiaID"],
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
            print(f"Getting Species {i + 1} Synonyms: ID: {sp['currentAphiaID']}")
            data = requests.get(aphiaSynonymsByID_URI.format(sp["currentAphiaID"]))
            if data.status_code == 200:
                data = data.json()
            else:
                print(f"Response code for: {i + 1} = {data.status_code}")
                sp["synonyms"] = []
                continue

            print(f"Data received length: {len(data)}")
            synonyms = [
                {
                    "AphiaID": syn["AphiaID"],
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
                f"Getting Species number {i + 1}'s common names. ID:{sp['valid_AphiaID']}"
            )
            data = requests.get(vernacularsByAphiaID.format(sp["valid_AphiaID"]))

            sp["commonNames"] = data.json() if data.status_code == 200 else []


if __name__ == "__main__":
    species_status = get_species_status(index_species, LIMIT)

    # species_status = import_json(index_species_status_json)

    get_accepted_species(species_status["species"])

    get_species_synonyms(species_status["species"])

    get_vernaculars(species_status["species"])

    export_json(index_species_status_json, species_status)
