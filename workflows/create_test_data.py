"""
This script handles the following:
- Extract a subset of data from the `species.json` and `stations.json` files.
- Load this data into separate `.json` files so that it can be used to create POSTGRES_TEST_DB
"""

import json
import pathlib
import sys
from typing import Union

# Type Definition
Data = Union[dict, list[dict]]

WORK_DIR = pathlib.Path("../data")
print(WORK_DIR)
# Define system specific paths
species_json = WORK_DIR / "Oceans1876" / "species.json"
stations_json = WORK_DIR / "Oceans1876" / "stations.json"
ouput_species_json = WORK_DIR / "Oceans1876_subset" / "test_species.json"
ouput_stations_json = WORK_DIR / "Oceans1876_subset" / "test_stations.json"

# Define Cardinality of the Stations subset.
N_stations = 15

# Import JSON data
def import_json(filename: str) -> Data:
    try:
        with open(filename) as jf:
            data = json.load(jf)
        return data
    except FileNotFoundError:
        sys.exit(f"{filename} is missing")


# Import JSON data
def export_json(filename: str, output: Data) -> None:
    try:
        (WORK_DIR / "Oceans1876_subset").mkdir(exist_ok=True)
        with open(filename, "w") as ojf:
            json.dump(output, ojf, indent=4)
    except Exception as e:
        print(e.with_traceback())




def create_subset(species_json_path: str, stations_json_path: str, ouput_stations_json_path: str, ouput_species_json_path: str, N_stations: int) -> None:
    
    # Species only
    species_data = import_json(species_json_path)[
        "species"
    ]  # Dictionary of Species (Dictionary of Dictionaries)

    # Stations only (List of Dictionaries)
    stations_data = import_json(stations_json_path)

    # Sample the Stations Data
    subset_stations = stations_data[:N_stations]  # (List of Dictionaries)
    subset_species = {}  # (Dictionary of Dictionaries)

    for station in subset_stations:  # iterate through stations
        for sp in station["Species"]:
            # stations have various species hence go
            # through each specie
            if sp["name"] not in subset_species.keys():
                # if the specie as already been added then skip,
                # else, add a key: value pair
                subset_species[sp["name"]] = species_data[sp["name"]]

    export_json(ouput_stations_json_path, subset_stations)
    export_json(ouput_species_json_path, subset_species)


if __name__ == "__main__":

    create_subset(species_json, stations_json, ouput_stations_json, ouput_species_json, N_stations)
