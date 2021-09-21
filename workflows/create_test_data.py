"""
This script handles the following:
- Extract a subset of data from the `species.json` and `stations.json` files.
- Load this data into separate `.json` files so that it can be used to create POSTGRES_TEST_DB
"""

import json
import os
import sys
from typing import Union

# Type Definition
Data = Union[dict, list[dict]]

# Define system specific paths
species_json = os.path.join("data", "Oceans1876", "species.json")
stations_json = os.path.join("data", "Oceans1876", "stations.json")
ouput_species_json = os.path.join("data", "Oceans1876_subset", "test_species.json")
ouput_stations_json = os.path.join("data", "Oceans1876_subset", "test_stations.json")

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
        os.makedirs(os.path.join("data", "Oceans1876_subset"), exist_ok=True)
        with open(filename, "w") as ojf:
            json.dump(output, ojf, indent=4)
    except Exception as e:
        print(e.with_traceback())


# Species only
species_data = import_json(species_json)[
    "species"
]  # Dictionary of Species (Dictionary of Dictionaries)

# Stations only (List of Dictionaries)
stations_data = import_json(stations_json)

# Sample the Stations Data
subset_stations = stations_data[:N_stations]  # (List of Dictionaries)
subset_species = {}  # (Dictionary of Dictionaries)


if __name__ == "__main__":

    for station in subset_stations:  # iterate through stations
        for specie in station["Species"]:
            # stations have various species hence go
            # through each specie
            if specie["name"] not in subset_species.keys():
                # if the specie as already been added then skip,
                # else, add a key: value pair
                subset_species[specie["name"]] = species_data[specie["name"]]

    export_json(ouput_stations_json, subset_stations)
    export_json(ouput_species_json, subset_species)
