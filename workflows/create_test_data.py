"""
This script handles the following:
- Extract a subset of data from the `species.json` and `stations.json` files.
- Load this data into separate `.json` files so that it can be used to
create POSTGRES_TEST_DB
"""

import json
import pathlib
import sys
from datetime import datetime
from typing import Union

# Type Definition
Data = Union[dict, list[dict]]

WORK_DIR = pathlib.Path("../data")

# Define system specific paths
species_json = WORK_DIR / "Oceans1876" / "species.json"
stations_json = WORK_DIR / "Oceans1876" / "stations.json"
output_species_json = WORK_DIR / "Oceans1876_subset" / "species.json"
output_stations_json = WORK_DIR / "Oceans1876_subset" / "stations.json"

# Define Cardinality of the Stations subset.
N_stations = 15


# Import JSON data
def import_json(filename: pathlib.Path) -> Data:
    try:
        with open(filename) as jf:
            data = json.load(jf)
        return data
    except FileNotFoundError:
        sys.exit(f"{filename} is missing")


# Export JSON data
def export_json(filename: pathlib.Path, output: Data) -> None:
    try:
        (WORK_DIR / "Oceans1876_subset").mkdir(exist_ok=True)
        with open(filename, "w") as ojf:
            json.dump(output, ojf, indent=4)
    except Exception as e:
        print(e)


def create_subset(
    species_json_path: pathlib.Path,
    stations_json_path: pathlib.Path,
    output_stations_json_path: pathlib.Path,
    output_species_json_path: pathlib.Path,
    n_stations: int,
) -> None:

    # Species only
    species_data = import_json(species_json_path)

    # Stations only (List of Dictionaries)
    stations_data = import_json(stations_json_path)

    # Sample the Stations Data
    subset_stations = stations_data[:n_stations]  # (List of Dictionaries)
    subset_species = {}  # (Dictionary of Dictionaries)
    subset_species["metadata"] = species_data["metadata"]
    subset_species["metadata"]["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    species = {}

    for station in subset_stations:  # iterate through stations
        for sp in station["Species"]:
            # stations have various species hence go
            # through each specie
            if sp["name"] not in subset_species.keys():
                # if the specie as already been added then skip,
                # else, add a key: value pair
                species[sp["name"]] = species_data["species"][sp["name"]]
    subset_species["species"] = species
    export_json(output_stations_json_path, subset_stations)
    export_json(output_species_json_path, subset_species)


if __name__ == "__main__":
    create_subset(
        species_json,
        stations_json,
        output_stations_json,
        output_species_json,
        N_stations,
    )
