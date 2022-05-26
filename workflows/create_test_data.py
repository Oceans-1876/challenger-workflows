"""
This script handles the following:
- Extracts a subset of data from the `species.json` and `stations.json` files.
- Loads this data into separate `.json` files in `Oceans1876_subset` so that it can
be used for the test database in `challenger-api`.
"""
import json
import pathlib
from datetime import datetime

WORK_DIR = pathlib.Path("./data")
INPUT_DIR = WORK_DIR / "Oceans1876"
OUTPUT_DIR = WORK_DIR / "Oceans1876_test"

OUTPUT_DIR.mkdir(exist_ok=True)

SPECIES_JSON = INPUT_DIR / "species.json"
STATIONS_JSON = INPUT_DIR / "stations.json"
OUTPUT_SPECIES_JSON = OUTPUT_DIR / "species.json"
OUTPUT_STATIONS_JSON = OUTPUT_DIR / "stations.json"


def create_subset(stations_count: int = 15) -> None:
    with open(SPECIES_JSON) as f:
        species_data = json.load(f)

    with open(STATIONS_JSON) as f:
        stations_data = json.load(f)

    subset_stations = stations_data[:stations_count]

    subset_species = {"metadata": species_data["metadata"], "species": {}}
    subset_species["metadata"]["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for station in subset_stations:
        for sp in station["Species"]:
            if "recordId" in sp and sp["recordId"] not in subset_species["species"]:
                subset_species["species"][sp["recordId"]] = species_data["species"][
                    sp["recordId"]
                ]

    with open(OUTPUT_STATIONS_JSON, "w") as f:
        json.dump(subset_stations, f, indent=2)

    with open(OUTPUT_SPECIES_JSON, "w") as f:
        json.dump(subset_species, f, indent=2)


if __name__ == "__main__":
    create_subset()
