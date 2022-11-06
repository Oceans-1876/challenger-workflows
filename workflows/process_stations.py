"""
This script handles the following:
- Extracts stations' text from the OCRed text by HathiTrust (`data/HathiTrust`)
- Parses and verifies species from the extracted stations' text
- Attaches the extracted texts and species to the RAMM data (`data/RAMM/stations.csv`)
- Saves the updated RAMM data and all the extracted species in `data/Oceans1876`
"""

import argparse
import json
import logging
import pathlib
import re
import sys
from typing import Any, Dict, List

import fuzzysearch
import numpy as np
import pandas as pd

from data.schemas.species.global_names import GNMetadata

from .gnames import GNames
from .utils import PydanticJSONEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HathiTrust")

STATION_NAMES_MAX_LEVENSHTEIN_DISTANCE = 4

WORK_DIR = pathlib.Path("./data")

DEBUG_OUTPUT_PATH = WORK_DIR / "tmp" / "stations"
if not DEBUG_OUTPUT_PATH.exists():
    DEBUG_OUTPUT_PATH.mkdir(parents=True)

RAMM_STATION_COLUMN_TYPES = {
    "Station": "object",
    "Sediment sample": "object",
    "Latitude Degrees": "int64",
    "Latitude Minutes": "int64",
    "Latitude Seconds": "int64",
    "North/South": "object",
    "Longitude Degrees": "int64",
    "Longitude Minutes": "int64",
    "Longitude Seconds": "int64",
    "East/West": "object",
    "Decimal Longitude": "float64",
    "Decimal Latitude": "float64",
    "Location": "object",
    "FAOarea": "int64",
    "Water body": "object",
    "Sea Area": "object",
    "Place": "object",
    "Date": "object",
    "Gear": "object",
    "Depth (fathoms)": "float64",
    "Bottom water temperature (C)": "float64",
    "Bottom temp (F)": "float64",
    "Bottom water depth D (fathoms)": "float64",
    "Specific Gravity at bottom": "float64",
    "Surface temp (C)": "float64",
    "Surface temp (F)": "float64",
    "Specific Gravity at surface": "float64",
    "Temp (F) at 10 Fathoms": "float64",
    "Temp (F) at 20 Fathoms": "float64",
    "Temp (F) at 25 Fathoms": "float64",
    "Temp (F) at 30 Fathoms": "float64",
    "Temp (F) at 40 Fathoms": "float64",
    "Temp (F) at 50 Fathoms": "float64",
    "Temp (F) at 60 Fathoms": "float64",
    "Temp (F) at 70 Fathoms": "float64",
    "Temp (F) at 75 Fathoms": "float64",
    "Temp (F) at 80 Fathoms": "float64",
    "Temp (F) at 90 Fathoms": "float64",
    "Temp (F) at 100 Fathoms": "float64",
    "Temp (F) at 110 Fathoms": "float64",
    "Temp (F) at 120 Fathoms": "float64",
    "Temp (F) at 125 Fathoms": "float64",
    "Temp (F) at 130 Fathoms": "float64",
    "Temp (F) at 140 Fathoms": "float64",
    "Temp (F) at 150 Fathoms": "float64",
    "Temp (F) at 160 Fathoms": "float64",
    "Temp (F) at 170 Fathoms": "float64",
    "Temp (F) at 175 Fathoms": "float64",
    "Temp (F) at 180 Fathoms": "float64",
    "Temp (F) at 190 Fathoms": "float64",
    "Temp (F) at 200 Fathoms": "float64",
    "Temp (F) at 225 Fathoms": "float64",
    "Temp (F) at 250 Fathoms": "float64",
    "Temp (F) at 275 Fathoms": "float64",
    "Temp (F) at 300 Fathoms": "float64",
    "Temp (F) at 400 Fathoms": "float64",
    "Temp (F) at 500 Fathoms": "float64",
    "Temp (F) at 600 Fathoms": "float64",
    "Temp (F) at 700 Fathoms": "float64",
    "Temp (F) at 800 Fathoms": "float64",
    "Temp (F) at 900 Fathoms": "float64",
    "Temp (F) at 1000 Fathoms": "float64",
    "Temp (F) at 1100 Fathoms": "float64",
    "Temp (F) at 1200 Fathoms": "float64",
    "Temp (F) at 1300 Fathoms": "float64",
    "Temp (F) at 1330 Fathoms": "float64",
    "Temp (F) at 1400 Fathoms": "float64",
    "Temp (F) at 1450 Fathoms": "float64",
    "Temp (F) at 1500 Fathoms": "float64",
    "Temp (F) at 1530 Fathoms": "float64",
    "Temp (F) at 1580 Fathoms": "float64",
    "Temp (F) at 1600 Fathoms": "float64",
    "Temp (F) at 1650 Fathoms": "float64",
    "Temp (F) at 1700 Fathoms": "float64",
    "Temp (F) at 1725 Fathoms": "float64",
    "Temp (F) at 1730 Fathoms": "float64",
    "Temp (F) at 1775 Fathoms": "float64",
    "Temp (F) at 1780 Fathoms": "float64",
    "Temp (F) at 1800 Fathoms": "float64",
    "Temp (F) at 1825 Fathoms": "float64",
    "Temp (F) at 1850 Fathoms": "float64",
    "Temp (F) at 1900 Fathoms": "float64",
    "Temp (F) at 1915 Fathoms": "float64",
    "Temp (F) at 1980 Fathoms": "float64",
    "Temp (F) at 2000 Fathoms": "float64",
    "Temp (F) at 2025 Fathoms": "float64",
    "Temp (F) at 2100 Fathoms": "float64",
    "Temp (F) at 2125 Fathoms": "float64",
    "Temp (F) at 2180 Fathoms": "float64",
    "Temp (F) at 2200 Fathoms": "float64",
    "Temp (F) at 2225 Fathoms": "float64",
    "Temp (F) at 2270 Fathoms": "float64",
    "Temp (F) at 2300 Fathoms": "float64",
    "Temp (F) at 2325 Fathoms": "float64",
    "Temp (F) at 2400 Fathoms": "float64",
    "Temp (F) at 2425 Fathoms": "float64",
    "Temp (F) at 2440 Fathoms": "float64",
    "Temp (F) at 2500 Fathoms": "float64",
    "Temp (F) at 2525 Fathoms": "float64",
    "Temp (F) at 2600 Fathoms": "float64",
    "Temp (F) at 2625 Fathoms": "float64",
    "Temp (F) at 2650 Fathoms": "float64",
    "Temp (F) at 2675 Fathoms": "float64",
    "Temp (F) at 2700 Fathoms": "float64",
    "Temp (F) at 2775 Fathoms": "float64",
    "Temp (F) at 2800 Fathoms": "float64",
    "Temp (F) at 2900 Fathoms": "float64",
}


def run(debug: bool = False) -> None:
    gnames = GNames()

    # Load all columns as string, i.e. dtype="object"
    ramm_stations = pd.read_csv(WORK_DIR / "RAMM" / "stations.csv", dtype="object")
    # Fix RAMM stations data types
    for column in ramm_stations.columns:
        # Replace all empty strings with np.nan
        ramm_stations[column].replace(r"^\s*$", np.nan, regex=True, inplace=True)

        # If the target column type is not string, update its type
        if RAMM_STATION_COLUMN_TYPES[column] != "object":
            try:
                ramm_stations[column] = ramm_stations[column].astype(
                    RAMM_STATION_COLUMN_TYPES[column]
                )
            except ValueError as e:
                sys.exit(f"Error in column {column}: {e}")

    hathitrust_stations = pd.read_csv(WORK_DIR / "HathiTrust" / "stations.csv").dropna()
    hathitrust_stations["Range"] = hathitrust_stations["Range"].apply(json.loads)

    # Cache the texts based on stations' text identifiers
    stations_texts: Dict[str, List[str]] = {}

    def update_previous_station_text(
        current_idx: int,
        current_page: str,
        current_text: str,
        current_station_start_index: int,
    ) -> None:
        """
        This is called when a new station is found. At this point,
        we need to trim the text that belongs to the new
        station from the previous station, if they shared a page.
        The previous station text is updated directly in `hathitrust_stations`.

        Parameters
        ----------
        current_idx: row number for the current station in the loaded dataframe
        current_page: start page of the current station
        current_text: full text of the first page of the current station
        current_station_start_index: where the current station text
            starts in the current page (i.e. `current_text`)

        """
        previous_station = hathitrust_stations.loc[current_idx - 1]

        if previous_station["Range"][-1][-1].split("-")[-1] == current_page:
            # Only update if the end page of the previous station is the same
            # as the start page of the current station
            previous_station_text_identifier = previous_station["Text Identifier"]
            if previous_station_text_identifier in stations_texts:
                # Get the offset value for the previous station in the current page
                previous_station_text = stations_texts[previous_station_text_identifier]

                previous_station_text_last_section = previous_station_text[-1]

                previous_station_start_index = current_text.find(
                    previous_station_text_last_section
                )
                previous_station_end_index = (
                    current_station_start_index - previous_station_start_index
                )

                previous_station_text_last_section_updated = (
                    previous_station_text_last_section[:previous_station_end_index]
                )

                previous_station_text[-1] = previous_station_text_last_section_updated
            else:
                logger.warning(
                    f"Previous station does not have text: "
                    f"{previous_station['Station']}"
                )

    station_text_identifier = None
    page_text = None
    for idx, station in hathitrust_stations.iterrows():
        station_text_identifier = station["Text Identifier"]
        logger.info(f"Processing {station_text_identifier}")

        if station_text_identifier not in stations_texts:
            station_text = []

            for section_idx, (section, pages) in enumerate(station["Range"]):
                # Each section page range (`pages`) is a string
                # in the following format: "<start>-<end>"
                pages_range = [int(p) for p in pages.split("-")]
                if len(pages_range) == 1:
                    pages_range.append(pages_range[0])
                pages_range[1] += 1
                for page_idx, page in enumerate(range(*pages_range)):
                    filename = (
                        WORK_DIR
                        / "HathiTrust"
                        / section
                        / "texts"
                        / f"{page - 1:08}.txt"
                    )
                    try:
                        with open(filename, "r") as f:
                            page_text = f.read()

                        if section_idx == 0 and page_idx == 0:
                            # First page of range contains the station text identifier
                            results = fuzzysearch.find_near_matches(
                                station_text_identifier,
                                page_text,
                                max_l_dist=STATION_NAMES_MAX_LEVENSHTEIN_DISTANCE,
                            )
                            if results:
                                results.sort(key=lambda r: r.dist)
                                station_text.append(page_text[results[0].start :])

                                if idx > 0:
                                    # On the first page of a station section,
                                    # update the previous station and remove
                                    # the part that belongs to the new station
                                    update_previous_station_text(
                                        idx, str(page), page_text, results[0].start
                                    )
                            else:
                                logger.warning(
                                    f"Could not find the station name in page: "
                                    f"{station_text_identifier} - {filename}"
                                )
                        else:
                            station_text.append(page_text)
                    except FileNotFoundError:
                        logger.warning(f"File does not exist: {filename}")

            stations_texts[station_text_identifier] = station_text

    if station_text_identifier and page_text:
        stations_texts[station_text_identifier].append(page_text)

    hathitrust_stations["Text"] = hathitrust_stations["Text Identifier"].map(
        lambda text_id: "\n".join(stations_texts[text_id])
    )

    all_species_by_record_id: Dict[
        str, Dict[str, Any]
    ] = {}  # Holds all species across all stations by record id
    stations_species = {}  # Holds species by stations' text identifier

    for (text_identifier, text_list) in stations_texts.items():
        text_str = "\n".join(text_list)
        stations_texts[text_identifier] = text_str.split("\n")

        logger.info(f"Getting species for {text_identifier}")

        # Use gnfinder to parse species from station text without verification
        station_species = gnames.extract(text_str)

        stations_species[text_identifier] = station_species

        # Use gnverifier to verify the parsed species,
        # if they are not already in `all_species_by_name`
        for species in station_species:
            verified_species = gnames.verify(species["name"])
            record_id = verified_species.get("bestResult", {}).get("recordId", None)
            if record_id:
                species["recordId"] = record_id
                if record_id in all_species_by_record_id:
                    if (
                        verified_species["matchType"] == "Exact"
                        and all_species_by_record_id[record_id]["matchType"] != "Exact"
                    ):
                        all_species_by_record_id[record_id] = verified_species
                else:
                    all_species_by_record_id[record_id] = verified_species

    # Rename temp columns so they can be aggregated into one column
    fathom_temp_f = ramm_stations.filter(regex="Temp(.*)")
    ramm_stations.drop(columns=fathom_temp_f.columns, inplace=True)
    ramm_stations["Temp (F) at Fathoms"] = (
        fathom_temp_f.rename(
            columns=lambda c: re.sub(
                r"Temp \(F\) at (\d+) Fathoms(?:\.(\d+))?",
                lambda s: f"{s.groups()[0]}"
                f"{f'-{s.groups()[1]}' if s.groups()[1] else ''}",
                c,
            )
        )
        .to_dict(orient="index")
        .values()
    )

    # Append the stations' text to RAMM data
    hathitrust_stations.set_index("Station", inplace=True)
    ramm_stations["HathiTrust"] = ramm_stations["Station"].map(
        hathitrust_stations.to_dict(orient="index")
    )

    # Append the stations' species to RAMM data
    ramm_stations["Species"] = ramm_stations["HathiTrust"].apply(
        lambda r: stations_species.get(r["Text Identifier"] if pd.notna(r) else None)
    )

    # Save the updated RAMM data
    ramm_stations.to_json(
        WORK_DIR / "Oceans1876" / "stations.json", orient="records", indent=2
    )

    if debug:
        for idx, station in ramm_stations.iterrows():
            station.to_json(
                DEBUG_OUTPUT_PATH / f"{idx+1:03}_{station.Station}.json", indent=2
            )

    # Save all species data
    with open(WORK_DIR / "Oceans1876" / "species.json", "w") as f:
        json.dump(
            {
                "metadata": GNMetadata(
                    gnfinder=gnames.app_versions["gnfinder"],
                    gnverifier=gnames.app_versions["gnverifier"],
                ),
                "species": all_species_by_record_id,
            },
            f,
            indent=2,
            cls=PydanticJSONEncoder,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    run(args.debug)
