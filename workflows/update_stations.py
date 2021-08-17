"""
This script handles the following:
- Extract stations' text from the OCRed text by HathiTrust (`data/HathiTrust`)
- Parse and verify species from the extracted stations' text
- Attach the extract texts and species to the RAMM data (`data/RAMM/stations.csv`)
- Saves the update RAMM data and all the extracted species in `data/Oceans1876`
"""

import datetime
import json
import logging
import pathlib
import re
import subprocess
import sys
import typing

import fuzzysearch
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HathiTrust")

STATION_NAMES_MAX_LEVENSHTEIN_DISTANCE = 4

WORK_DIR = pathlib.Path("../data")


def check_gnames_app(app_name: str, min_version: float) -> str:
    """
    Check if the given gnames app exists on the system
    and its version is not less than min_version.
    If the app does not exist terminate the process.
    If the version is not satisfied, log a warning.

    Parameters
    ----------
    app_name: names of a Global Names (gnames) app
    min_version: minimum version required by the script

    Returns
    -------
    version of the existing gnames app
    """
    try:
        version_text = subprocess.run(
            [app_name, "-V"], check=True, capture_output=True
        ).stdout.decode("utf-8")
        version = re.search(r"version: v(\d+).(\d+)", version_text)
        if version:
            version_number = float(f"{version.groups()[0]}.{version.groups()[1]}")
            if version_number < min_version:
                logger.warning(
                    f"You have {app_name} version {version_number}. "
                    f"The script is tested with {app_name} v{min_version}. "
                    f"The calls to {app_name} might not work as expected."
                )
            return version_text.strip().split("\n")[0]
        else:
            sys.exit(
                f"Could not get {app_name} version. "
                f"The script is tested with {app_name} v{min_version}. "
                "Make sure you have the right version on your system."
            )

    except FileNotFoundError:
        sys.exit(f"{app_name} is missing")
    except subprocess.CalledProcessError:
        sys.exit(
            f"The script is tested with {app_name} v{min_version}. "
            "Make sure you have the right version on your system."
        )


def run() -> None:
    gnfinder_version = check_gnames_app("gnfinder", 0.14)
    gnverifier_version = check_gnames_app("gnverifier", 0.3)

    ramm_stations = pd.read_csv(WORK_DIR / "RAMM" / "stations.csv")

    hathitrust_stations = pd.read_csv(WORK_DIR / "HathiTrust" / "stations.csv").dropna()
    hathitrust_stations["Range"] = hathitrust_stations["Range"].apply(json.loads)

    # Cache the texts based on stations' text identifiers
    stations_texts: typing.Dict[str, typing.List[str]] = {}

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

    for idx, station in hathitrust_stations.iterrows():
        station_text_identifier = station["Text Identifier"]
        logger.info(f"Processing {station_text_identifier}")

        if station_text_identifier not in stations_texts:
            station_text = []

            for section_idx, (section, pages) in enumerate(station["Range"]):
                # Each section page range (`pages`) is a string
                # in the following format: "<start>-<end>"
                for page_idx, page in enumerate(pages.split("-")):
                    filename = (
                        WORK_DIR
                        / "HathiTrust"
                        / section
                        / "texts"
                        / f"{int(page) - 1:08}.txt"
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
                                        idx, page, page_text, results[0].start
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

    hathitrust_stations["Text"] = hathitrust_stations["Text Identifier"].map(
        lambda text_id: "\n".join(stations_texts[text_id])
    )

    all_species = {}  # Holds all species across all stations
    stations_species = {}  # Holds species by stations' text identifier

    for (text_identifier, text_list) in stations_texts.items():
        text_str = "\n".join(text_list)
        stations_texts[text_identifier] = text_str.split("\n")

        logger.info(f"Getting species for {text_identifier}")

        # Use gnfinder to parse species from station text without verification
        with subprocess.Popen(["echo", text_str], stdout=subprocess.PIPE) as echo_proc:
            with subprocess.Popen(
                ["gnfinder", "-f", "compact", "-w", "2"],
                stdin=echo_proc.stdout,
                stdout=subprocess.PIPE,
            ) as gnfinder_proc:
                if gnfinder_proc.stdout:
                    station_species = (
                        json.loads(gnfinder_proc.stdout.read())["names"] or []
                    )

        stations_species[text_identifier] = station_species

        # Use gnverifier to verify the parsed species,
        # if they are not already in `all_species`
        for species in station_species:
            if species["name"] not in all_species:
                with subprocess.Popen(
                    # -s "9" uses World Register of Marine Species
                    ["gnverifier", "-f", "compact", species["name"]],
                    stdout=subprocess.PIPE,
                ) as gnverifier_proc:
                    if gnverifier_proc.stdout:
                        all_species[species["name"]] = json.loads(
                            gnverifier_proc.stdout.read()
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

    # Save the updated RAMM data
    ramm_stations.to_json(
        WORK_DIR / "Oceans1876" / "stations.json", orient="records", indent=2
    )

    # Save all species data
    with open(WORK_DIR / "Oceans1876" / "species.json", "w") as f:
        json.dump(
            {
                "metadata": {
                    "gnfinder": gnfinder_version,
                    "gnverifier": gnverifier_version,
                    "date": str(datetime.datetime.now()),
                },
                "species": all_species,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    run()
