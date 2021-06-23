import json
import logging
import subprocess
from pathlib import Path
from typing import Any, List, Optional

import click
import pandas as pd
import parsers
import pdf2image
from models import Station

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

VOLUMES = {
    "s1": "Challenger Summary part 1.pdf",
    "s2": "Challenger Summary part 2.pdf",
}

WORKING_DIR = Path("data")


def jsonify(obj: Any) -> str:
    return json.dumps(obj, indent=2)


def create_new_station(volume: str) -> Station:
    """
    Create an empty Station instance for the given volume.

    Parameters
    ----------
    volume : str

    Returns
    -------
    station : Station
        An empty instance of Station class
    """
    return Station(
        station=None,
        volume=volume,
        start_page=None,
        start_page_line=None,
        end_page=None,
        end_page_line=None,
        raw_text=[],
        date=None,
        raw_coordinates=None,
        lat=None,
        long=None,
        raw_air_temp_noon=None,
        raw_air_temp_daily_mean=None,
        air_temp_daily_mean=None,
        air_temp_noon=None,
        raw_water_temp=None,
        water_temp_bottom=None,
        water_temp_surface=None,
        raw_water_density=None,
        water_density_bottom=None,
        water_density_surface=None,
        species_name=None,
        offset_start=None,
        offset_end=None,
        canonical_form=None,
        data_source_id=None,
        data_source_title=None,
        taxonId=None,
        classification_path=[],
        classification_path_rank=[],
        errors={},
    )


def process_pdf():
    """
    Convert PDF reports from `<WORKING_DIR>/pdfs` to images.
    Extracted images will be saved in the following path: `<WORKING_DIR>/<VOLUME>/pages/images/<PAGE_NUMBER>.png`.
    `PAGE_NUMBER` has 8 digits with leading zeros.
    """
    for volume, pdf in VOLUMES.items():
        images_path = WORKING_DIR / volume / "pages" / "images"
        texts_path = WORKING_DIR / volume / "pages" / "texts"
        images_path.mkdir(parents=True, exist_ok=True)
        texts_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing {pdf}...")
        images = pdf2image.convert_from_path(WORKING_DIR / "pdfs" / pdf)
        logger.info(f"Converted {volume} ({pdf}) to image")

        for idx, image in enumerate(images):
            filename = f"{idx:08}"
            image.save(images_path / f"{filename}.png")
            logger.info(f"Saved {filename}.png")


def parse_data():
    """
    Process all volumes, extract stations data, and merge station data for each volume in
    `<WORKING_DIR>/<VOLUME>/stations/stations.csv`.
    """
    for volume in VOLUMES.keys():
        texts_path = WORKING_DIR / volume / "pages" / "texts"
        if not texts_path.exists():
            logger.warning(f"Could not find the text files for {volume}.")
            continue

        stations_output_path = WORKING_DIR / volume / "stations"
        stations_output_path.mkdir(exist_ok=True)

        logger.info(f"Processing {volume} stations...")

        stations = extract_volume_stations(volume)
        stations.to_csv(WORKING_DIR / volume / "stations" / "stations.csv")


def extract_volume_stations(volume: str) -> pd.DataFrame:
    """Break summary texts into sections for each station, put in dictionary."""
    stations_output_path = WORKING_DIR / volume / "stations"
    stations = []

    current_station = create_new_station(volume)
    station_index = 0
    station_text = []

    def add_station():
        current_station["end_page"] = int(text_file_path.stem[4:])
        current_station["end_page_line"] = line_number
        current_station["raw_text"] = station_text
        get_environment_info(current_station)
        # parse_species_names_gnrd(current_station)

        with open(stations_output_path / f"{station_index:08}.json", "w") as json_file:
            json_file.write(jsonify(current_station))

        stations.append(current_station)

    text_file_paths = list((WORKING_DIR / volume / "pages" / "texts").glob("*.txt"))
    text_file_paths.sort()

    for text_file_path in text_file_paths:
        logger.info(f"Processing {text_file_path}")

        with open(text_file_path, mode="r", encoding="utf-8") as text_file:
            text = text_file.read().strip()

        lines = text.split("\n")

        for line_number, line_text in enumerate(lines):
            line_cleaned = line_text.strip()

            # example: Station 16 (Sounding 60)
            if "(Sounding" in line_cleaned:
                # Found a new station

                if station_index > 0:
                    add_station()

                current_station = create_new_station(volume)
                station_text = []

                station_index += 1
                station_name = (
                    line_cleaned[: line_cleaned.find("(Sounding")]
                    .strip()
                    .encode("ascii", errors="ignore")
                    .decode("utf8")
                )

                logger.info(
                    f"Found {station_name} ({text_file_path} -> {station_index:08}.json)"
                )

                current_station["station"] = station_name
                current_station["start_page"] = int(text_file_path.stem[4:])
                current_station["start_page_line"] = line_number

            station_text.append(line_cleaned)

    if current_station:
        add_station()

    return pd.DataFrame(stations)


def merge_files():
    """
    Merge volumes' stations from `<WORKING_DIR>/<VOLUME>/stations/stations.csv` into `<WORKING_DIR>/stations.csv`.
    """
    concatenated_output = []
    for volume in VOLUMES.keys():
        concatenated_output.append(
            pd.read_csv(WORKING_DIR / volume / "stations" / "stations.csv")
        )
    pd.concat(concatenated_output).reset_index().to_csv(WORKING_DIR / "stations.csv")


def parse_stations(stations_str: str):
    """
    Parse the data for the given stations and update their json files.

    Parameters
    ----------
    stations_str : str
        A comma-separated list of stations to parse in the following format: <volume>/<station_index>
        `station_index` is the name of the station json file. Preceding zeros are not required.
    """
    for station in stations_str.split(","):
        (volume, station_idx) = station.split("/")
        filename = WORKING_DIR / volume / "stations" / f"{int(station_idx):08}.json"
        if filename.exists():
            with open(filename, "r") as station_file:
                station_json: Station = json.loads(station_file.read())
            station_json["errors"] = {}
            get_environment_info(station_json)
            parse_species_names_gnrd(station_json)
            with open(filename, "w") as station_file:
                station_file.write(jsonify(station_json))


def parse_species_names_gnrd(station: Station):
    # TODO update and document
    gnfinder = subprocess.run(
        ["gnfinder", "find", "-c", "-l", "eng"],
        stdout=subprocess.PIPE,
        input="\n".join(station["raw_text"]),
        encoding="utf-8",
    )

    try:
        names = json.loads(gnfinder.stdout).get("names")
        if names:
            for name_dict in names:
                best_results = name_dict["verification"]["bestResult"]

                station.update(
                    {
                        "species_name": name_dict["name"],
                        "offset_start": name_dict["start"],
                        "offset_end": name_dict["end"],
                        "canonical_form": best_results.get("matchedCanonicalFull"),
                        "data_source_id": best_results.get("dataSourceId"),
                        "data_source_title": best_results.get("dataSourceTitle"),
                        "taxonId": best_results.get("taxonId"),
                        "classification_path": best_results.get(
                            "classificationPath", ""
                        ).split("|"),
                        "classification_path_rank": best_results.get(
                            "classificationRank", ""
                        ).split("|"),
                    }
                )
    except json.JSONDecodeError:
        station["errors"]["species"] = ["no species json"]


# def verify_species_names_gni(df: pd.DataFrame) -> pd.DataFrame:
#     """ Request http://resolver.globalnames.org/name_resolvers.json """
#
#     is_known_name_list = []
#     data_source_list = []
#     gni_uuid_list = []
#     classification_path_list = []
#     classification_path_rank_list = []
#     vernaculars_list = []
#     canonical_form_list = []
#
#     for _, row in df.iterrows():
#         name = row.species_name
#
#         query_params = [
#             f"names={name.replace(' ', '+')}",
#             "with_context=true",
#             "header_only=false",
#             "with_canonical_ranks=true",
#             "with_vernaculars=true",
#             "best_match_only=true",
#             "resolve_once=false",  # first match ==> much faster
#         ]
#         url_full = f"http://resolver.globalnames.org/name_resolvers.json?{'&'.join(query_params)}"
#
#         logger.info(f"Checking {url_full}...")
#
#         try:
#             r = requests.get(url_full)
#
#             # r.json() is what is returned by the server
#             logger.info(f"Verified {name}.")
#             for name_dict in r.json()["data"]:
#                 try:
#                     is_known_name_list.append(name_dict["is_known_name"])
#                 except Exception as e:
#                     logger.warning(e)
#                     is_known_name_list.append("")
#
#                 try:
#                     data_source_list.append(
#                         name_dict["results"][0]["data_source_title"]
#                     )
#                 except Exception as e:
#                     logger.warning(e)
#                     data_source_list.append("")
#
#                 try:
#                     gni_uuid_list.append(name_dict["results"][0]["gni_uuid"])
#                 except Exception as e:
#                     logger.warning(e)
#                     gni_uuid_list.append("")
#
#                 try:
#                     canonical_form_list.append(
#                         name_dict["results"][0]["canonical_form"]
#                     )
#                 except Exception as e:
#                     logger.warning(e)
#                     canonical_form_list.append("")
#
#                 try:
#                     classification_path_list.append(
#                         name_dict["results"][0]["classification_path"]
#                     )
#                 except Exception as e:
#                     logger.warning(e)
#                     classification_path_list.append("")
#
#                 try:
#                     classification_path_rank_list.append(
#                         name_dict["results"][0]["classification_path_ranks"]
#                     )
#                 except Exception as e:
#                     logger.warning(e)
#                     classification_path_rank_list.append("")
#
#                 try:
#                     vernaculars_list.append(
#                         name_dict["results"][0]["vernaculars"][0]["name"]
#                     )
#                 except Exception as e:
#                     logger.warning(e)
#                     vernaculars_list.append("")
#         except Exception as e:
#             logger.error(e)
#             is_known_name_list.append("")
#             data_source_list.append("")
#             gni_uuid_list.append("")
#             canonical_form_list.append("")
#             classification_path_list.append("")
#             classification_path_rank_list.append("")
#             vernaculars_list.append("")
#
#     df["vernacular"] = vernaculars_list
#     df["canonical_form"] = canonical_form_list
#     df["verified"] = is_known_name_list
#     df["data_source"] = data_source_list
#     df["gni_uuid"] = gni_uuid_list
#     df["classification_path"] = classification_path_list
#     df["classification_path_rank"] = classification_path_rank_list
#
#     return df


def get_environment_info(station: Station):
    """
    Parse the environmental data for the given station and update the relevant attributes on the instance.

    Parameters
    ----------
    station : Station
        An instance of Station class. It must have `raw_text` attribute.
    """
    text = "\n".join(station["raw_text"])

    (date, date_errors) = parsers.date(text)
    station["date"] = date
    station["errors"].update(date_errors)

    (coords, coords_errors) = parsers.coordinates(text)
    station.update(coords)
    station["errors"].update(coords_errors)

    (air_temperature, air_temperature_errors) = parsers.air_temperature(text)
    station.update(air_temperature)
    station["errors"].update(air_temperature_errors)

    (water_temperature, water_temperature_errors) = parsers.water_temperature(text)
    station.update(water_temperature)
    station["errors"].update(water_temperature_errors)

    (density, density_errors) = parsers.density(text)
    station.update(density)
    station["errors"].update(density_errors)


def get_stations_with_errors(errors: List[str]):
    """
    Print out list of all station json files that have an error key in the given `errors` list.

    Parameters
    ----------
    errors : List[str]
        A list of error keys. Keys must be from the values in `Error` enum or `any` to consider any error.
    """
    any_error = "any" in errors
    for volume in VOLUMES.keys():
        for file_name in (WORKING_DIR / volume / "stations").iterdir():
            if file_name.suffix == ".json":
                with open(file_name, "r") as f:
                    station_errors = json.loads(f.read())["errors"]
                    if (any_error and station_errors) or any(
                        [error in station_errors for error in errors]
                    ):
                        print(file_name)


def get_stations_with_missing_attrs(keys: List[str]):
    """
    Print out list of all station json files that have a missing value for any of the given keys.

    Parameters
    ----------
    keys : List[str]
        A list of `Station` attributes
    """
    for volume in VOLUMES.keys():
        for file_name in (WORKING_DIR / volume / "stations").iterdir():
            if file_name.suffix == ".json":
                with open(file_name, "r") as f:
                    station = json.loads(f.read())
                    for key in keys:
                        if key in station and not station[key]:
                            print(file_name)
                            break


@click.command(no_args_is_help=True)
@click.option("--parse", is_flag=True, help="Parse data from converted text.")
@click.option("--pdf", is_flag=True, help="Split PDFs into images.")
@click.option(
    "--stations",
    type=str,
    help="""
    Parse the given stations and update their json and entries in CSVs.\n
    Accepts a comma-separated list of stations in the following format:\n
    <volume>/<station_index>\n
    `station_index` is the number at the beginning of a station json file in `data/<volume>/stations/`.
    You can leave out the leading zeros for index.\n
    \b
    Example: --stations s1/12,s1/13,s2/115 (parses stations 12 and 13 of summary 1 and station 115 of summary 2).
    """,
)
@click.option(
    "--errors",
    type=str,
    help="""
    Print out stations with a given error.\n
    Accepts a comma-separated list of error keys.\n
    You can pass "any" to get all stations with any kind of error.\n
    Example: --errors date,lat,long (find all stations with error for their date, lat, and longs fields).
    """,
)
@click.option(
    "--missing",
    type=str,
    help="""
    Print out stations with no value for the given attributes.\n
    Accepts a comma-separated list of attributes.\n
    Example: --missing air_temp_noon,date (find all stations with no value for air_temp_noon and date).
    """,
)
def main(
    pdf: bool,
    parse: bool,
    stations: Optional[str],
    errors: Optional[str],
    missing: Optional[str],
):
    if pdf:
        process_pdf()
    if parse:
        parse_data()
        merge_files()
    if stations:
        parse_stations(stations)
    if errors:
        get_stations_with_errors(errors.split(","))
    if missing:
        get_stations_with_missing_attrs(missing.split(","))


if __name__ == "__main__":
    main()
