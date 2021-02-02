import json
import logging
import re
import subprocess
from json import JSONDecodeError
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypedDict

import click
import pandas as pd
import pdf2image

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

input_path = Path("inputs")
output_path = Path("outputs")

ErrorType = Dict[str, List[str]]


class Coordinates(TypedDict):
    raw_coordinates: Optional[str]
    lat: Optional[float]
    long: Optional[float]


class AirTemperature(TypedDict):
    raw_air_temp_noon: Optional[str]
    raw_air_temp_daily_mean: Optional[str]
    air_temp_noon: Optional[float]
    air_temp_daily_mean: Optional[float]


class WaterTemperature(TypedDict):
    raw_water_temp: Optional[str]
    water_temp_surface: Optional[float]
    water_temp_bottom: Optional[float]


class Density(TypedDict):
    raw_water_density: Optional[str]
    water_density_surface: Optional[float]
    water_density_bottom: Optional[float]


class Species(TypedDict):
    species_name: Optional[str]
    offset_start: Optional[int]
    offset_end: Optional[int]
    canonical_form: Optional[str]
    data_source_id: Optional[str]
    data_source_title: Optional[str]
    taxonId: Optional[str]
    classification_path: List[str]
    classification_path_rank: List[str]


class Station(
    TypedDict, Coordinates, AirTemperature, WaterTemperature, Density, Species
):
    station: Optional[str]
    part: Optional[int]
    start_page: Optional[int]
    start_page_line: Optional[int]
    end_page: Optional[int]
    end_page_line: Optional[int]
    raw_text: List[str]
    date: Optional[str]
    errors: ErrorType


def get_new_station(part: int) -> Station:
    return Station(
        station=None,
        part=part,
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
    pdfs = {
        "part1": "Challenger Summary part 1.pdf",
        "part2": "Challenger Summary part 2.pdf",
    }

    for name, pdf in pdfs.items():
        images_path = output_path / name / "pages" / "images"
        texts_path = output_path / name / "pages" / "texts"
        images_path.mkdir(parents=True, exist_ok=True)
        texts_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Processing {pdf}...")
        images = pdf2image.convert_from_path(input_path / pdf)
        logger.info(f"Converted {pdf} to image")

        for idx, image in enumerate(images):
            filename = f"{idx:08}"
            image.save(images_path / f"{filename}.png")
            logger.info(f"Saved {filename}.png")


def parse_data():
    """ Run info extraction techniques on file """
    parts = [1, 2]

    for part_number in parts:
        texts_path = output_path / f"part{part_number}" / "pages" / "texts"
        if not texts_path.exists():
            logger.warning(f"Could not find the text files for part {part_number}.")
            return

        logger.info(f"Processing part {part_number} stations...")
        stations = get_stations(part_number, texts_path)
        stations.to_csv(
            output_path / f"part{part_number}" / "stations" / "000_stations.csv"
        )

    # merge files from part 1 and part 2 summaries
    merge_files(parts)


def parse_stations(stations_str: str):
    parts = {"1": [], "2": []}
    for station in stations_str.split(","):
        (part_number, station_idx) = station.split("/")
        parts[part_number].append(f"{int(station_idx):03}")

    for part_number, stations in parts.items():
        for f in (output_path / f"part{part_number}" / "stations").iterdir():
            if f.name[:3] in stations:
                with open(f, "r") as station_file:
                    station_json: Station = json.loads(station_file.read())
                station_json["errors"] = {}
                get_environment_info(station_json)
                parse_species_names_gnrd(station_json)
                with open(f, "w") as station_file:
                    station_file.write(json.dumps(station_json, indent=2))


def merge_files(parts: List[int]):
    concatenated_output = []
    for part_number in parts:
        concatenated_output.append(
            pd.read_csv(
                output_path / f"part{part_number}" / "stations" / "000_stations.csv"
            )
        )
    pd.concat(concatenated_output).reset_index().to_csv(output_path / "stations.csv")


def get_stations(part_number: int, texts_path: Path) -> pd.DataFrame:
    """ Break summary texts into sections for each station, put in dictionary"""
    stations_output_path = texts_path.parent.parent / "stations"
    stations_output_path.mkdir(exist_ok=True)

    stations = []
    previous_station = get_new_station(part_number)

    station_index = -1
    station_text = []

    found_station = False
    i = 0
    text_file = None

    def add_station():
        previous_station["end_page"] = int(text_file.stem[4:])
        previous_station["end_page_line"] = i
        previous_station["raw_text"] = station_text
        get_environment_info(previous_station)
        parse_species_names_gnrd(previous_station)
        stations.append(previous_station)
        with open(
            stations_output_path
            / f"{station_index:03}_{previous_station['station']}.json",
            "w",
        ) as f:
            f.write(json.dumps(previous_station, indent=2))

    for text_file in texts_path.glob("*.txt"):
        with open(text_file, mode="r", encoding="utf-8") as fd:
            text = fd.read().strip()

        lines = text.split("\n")

        for i, line in enumerate(lines):
            line_cleaned = line.strip()

            # example: Station 16 (Sounding 60)
            if "(Sounding" in line_cleaned:
                station = (
                    line_cleaned[: line_cleaned.find("(Sounding")]
                    .strip()
                    .encode("ascii", errors="ignore")
                    .decode("utf8")
                )
                logger.info(f"Found {station}")
                if not found_station:
                    found_station = True
                    station_text.append(line_cleaned)
                station_index += 1

                if station_index > 0:
                    add_station()
                    previous_station = get_new_station(part_number)
                    station_text = [line_cleaned]
                previous_station["station"] = station
                previous_station["start_page"] = int(text_file.stem[4:])
                previous_station["start_page_line"] = i
            elif found_station:
                station_text.append(line_cleaned)

    if i and text_file:
        add_station()

    return pd.DataFrame(stations)


def parse_species_names_gnrd(station: Station):
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
    except JSONDecodeError:
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


def parse_coordinates(text: str) -> Tuple[Coordinates, ErrorType]:
    try:
        dms = re.search(
            r"lat[^\d]*"
            r"(?P<lat_degree>\d+(?:\.?\d+)?)[^\d]*"
            r"(?P<lat_minute>\d+(?:\.?\d+)?)?[^\d]*"
            r"(?P<lat_second>\d+(?:\.?\d+)?)?[^\d]*"
            r"(?P<lat_direction>[NS]).*"
            r"long[^\d]*"
            r"(?P<long_degree>\d+(?:\.?\d+)?)[^\d]*"
            r"(?P<long_minute>\d+(?:\.?\d+)?)?[^\d]*"
            r"(?P<long_second>\d+(?:\.?\d+)?)?[^\d]*"
            r"(?P<long_direction>[WE])",
            text,
        )
        if dms:
            matches = dms.groupdict()

            lat_degree = float(matches["lat_degree"] or 0)
            lat_minute = float(matches["lat_minute"] or 0)
            lat_second = float(matches["lat_second"] or 0)
            lat = lat_degree + lat_minute / 60 + lat_second / 3600
            if matches["lat_direction"] == "S":
                lat = -lat

            long_degree = float(matches["long_degree"] or 0)
            long_minute = float(matches["long_minute"] or 0)
            long_second = float(matches["long_second"] or 0)
            long = long_degree + long_minute / 60 + long_second / 3600
            if matches["long_direction"] == "W":
                long = -long

            return {"raw_coordinates": dms.group(), "lat": lat, "long": long}, {}
        return {}, {"coords": ["Could not find coordinates"]}
    except Exception as e:
        return {}, {"coords": [f"line: {e.__traceback__.tb_lineno}: {e}"]}


def parse_air_temperature(text: str) -> Tuple[AirTemperature, ErrorType]:
    results = {}
    errors = {}

    raw_air_temp_noon = re.search(
        r"air at noon[^\d]*(?P<integer>\d+)[^\d]*(?P<fraction>\d+)", text
    )
    if raw_air_temp_noon:
        results["raw_air_temp_noon"] = raw_air_temp_noon.group()
        air_temp_noon = raw_air_temp_noon.groupdict()
        results["air_temp_noon"] = float(
            f"{air_temp_noon['integer']}.{air_temp_noon['fraction']}"
        )
    else:
        errors["air_temp_noon"] = ["Could not parse air temperature at noon"]

    raw_air_temp_daily_mean = re.search(
        r"mean for the day[^\d]*(?P<integer>\d+)[^\d]*(?P<fraction>\d+)", text
    )
    if raw_air_temp_daily_mean:
        results["raw_air_temp_daily_mean"] = raw_air_temp_daily_mean.group()
        air_temp_daily_mean = raw_air_temp_daily_mean.groupdict()
        results["air_temp_daily_mean"] = float(
            f"{air_temp_daily_mean['integer']}.{air_temp_daily_mean['fraction']}"
        )
    else:
        errors["air_temp_daily_mean"] = ["Could not parse daily mean air temperature"]

    return results, errors


def parse_water_temperature(text: str) -> Tuple[WaterTemperature, ErrorType]:
    # TODO add support for parsing bottom water temperatures that are presented as a list. e.g.:
    #  Temperature of water :—
    #  Surface, . . . . 72'5 900 fathoms, . . . 39°8
    #  100 fathoms, . , . 66:5 1000 _—sé=éy«y . . . 39°3
    #  200_ Cs, . , ; 60°3 1100 _s=»“"»~ . . . 38°8
    #  300_—SC=é»; , , . 53°8 1200 __s=»“", . . . 38°3
    #  400_ ,, , . ~ 475 1300 _—sé=é“»"» . . . 37°9
    #  500 _—Ssé=»; ; . . 43°2 1400 _ _,, . . . 37°5
    #  600 _,, , , . 41°6 1500 _—=s=é»; : , . 71
    #  700_—=C=»y . . , 40°7 Bottom, . ; . , 36°2
    #  800 __—,, . . , 40°2

    results = {}
    errors = {}

    raw_water_temp = re.search(
        r"water at surface[^\d]*"
        r"(?P<surface_integer>\d+)[^\d]*"
        r"(?P<surface_fraction>\d+)"
        r"(?:"
        r"[^\d]*bottom[^\d]*"
        r"(?P<bottom_integer>\d+)[^\d]*"
        r"(?P<bottom_fraction>\d+)"
        r")?",
        text,
    )
    if raw_water_temp:
        results["raw_water_temp"] = raw_water_temp.group()
        water_temp = raw_water_temp.groupdict()

        results["water_temp_surface"] = float(
            f"{water_temp['surface_integer']}.{water_temp['surface_fraction']}"
        )

        water_temp_bottom = ""
        if water_temp["bottom_integer"]:
            water_temp_bottom = water_temp["bottom_integer"]
        if water_temp["bottom_fraction"]:
            water_temp_bottom = f"{water_temp_bottom}.{water_temp['bottom_fraction']}"

        if water_temp_bottom:
            results["water_temp_bottom"] = float(water_temp_bottom)
        else:
            errors["water_temp_bottom"] = ["Could not parse bottom water temperature"]
    else:
        errors["water_temp"] = ["Could not parse water temperature"]

    return results, errors


def parse_density(text: str) -> Tuple[Density, ErrorType]:
    # TODO add support for parsing bottom water densities that are presented as a list. e.g.:
    #  need to fix if in the form:
    #  Density at 60° F. :—
    #  Surface, . . . 1:02739 400 fathoms, . . 102640
    #  100 fathoms, ; 102782 500 - , . 102612
    #  200 , =. . 1:02708 Bottom, . , ; 102607
    #  300 , ~~. , 1:02672

    results = {}
    errors = {}

    raw_water_density = re.search(
        r"Density.*?at surface[^\d]*"
        r"(?P<surface_integer>\d+)[^\d]*"
        r"(?P<surface_fraction>\d+)"
        r"(?:.*?"
        r"bottom[^\d]*"
        r"(?P<bottom_integer>\d+)[^\d]*"
        r"(?P<bottom_fraction>\d+)"
        r")?",
        text,
    )

    if raw_water_density:
        results["raw_water_density"] = raw_water_density.group()
        water_density = raw_water_density.groupdict()

        results["water_density_surface"] = float(
            f"{water_density['surface_integer']}.{water_density['surface_fraction']}"
        )

        water_density_bottom = ""
        if water_density["bottom_integer"]:
            water_density_bottom = water_density["bottom_integer"]
        if water_density["bottom_fraction"]:
            water_density_bottom = (
                f"{water_density_bottom}.{water_density['bottom_fraction']}"
            )

        if water_density_bottom:
            results["water_density_bottom"] = float(water_density_bottom)
        else:
            errors["water_density_bottom"] = ["Could not parse bottom water density"]
    else:
        errors["water_density"] = ["Could not parse water density"]

    return results, errors


def parse_date(text: str) -> Tuple[Optional[str], ErrorType]:
    try:
        return re.search(r"\w+ \d?\d, ?\d{4}", text).group(), {}
    except Exception as e:
        return None, {"date": [f"line: {e.__traceback__.tb_lineno}: {e}"]}


def get_environment_info(station: Station):
    text = "\n".join(station["raw_text"])

    (date, date_errors) = parse_date(text)
    station["date"] = date
    station["errors"].update(date_errors)

    (coords, coords_errors) = parse_coordinates(text)
    station.update(coords)
    station["errors"].update(coords_errors)

    (air_temperature, air_temperature_errors) = parse_air_temperature(text)
    station.update(air_temperature)
    station["errors"].update(air_temperature_errors)

    (water_temperature, water_temperature_errors) = parse_water_temperature(text)
    station.update(water_temperature)
    station["errors"].update(water_temperature_errors)

    (density, density_errors) = parse_density(text)
    station.update(density)
    station["errors"].update(density_errors)


def get_stations_with_errors(errors: List[str]):
    any_error = "any" in errors
    for part in ["1", "2"]:
        for file_name in (output_path / f"part{part}" / "stations").iterdir():
            if file_name.suffix == ".json":
                with open(file_name, "r") as f:
                    station_errors = json.loads(f.read())["errors"]
                    if (any_error and station_errors) or any(
                        [error in station_errors for error in errors]
                    ):
                        print(file_name)


def get_stations_with_missing_attrs(keys: List[str]):
    for part in ["1", "2"]:
        for file_name in (output_path / f"part{part}" / "stations").iterdir():
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
    Parse the given stations and updates their json and entries in CSVs.\n
    Accepts a comma-separated list of stations in the following format:\n
    <part_number>/<station_index>\n
    `station_index` is the number at the beginning of a station json file in `outputs/<part_number>/stations/`.
    You can leave out the leading zeros for index.\n
    \b
    Example: --stations 1/12,1/13,2/115 (parses stations 12 and 13 of part 1 and station 115 of part 2).
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
    if stations:
        parse_stations(stations)
    if errors:
        get_stations_with_errors(errors.split(","))
    if missing:
        get_stations_with_missing_attrs(missing.split(","))


if __name__ == "__main__":
    main()
