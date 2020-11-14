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
import pytesseract

logger = logging.getLogger(__name__)

input_path = Path("inputs")
output_path = Path("outputs")


ErrorType = Dict[str, List[str]]


class Coordinates(TypedDict):
    lat_degree: Optional[int]
    lat_minute: Optional[int]
    lat_second: Optional[int]
    lat_coord: Optional[float]
    long_degree: Optional[int]
    long_minute: Optional[int]
    long_second: Optional[int]
    long_coord: Optional[float]


class AirTemperature(TypedDict):
    raw_air_temp_noon: Optional[str]
    raw_air_temp_daily_mean: Optional[str]
    air_temp_noon: Optional[float]
    air_temp_daily_mean: Optional[float]


class WaterTemperature(TypedDict):
    raw_water_temp_surface: Optional[str]
    raw_water_temp_bottom: Optional[str]
    water_temp_surface: Optional[float]
    water_temp_bottom: Optional[float]


class Density(TypedDict):
    raw_water_density_surface: Optional[str]
    raw_water_density_bottom: Optional[str]
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
    raw_dms_coords: Optional[str]
    line_number_of_date: Optional[int]
    line_number_of_lat_long: Optional[int]
    line_number_air_temp_noon: Optional[int]
    line_number_of_air_temp_daily_mean: Optional[int]
    line_number_of_water_temp_surface: Optional[int]
    line_number_of_water_temp_bottom: Optional[int]
    line_number_of_water_density_surface: Optional[int]
    line_number_of_water_density_bottom: Optional[int]
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
        raw_dms_coords=None,
        lat_degree=None,
        lat_minute=None,
        lat_second=None,
        lat_coord=None,
        long_degree=None,
        long_minute=None,
        long_second=None,
        long_coord=None,
        raw_air_temp_noon=None,
        raw_air_temp_daily_mean=None,
        air_temp_daily_mean=None,
        air_temp_noon=None,
        raw_water_temp_surface=None,
        water_temp_bottom=None,
        water_temp_surface=None,
        raw_water_temp_bottom=None,
        raw_water_density_bottom=None,
        raw_water_density_surface=None,
        water_density_bottom=None,
        water_density_surface=None,
        line_number_of_date=None,
        line_number_of_lat_long=None,
        line_number_air_temp_noon=None,
        line_number_of_air_temp_daily_mean=None,
        line_number_of_water_temp_surface=None,
        line_number_of_water_temp_bottom=None,
        line_number_of_water_density_bottom=None,
        line_number_of_water_density_surface=None,
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
            filename = f"{idx+1:04}"
            image.save(images_path / f"page{filename}.png")
            logger.info(f"Saved {filename}.png")
            text = pytesseract.image_to_string(image)
            with open(texts_path / f"page{filename}.txt", "w", encoding="utf-8") as f:
                f.write(text)
            logger.info(f"Saved {filename}.txt")


def parse_data():
    """ Run info extraction techniques on file """
    parts = [1, 2]

    for part_number in parts:
        texts_path = output_path / f"part{part_number}" / "pages" / "texts"
        if not texts_path.exists():
            logger.warning(
                f"Could not find the text files for part {part_number}. "
                f"You need to include the `--pdf` flag to convert the PDF files first."
            )
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
            f.write(json.dumps(previous_station))

    for text_file in texts_path.iterdir():
        with open(text_file, mode="r", encoding="utf-8") as fd:
            text = fd.read().strip()

        lines = text.split("\n")

        for i, line in enumerate(lines):
            line_cleaned = line.strip()

            # example: Station 16 (Sounding 60)
            # for first station through station before last station
            if "(Sounding" in line_cleaned:
                station = (
                    line_cleaned[: line_cleaned.find("(Sounding")]
                    .strip()
                    .encode("ascii", errors="ignore")
                    .decode("utf8")
                )
                logger.info(f"Found {station}!")
                found_station = True
                station_index += 1

                if station_index > 0:
                    add_station()
                    previous_station = get_new_station(part_number)
                    station_text = []
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
        logger.error("No json was returned by the server - skipping station!")
        pass


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


def dms_to_lat_long(degree: float, minute: float, second: float) -> float:
    if not second and not minute:
        return degree
    elif not second and minute:
        return degree + minute / 60
    else:
        return degree + minute / 60 + second / 3600


def parse_coordinate(
    coordinate: str,
) -> Tuple[
    Optional[float], Optional[float], Optional[float], Optional[float], List[str]
]:
    errors = []

    try:
        degree = coordinate.split("°")[0]
        degree = float(re.sub(r"[^0-9]", "", degree).strip())
    except Exception as e:
        degree = None
        errors.append(
            f"Parse coordinates degree (line: {e.__traceback__.tb_lineno}: {e})"
        )

    if "’" in coordinate.split("°")[1]:
        try:
            minute = float(coordinate.split("°")[1].split("’")[0].strip())
        except Exception as e:
            minute = None
            errors.append(
                f"Parse coordinates minute (line: {e.__traceback__.tb_lineno}: {e})"
            )

        try:  # none of these so far, but just in case
            second = coordinate.split("°")[1].split("’")[1]
            second = float(re.sub("[^0-9]", "", second).strip())
        except Exception as e:
            second = None
            errors.append(
                f"Parse coordinates second (line: {e.__traceback__.tb_lineno}: {e})"
            )
    elif "'" in coordinate.split("°")[1]:
        try:
            minute = float(coordinate.split("°")[1].split("'")[0].strip())
        except Exception as e:
            minute = None
            errors.append(
                f"Parse coordinates minute (line: {e.__traceback__.tb_lineno}: {e})"
            )
        try:  # none of these so far, but just in case
            second = coordinate.split("°")[1].split("'")[1]
            second = float(re.sub("[^0-9]", "", second).strip())
        except Exception as e:
            second = None
            errors.append(
                f"Parse coordinates second (line: {e.__traceback__.tb_lineno}: {e})"
            )
    else:
        logger.warning(f"Could not find coords: {coordinate}")
        minute = None
        second = None

    lat_long = dms_to_lat_long(degree, minute, second) if degree else None

    return (
        lat_long,
        degree,
        minute,
        second,
        errors if not lat_long else [],
    )


def parse_coordinates(line: str) -> Tuple[Coordinates, ErrorType]:
    errors = {}

    dms_coords = line.split(";")[1]

    lat, long = dms_coords.split(",")

    (lat_coord, lat_degree, lat_minute, lat_second, lat_errors) = parse_coordinate(lat)
    (long_coord, long_degree, long_minute, long_second, long_errors) = parse_coordinate(
        long
    )

    if lat_errors:
        errors["lat"] = lat_errors
    if long_errors:
        errors["long"] = long_errors

    return (
        {
            "lat_degree": lat_degree,
            "lat_minute": lat_minute,
            "lat_second": lat_second,
            "lat_coord": lat_coord,
            "long_degree": long_degree,
            "long_minute": long_minute,
            "long_second": long_second,
            "long_coord": long_coord,
        },
        errors,
    )


def parse_air_temperature(line: str) -> Tuple[AirTemperature, ErrorType]:
    # fix for line 33964 of part 1 summary
    # 5.45 p.M. made sail and proceeded towards the Crozet Islands. Temperature of air at
    errors = {}

    if ";" in line:
        raw_air_temp_noon = line.split(";")[0].strip()
        air_temp_noon = raw_air_temp_noon.split(",")[1]
        air_temp_noon = re.sub("[^0-9]", "", air_temp_noon)
        air_temp_noon = float((air_temp_noon[0:-1] + "." + air_temp_noon[-1]).strip())

        try:
            raw_air_temp_daily_mean = line.split(";")[1].strip()
            air_temp_daily_mean = raw_air_temp_daily_mean.split(",")[1]
            try:
                air_temp_daily_mean = air_temp_daily_mean.split(".")[0]
                air_temp_daily_mean = re.sub("[^0-9]", "", air_temp_daily_mean)
                air_temp_daily_mean = float(
                    (air_temp_daily_mean[0:-1] + "." + air_temp_daily_mean[-1]).strip()
                )

            except Exception as e:
                logger.error(f"Parse air temperature: {e}")
                air_temp_daily_mean = re.sub("[^0-9]", "", air_temp_daily_mean)
                air_temp_daily_mean = float(
                    (air_temp_daily_mean[0:-1] + "." + air_temp_daily_mean[-1]).strip()
                )

        except Exception as e:
            logger.error(f"Parse air temperature: {e}")
            raw_air_temp_daily_mean = None
            air_temp_daily_mean = None
            errors["raw_air_temp_daily_mean"] = [str(e)]
    else:
        raw_air_temp_noon = line.strip()
        air_temp_noon = raw_air_temp_noon.split(",")[1]
        air_temp_noon = re.sub("[^0-9]", "", air_temp_noon)
        air_temp_noon = float((air_temp_noon[0:-1] + "." + air_temp_noon[-1]).strip())
        raw_air_temp_daily_mean = None
        air_temp_daily_mean = None

    return (
        {
            "raw_air_temp_noon": raw_air_temp_noon,
            "raw_air_temp_daily_mean": raw_air_temp_daily_mean,
            "air_temp_noon": air_temp_noon,
            "air_temp_daily_mean": air_temp_daily_mean,
        },
        errors,
    )


def parse_water_temperature(line: str) -> Tuple[WaterTemperature, ErrorType]:
    errors = {}

    if ";" in line:
        raw_water_temp_surface = line.split(";")[0].strip()
        try:
            water_temp_surface = raw_water_temp_surface.split(",")[1]
            water_temp_surface = re.sub("[^0-9]", "", water_temp_surface)
            water_temp_surface = float(
                (water_temp_surface[0:-1] + "." + water_temp_surface[-1]).strip()
            )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            # currentWaterTempSurfaceDegree = ""
            water_temp_surface = re.sub("[^0-9]", "", raw_water_temp_surface)
            water_temp_surface = float(
                (water_temp_surface[0:-1] + "." + water_temp_surface[-1]).strip()
            )

        try:
            raw_water_temp_bottom = line.split(";")[1].strip()
            water_temp_bottom = raw_water_temp_bottom.split(",")[1]
            try:
                water_temp_bottom = water_temp_bottom.split(".")[0]
                water_temp_bottom = re.sub("[^0-9]", "", water_temp_bottom)
                water_temp_bottom = float(
                    (water_temp_bottom[0:-1] + "." + water_temp_bottom[-1]).strip()
                )

            except Exception as e:
                logger.error(f"Parse water temperature: {e}")
                water_temp_bottom = water_temp_bottom
                water_temp_bottom = re.sub("[^0-9]", "", water_temp_bottom)
                water_temp_bottom = float(
                    (water_temp_surface[0:-1] + "." + water_temp_bottom[-1]).strip()
                )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            raw_water_temp_bottom = None
            water_temp_bottom = None
    else:
        raw_water_temp_surface = line.strip()
        try:
            water_temp_surface = raw_water_temp_surface.split(",")[1]
            water_temp_surface = re.sub("[^0-9]", "", water_temp_surface)
            water_temp_surface = float(
                (water_temp_surface[0:-1] + "." + water_temp_surface[-1]).strip()
            )
        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            water_temp_surface = None
            errors["water_temp_surface"] = [str(e)]

        raw_water_temp_bottom = None
        water_temp_bottom = None
        # need to fix if in the form:
        # Temperature of water :—
        #
        # Surface, . . . . 72'5 900 fathoms, . . . 39°8
        # 100 fathoms, . , . 66:5 1000 _—sé=éy«y . . . 39°3
        # 200_ Cs, . , ; 60°3 1100 _s=»“"»~ . . . 38°8
        # 300_—SC=é»; , , . 53°8 1200 __s=»“", . . . 38°3
        # 400_ ,, , . ~ 475 1300 _—sé=é“»"» . . . 37°9
        # 500 _—Ssé=»; ; . . 43°2 1400 _ _,, . . . 37°5
        # 600 _,, , , . 41°6 1500 _—=s=é»; : , . 71
        # 700_—=C=»y . . , 40°7 Bottom, . ; . , 36°2
        # 800 __—,, . . , 40°2

    return (
        {
            "raw_water_temp_surface": raw_water_temp_surface,
            "water_temp_surface": water_temp_surface,
            "raw_water_temp_bottom": raw_water_temp_bottom,
            "water_temp_bottom": water_temp_bottom,
        },
        errors,
    )


def parse_density(line: str) -> Tuple[Density, ErrorType]:
    errors = {}

    if ";" in line:
        raw_water_density_surface = line.split(";")[0].strip()
        water_density_surface = raw_water_density_surface.split(",")[1]
        water_density_surface = re.sub("[^0-9]", "", water_density_surface)
        water_density_surface = float(
            (water_density_surface[0] + "." + water_density_surface[1:]).strip()
        )

        raw_water_density_bottom = line.split(";")[1].strip()
        water_density_bottom = raw_water_density_bottom.split(",")[1].strip()
        try:
            water_density_bottom = water_density_bottom.split(".")[0]
            water_density_bottom = re.sub("[^0-9]", "", water_density_bottom)
            water_density_bottom = float(
                (water_density_bottom[0] + "." + water_density_bottom[1:]).strip()
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            water_density_bottom = re.sub("[^0-9]", "", water_density_bottom)
            water_density_bottom = float(
                (water_density_bottom[0] + "." + water_density_bottom[1:]).strip()
            )

    else:
        try:
            raw_water_density_surface = line.strip()
            water_density_surface = raw_water_density_surface.split(",")[1]
            water_density_surface = re.sub("[^0-9]", "", water_density_surface)
            water_density_surface = float(
                (water_density_surface[0] + "." + water_density_surface[1:]).strip()
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            raw_water_density_surface = None
            water_density_surface = None
            errors["water_density"] = [str(e)]

        raw_water_density_bottom = None
        water_density_bottom = None
        # need to fix if in the form:
        # Density at 60° F. :—

        # Surface, . . . 1:02739 400 fathoms, . . 102640
        # 100 fathoms, ; 102782 500 - , . 102612
        # 200 , =. . 1:02708 Bottom, . , ; 102607
        # 300 , ~~. , 1:02672

    return (
        {
            "raw_water_density_surface": raw_water_density_surface,
            "water_density_surface": water_density_surface,
            "raw_water_density_bottom": raw_water_density_bottom,
            "water_density_bottom": water_density_bottom,
        },
        errors,
    )


def get_environment_info(station: Station):
    first_lat_long = True
    first_air_temp = True
    first_water_temp = True
    first_density = True

    for idx, line in enumerate(station["raw_text"]):
        line_number = idx + 1
        if "lat." in line and "long." in line and first_lat_long:
            if ", 18" in line or ",18" in line:  # 18 for 1876, 1877...
                try:
                    first_lat_long = False

                    (coordinates, coordinates_errors) = parse_coordinates(line)
                    station.update(coordinates)
                    station["errors"].update(coordinates_errors)
                    station.update(
                        {
                            "date": line.split(";")[0].strip(),
                            "raw_dms_coords": line.split(";")[1].strip(),
                            "line_number_of_date": line_number,
                            "line_number_of_lat_long": line_number,
                        }
                    )
                except Exception as e:
                    logger.error(e)
                    station.update({"date": None, "raw_dms_coords": None})
                    station["errors"].update(
                        {"date": [str(e)], "raw_dms_coords": [str(e)]}
                    )
            else:
                lat_index = line.find("lat")
                station.update(
                    {"date": None, "raw_dms_coords": line[lat_index:].strip()}
                )

        if "Temperature of air" in line and first_air_temp:
            first_air_temp = False

            (air_temperature, air_temperature_errors) = parse_air_temperature(line)
            station.update(air_temperature)
            station["errors"].update(air_temperature_errors)
            station.update(
                {
                    "line_number_air_temp_noon": line_number,
                    "line_number_of_air_temp_daily_mean": line_number,
                }
            )

        if "Temperature of water" in line and first_water_temp:
            first_water_temp = False

            (water_temperature, water_temperature_errors) = parse_water_temperature(
                line
            )
            station.update(water_temperature)
            station["errors"].update(water_temperature_errors)
            station.update(
                {
                    "line_number_of_water_temp_surface": line_number,
                    "line_number_of_water_temp_bottom": line_number,
                }
            )

        if "Density" in line and first_density:
            first_density = False

            (density, density_errors) = parse_density(line)
            station.update(density)
            station["errors"].update(density_errors)
            station.update(
                {
                    "line_number_of_water_density_surface": line_number,
                    "line_number_of_water_density_bottom": line_number,
                }
            )


@click.command()
@click.option("--pdf", is_flag=True, help="Convert PDFs to text.")
@click.option("--parse", is_flag=True, help="Parse data from converted text.")
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
def main(pdf: bool, parse: bool, stations: Optional[str]):
    if not pdf and not parse and not stations:
        with click.Context(main) as ctx:
            click.echo(main.get_help(ctx))
    if pdf:
        process_pdf()
    if parse:
        parse_data()
    if stations:
        parse_stations(stations)


if __name__ == "__main__":
    main()
