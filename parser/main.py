import json
import logging
import re
import subprocess
from json import JSONDecodeError
from pathlib import Path
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypedDict
from typing import Union

import pandas as pd

logger = logging.getLogger(__name__)

input_path = Path("inputs")
output_path = Path("outputs")


class Coordinates(TypedDict):
    current_lat_degree: str
    current_lat_minute: str
    current_lat_second: str
    current_lat_coord: float
    current_long_degree: str
    current_long_minute: str
    current_long_second: str
    current_long_coord: float


class AirTemperature(TypedDict):
    current_air_temp_noon: str
    current_air_temp_noon_degree: str
    current_air_temp_daily_mean: str
    current_air_temp_daily_mean_degree: str


class WaterTemperature(TypedDict):
    current_water_temp_surface: str
    current_water_temp_surface_degree: str
    current_water_temp_bottom: str
    current_water_temp_bottom_degree: str


class Density(TypedDict):
    current_water_density_surface: str
    current_water_density_surface_number: str
    current_water_density_bottom: str
    current_water_density_bottom_number: str


EnvironmentAttributes = Union[Coordinates, AirTemperature, WaterTemperature, Density]


def main():
    """ Run info extraction techniques on file """
    file_name_list = ["part1OCR.txt", "part2OCR.txt"]

    for filename in file_name_list:
        file_output_path = output_path / filename[:-4]
        file_output_path.mkdir(parents=True, exist_ok=True)

        df_station_lines, station_text_dict = split_text_into_stations(filename)
        df_station_lines.to_csv(file_output_path / "station_lines.csv")
        get_environment_info(station_text_dict).to_csv(
            file_output_path / "station_environment_info.csv", encoding="utf-8-sig",
        )

        df_parsed = parse_species_names_gnrd(station_text_dict)
        df_parsed.to_csv(
            file_output_path / "parsed_species_names_with_station_and_offset.csv"
        )

    # merge files from part 1 and part 2 summaries
    merge_files(file_name_list)


def merge_files(file_name_list):
    for output in [
        "station_lines.csv",
        "verified_species_names.csv",
        "station_environment_info.csv",
    ]:
        concatenated_output = []
        for file_name in file_name_list:
            concatenated_output.append(
                pd.read_csv(output_path / file_name[:-4] / output)
            )
        pd.concat(concatenated_output).reset_index().to_csv(output_path / output)


def split_text_into_stations(filename: str) -> (pd.DataFrame, Dict[str, List[str]]):
    """ Break summary texts into sections for each station, put in dictionary"""
    logger.info(f"Splitting {filename} into stations...")

    f = input_path / filename

    with open(f, mode="r", encoding="utf-8") as fd:
        text = fd.read().strip()

    lines = text.split("\n")
    length_of_file = len(lines)

    df_station_lines = pd.DataFrame(columns=["station", "start_line", "end_line"])
    station_text_dict = {}
    found_station = False
    previous_station = None
    current_station_index = -1
    current_station_text = []

    for i, line in enumerate(lines):
        logger.info(f"{100 * round(i / length_of_file, 2)}% Line {i}/{length_of_file}")
        line_cleaned = line.strip()

        # example: Station 16 (Sounding 60)
        # for first station through station before last station
        if "(Sounding" in line_cleaned:
            station = line_cleaned[: line_cleaned.find("(Sounding")].strip()
            logger.info(f"Found {station}!")
            found_station = True
            current_station_index += 1

            if current_station_index > 0:
                previous_station.append(i)
                df_station_lines.loc[current_station_index - 1] = previous_station
                station_text_dict[
                    df_station_lines.loc[current_station_index - 1]["station"]
                ] = current_station_text
                current_station_text = []
            previous_station = [station, i]
        elif found_station:
            current_station_text.append(line_cleaned)

    previous_station.append(length_of_file)
    df_station_lines.loc[current_station_index] = previous_station
    station_text_dict[
        df_station_lines.loc[current_station_index]["station"]
    ] = current_station_text

    return df_station_lines, station_text_dict


def parse_species_names_gnrd(station_text_dict: Dict[str, List[str]]) -> pd.DataFrame:
    columns_list = ["station", "species_name", "offset_start", "offset_end"]
    df = pd.DataFrame(columns=columns_list)

    for station_name, station_text in list(station_text_dict.items())[:2]:
        gnfinder = subprocess.run(
            ["gnfinder", "find", "-c", "-l", "eng"],
            stdout=subprocess.PIPE,
            input="\n".join(station_text),
            encoding="utf-8",
        )

        try:
            for name_dict in json.loads(gnfinder.stdout)["names"]:
                best_results = name_dict["verification"]["bestResult"]

                df = df.append(
                    {
                        "station": station_name,
                        "species_name": name_dict["name"],
                        "offset_start": name_dict["start"],
                        "offset_end": name_dict["end"],
                        "canonical_form": best_results.get("matchedCanonicalFull"),
                        "data_source_id": best_results.get("dataSourceId"),
                        "data_source_title": best_results.get("dataSourceTitle"),
                        "taxonId": best_results.get("taxonId"),
                        "classification_path": best_results.get("classificationPath"),
                        "classification_path_rank": best_results.get(
                            "classificationRank"
                        ),
                    },
                    ignore_index=True,
                )
        except JSONDecodeError:
            logger.error("No json was returned by the server - skipping station!")
            pass

    return df


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


def dms_to_lat_long(degree, minute, second) -> float:
    if second == "" and minute == "":
        return float(degree)
    elif second == "" and minute != "":
        return float(degree) + float(minute) / 60
    else:
        return float(degree) + float(minute) / 60 + float(second) / 3600


def parse_coordinate(coordinate: str) -> Tuple[float, str, str, str]:
    try:
        degree = coordinate.split("°")[0]
        degree = re.sub(r"[^0-9]", "", degree)
    except Exception as e:
        logger.error(f"Parse coordinates: {e}")
        degree = f"{e}, {coordinate}"

    if "’" in coordinate.split("°")[1]:
        try:
            minute = coordinate.split("°")[1].split("’")[0]
        except Exception as e:
            logger.error(f"Parse coordinates: {e}")
            minute = ""

        try:  # none of these so far, but just in case
            second = coordinate.split("°")[1].split("’")[1]
            second = re.sub("[^0-9]", "", second)
        except Exception as e:
            logger.error(f"Parse coordinates: {e}")
            second = ""
    elif "'" in coordinate.split("°")[1]:
        try:
            minute = coordinate.split("°")[1].split("'")[0]
        except Exception as e:
            logger.error(f"Parse coordinates: {e}")
            minute = ""
        try:  # none of these so far, but just in case
            second = coordinate.split("°")[1].split("'")[1]
            second = re.sub("[^0-9]", "", second)
        except Exception as e:
            logger.error(f"Parse coordinates: {e}")
            second = ""
    else:
        logger.warning(f"Could not find coords: {coordinate}")
        minute = ""
        second = ""

    return dms_to_lat_long(degree, minute, second), degree, minute, second


def parse_coordinates(line: str) -> Coordinates:
    current_dms_coords = line.split(";")[1]

    lat, long = current_dms_coords.split(",")

    (
        current_lat_coord,
        current_lat_degree,
        current_lat_minute,
        current_lat_second,
    ) = parse_coordinate(lat)
    (
        current_long_coord,
        current_long_degree,
        current_long_minute,
        current_long_second,
    ) = parse_coordinate(long)

    return {
        "current_lat_degree": current_lat_degree,
        "current_lat_minute": current_lat_minute,
        "current_lat_second": current_lat_second,
        "current_lat_coord": current_lat_coord,
        "current_long_degree": current_long_degree,
        "current_long_minute": current_long_minute,
        "current_long_second": current_long_second,
        "current_long_coord": current_long_coord,
    }


def parse_air_temperature(line: str) -> AirTemperature:
    # fix for line 33964 of part 1 summary
    # 5.45 p.M. made sail and proceeded towards the Crozet Islands. Temperature of air at
    if ";" in line:
        current_air_temp_noon = line.split(";")[0]
        current_air_temp_noon_degree = current_air_temp_noon.split(",")[1]
        current_air_temp_noon_degree = re.sub(
            "[^0-9]", "", current_air_temp_noon_degree
        )
        current_air_temp_noon_degree = (
            current_air_temp_noon_degree[0:-1] + "." + current_air_temp_noon_degree[-1]
        )

        try:
            current_air_temp_daily_mean = line.split(";")[1]
            current_air_temp_daily_mean_degree = current_air_temp_daily_mean.split(",")[
                1
            ]
            try:
                current_air_temp_daily_mean_degree = current_air_temp_daily_mean_degree.split(
                    "."
                )[
                    0
                ]
                current_air_temp_daily_mean_degree = re.sub(
                    "[^0-9]", "", current_air_temp_daily_mean_degree
                )
                current_air_temp_daily_mean_degree = (
                    current_air_temp_daily_mean_degree[0:-1]
                    + "."
                    + current_air_temp_daily_mean_degree[-1]
                )

            except Exception as e:
                logger.error(f"Parse air temperature: {e}")
                current_air_temp_daily_mean_degree = re.sub(
                    "[^0-9]", "", current_air_temp_daily_mean_degree
                )
                current_air_temp_daily_mean_degree = (
                    current_air_temp_daily_mean_degree[0:-1]
                    + "."
                    + current_air_temp_daily_mean_degree[-1]
                )

        except Exception as e:
            logger.error(f"Parse air temperature: {e}")
            current_air_temp_daily_mean = ""
            current_air_temp_daily_mean_degree = ""
    else:
        current_air_temp_noon = line
        current_air_temp_noon_degree = current_air_temp_noon.split(",")[1]
        current_air_temp_noon_degree = re.sub(
            "[^0-9]", "", current_air_temp_noon_degree
        )
        current_air_temp_noon_degree = (
            current_air_temp_noon_degree[0:-1] + "." + current_air_temp_noon_degree[-1]
        )
        current_air_temp_daily_mean = ""
        current_air_temp_daily_mean_degree = ""

    return {
        "current_air_temp_noon": current_air_temp_noon,
        "current_air_temp_noon_degree": current_air_temp_noon_degree,
        "current_air_temp_daily_mean": current_air_temp_daily_mean,
        "current_air_temp_daily_mean_degree": current_air_temp_daily_mean_degree,
    }


def parse_water_temperature(line: str) -> WaterTemperature:
    if ";" in line:
        current_water_temp_surface = line.split(";")[0]
        try:
            current_water_temp_surface_degree = current_water_temp_surface.split(",")[1]
            current_water_temp_surface_degree = re.sub(
                "[^0-9]", "", current_water_temp_surface_degree
            )
            current_water_temp_surface_degree = (
                current_water_temp_surface_degree[0:-1]
                + "."
                + current_water_temp_surface_degree[-1]
            )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            # currentWaterTempSurfaceDegree = ""
            current_water_temp_surface_degree = re.sub(
                "[^0-9]", "", current_water_temp_surface
            )
            current_water_temp_surface_degree = (
                current_water_temp_surface_degree[0:-1]
                + "."
                + current_water_temp_surface_degree[-1]
            )

        try:
            current_water_temp_bottom = line.split(";")[1]
            current_water_temp_bottom_degree = current_water_temp_bottom.split(",")[1]
            try:
                current_water_temp_bottom_degree = current_water_temp_bottom_degree.split(
                    "."
                )[
                    0
                ]
                current_water_temp_bottom_degree = re.sub(
                    "[^0-9]", "", current_water_temp_bottom_degree
                )
                current_water_temp_bottom_degree = (
                    current_water_temp_bottom_degree[0:-1]
                    + "."
                    + current_water_temp_bottom_degree[-1]
                )

            except Exception as e:
                logger.error(f"Parse water temperature: {e}")
                current_water_temp_bottom_degree = current_water_temp_bottom_degree
                current_water_temp_bottom_degree = re.sub(
                    "[^0-9]", "", current_water_temp_bottom_degree
                )
                current_water_temp_bottom_degree = (
                    current_water_temp_surface_degree[0:-1]
                    + "."
                    + current_water_temp_bottom_degree[-1]
                )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            current_water_temp_bottom = ""
    else:
        current_water_temp_surface = line
        try:
            current_water_temp_surface_degree = current_water_temp_surface.split(",")[1]
            current_water_temp_surface_degree = re.sub(
                "[^0-9]", "", current_water_temp_surface_degree
            )
            current_water_temp_surface_degree = (
                current_water_temp_surface_degree[0:-1]
                + "."
                + current_water_temp_surface_degree[-1]
            )
        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            current_water_temp_surface_degree = ""

        current_water_temp_bottom = ""
        current_water_temp_bottom_degree = ""
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

    return {
        "current_water_temp_surface": current_water_temp_surface,
        "current_water_temp_surface_degree": current_water_temp_surface_degree,
        "current_water_temp_bottom": current_water_temp_bottom,
        "current_water_temp_bottom_degree": current_water_temp_bottom_degree,
    }


def parse_density(line: str) -> Density:
    if ";" in line:
        current_water_density_surface = line.split(";")[0]
        current_water_density_surface_number = current_water_density_surface.split(",")[
            1
        ]
        current_water_density_surface_number = re.sub(
            "[^0-9]", "", current_water_density_surface_number
        )
        current_water_density_surface_number = (
            current_water_density_surface_number[0]
            + "."
            + current_water_density_surface_number[1:]
        )

        current_water_density_bottom = line.split(";")[1]
        current_water_density_bottom_number = current_water_density_bottom.split(",")[1]
        try:
            current_water_density_bottom_number = current_water_density_bottom_number.split(
                "."
            )[
                0
            ]
            current_water_density_bottom_number = re.sub(
                "[^0-9]", "", current_water_density_bottom_number
            )
            current_water_density_bottom_number = (
                current_water_density_bottom_number[0]
                + "."
                + current_water_density_bottom_number[1:]
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            current_water_density_bottom_number = re.sub(
                "[^0-9]", "", current_water_density_bottom_number
            )
            current_water_density_bottom_number = (
                current_water_density_bottom_number[0]
                + "."
                + current_water_density_bottom_number[1:]
            )

    else:
        try:
            current_water_density_surface = line
            current_water_density_surface_number = current_water_density_surface.split(
                ","
            )[1]
            current_water_density_surface_number = re.sub(
                "[^0-9]", "", current_water_density_surface_number
            )
            current_water_density_surface_number = (
                current_water_density_surface_number[0]
                + "."
                + current_water_density_surface_number[1:]
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            current_water_density_surface = ""
            current_water_density_surface_number = ""

        current_water_density_bottom = ""
        current_water_density_bottom_number = ""
        # need to fix if in the form:
        # Density at 60° F. :—

        # Surface, . . . 1:02739 400 fathoms, . . 102640
        # 100 fathoms, ; 102782 500 - , . 102612
        # 200 , =. . 1:02708 Bottom, . , ; 102607
        # 300 , ~~. , 1:02672

    return {
        "current_water_density_surface": current_water_density_surface,
        "current_water_density_surface_number": current_water_density_surface_number,
        "current_water_density_bottom": current_water_density_bottom,
        "current_water_density_bottom_number": current_water_density_bottom_number,
    }


def get_environment_info(station_text_dict: Dict[str, List[str]]) -> pd.DataFrame:
    """ Takes in text file and parses for environment information """
    column_names = [
        "current_station",
        "current_date",
        "current_dms_coords",
        "current_lat_degree",
        "current_lat_minute",
        "current_lat_second",
        "current_lat_coord",
        "current_long_degree",
        "current_long_minute",
        "current_long_second",
        "current_long_coord",
        "current_air_temp_noon",
        "current_air_temp_noon_degree",
        "current_air_temp_daily_mean",
        "current_air_temp_daily_mean_degree",
        "current_water_temp_surface",
        "current_water_temp_surface_degree",
        "current_water_temp_bottom",
        "current_water_temp_bottom_degree",
        "current_water_density_surface",
        "current_water_density_surface_number",
        "current_water_density_bottom",
        "current_water_density_bottom_number",
        "line_number_of_date",
        "line_number_of_lat_long",
        "line_number_air_temp_noon",
        "line_number_of_air_temp_daily_mean",
        "line_number_of_water_temp_surface",
        "line_number_of_water_temp_bottom",
        "line_number_of_water_density_surface",
        "line_number_of_water_density_bottom",
    ]

    df = pd.DataFrame(columns=column_names)
    stations_seen_so_far = 0

    line_number_from_beginning_of_text = 0

    for station_name, station_text in station_text_dict.items():
        stations_seen_so_far += 1

        len_text = len(station_text)

        line_number_from_station_begin = 0

        first_lat_long = True
        first_air_temp = True
        first_water_temp = True
        first_density = True

        d: EnvironmentAttributes = {
            "current_station": station_name,
            "current_date": "",
            "current_dms_coords": "",
            "current_lat_degree": "",
            "current_lat_minute": "",
            "current_lat_second": "",
            "current_lat_coord": "",
            "current_long_degree": "",
            "current_long_minute": "",
            "current_long_second": "",
            "current_long_coord": "",
            "current_air_temp_noon": "",
            "current_air_temp_noon_degree": "",
            "current_air_temp_daily_mean": "",
            "current_air_temp_daily_mean_degree": "",
            "current_water_temp_surface": "",
            "current_water_temp_surface_degree": "",
            "current_water_temp_bottom": "",
            "current_water_temp_bottom_degree": "",
            "current_water_density_surface": "",
            "current_water_density_surface_number": "",
            "current_water_density_bottom": "",
            "current_water_density_bottom_number": "",
            "line_number_of_date": "",
            "line_number_of_lat_long": "",
            "line_number_air_temp_noon": "",
            "line_number_of_air_temp_daily_mean": "",
            "line_number_of_water_temp_surface": "",
            "line_number_of_water_temp_bottom": "",
            "line_number_of_water_density_surface": "",
            "line_number_of_water_density_bottom": "",
        }

        for line in station_text:
            line_number_from_station_begin += 1
            line_number_from_beginning_of_text += 1

            if "lat." in line and "long." in line and first_lat_long:
                if ", 18" in line or ",18" in line:  # 18 for 1876, 1877...
                    try:
                        first_lat_long = False

                        d.update(parse_coordinates(line))
                        d.update(
                            {
                                "current_date": line.split(";")[0],
                                "current_dms_coords": line.split(";")[1],
                                "line_number_of_date": line_number_from_beginning_of_text,
                                "line_number_of_lat_long": line_number_from_beginning_of_text,
                            }
                        )
                    except Exception as e:
                        logger.error(e)
                        d.update(
                            {
                                "current_date": f"{e}, {line}",
                                "current_dms_coords": f"{e}, {line}",
                            }
                        )
                else:
                    lat_index = line.find("lat")
                    d.update(
                        {"current_date": "", "current_dms_coords": line[lat_index:]}
                    )

            if "Temperature of air" in line and first_air_temp:
                first_air_temp = False

                d.update(parse_air_temperature(line))
                d.update(
                    {
                        "line_number_air_temp_noon": line_number_from_beginning_of_text,
                        "line_number_of_air_temp_daily_mean": line_number_from_beginning_of_text,
                    }
                )

            if "Temperature of water" in line and first_water_temp:
                first_water_temp = False

                d.update(parse_water_temperature(line))
                d.update(
                    {
                        "line_number_of_water_temp_surface": line_number_from_beginning_of_text,
                        "line_number_of_water_temp_bottom": line_number_from_beginning_of_text,
                    }
                )

            if "Density" in line and first_density:
                first_density = False

                d.update(parse_density(line))
                d.update(
                    {
                        "line_number_of_water_density_surface": (
                            line_number_from_beginning_of_text
                        ),
                        "line_number_of_water_density_bottom": line_number_from_beginning_of_text,
                    }
                )

            if line_number_from_station_begin == len_text:
                df = df.append(d, ignore_index=True)

    return df


if __name__ == "__main__":
    main()
