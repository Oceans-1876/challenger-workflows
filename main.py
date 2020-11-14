import json
import logging
import re
import subprocess
from json import JSONDecodeError
from pathlib import Path
from typing import List
from typing import Tuple
from typing import TypedDict
from typing import Union

import click
import pandas as pd
import pdf2image
import pytesseract

logger = logging.getLogger(__name__)

input_path = Path("inputs")
output_path = Path("outputs")


class Coordinates(TypedDict):
    lat_degree: str
    lat_minute: str
    lat_second: str
    lat_coord: float
    long_degree: str
    long_minute: str
    long_second: str
    long_coord: float


class AirTemperature(TypedDict):
    air_temp_noon: str
    air_temp_noon_degree: str
    air_temp_daily_mean: str
    air_temp_daily_mean_degree: str


class WaterTemperature(TypedDict):
    water_temp_surface: str
    water_temp_surface_degree: str
    water_temp_bottom: str
    water_temp_bottom_degree: str


class Density(TypedDict):
    water_density_surface: str
    water_density_surface_number: str
    water_density_bottom: str
    water_density_bottom_number: str


EnvironmentAttributes = Union[Coordinates, AirTemperature, WaterTemperature, Density]


class Species(TypedDict):
    species_name: str
    offset_start: int
    offset_end: int
    canonical_form: str
    data_source_id: str
    data_source_title: str
    taxonId: str
    classification_path: str
    classification_path_rank: str


def get_new_species() -> Species:
    return Species(
        species_name="",
        offset_start=0,
        offset_end=0,
        canonical_form="",
        data_source_id="",
        data_source_title="",
        taxonId="",
        classification_path="",
        classification_path_rank="",
    )


class Station(
    TypedDict, Coordinates, AirTemperature, WaterTemperature, Density, total=False
):
    station: str
    start_page: int
    start_page_line: int
    end_page: int
    end_page_line: int
    raw_text: List[str]
    date: str
    raw_dms_coords: str
    line_number_of_date: int
    line_number_of_lat_long: int
    line_number_air_temp_noon: int
    line_number_of_air_temp_daily_mean: int
    line_number_of_water_temp_surface: int
    line_number_of_water_temp_bottom: int
    line_number_of_water_density_surface: int
    line_number_of_water_density_bottom: int


def get_new_station() -> Station:
    return Station(
        station="",
        start_page=0,
        start_page_line=0,
        end_page=0,
        end_page_line=0,
        raw_text=[],
        date="",
        raw_dms_coords="",
        lat_degree="",
        lat_minute="",
        lat_second="",
        lat_coord=0.0,
        long_degree="",
        long_minute="",
        long_second="",
        long_coord=0.0,
        air_temp_noon="",
        air_temp_daily_mean="",
        air_temp_daily_mean_degree="",
        air_temp_noon_degree="",
        water_temp_surface="",
        water_temp_bottom_degree="",
        water_temp_surface_degree="",
        water_temp_bottom="",
        water_density_bottom="",
        water_density_bottom_number="",
        water_density_surface="",
        water_density_surface_number="",
        line_number_of_date=0,
        line_number_of_lat_long=0,
        line_number_air_temp_noon=0,
        line_number_of_air_temp_daily_mean=0,
        line_number_of_water_temp_surface=0,
        line_number_of_water_temp_bottom=0,
        line_number_of_water_density_bottom=0,
        line_number_of_water_density_surface=0,
    )


def parse_data():
    """ Run info extraction techniques on file """
    parts = ["part1", "part2"]

    for part in parts:
        texts_path = output_path / part / "pages" / "texts"
        if not texts_path.exists():
            logger.warning(
                f"Could not find the text files for {part}. "
                f"You need to include the `--pdf` flag to convert the PDF files first."
            )
            return

        logger.info(f"Processing {part} stations...")
        stations = get_stations(texts_path)
        stations.to_csv(output_path / part / "stations" / "000_stations.csv")

    # merge files from part 1 and part 2 summaries
    merge_files(parts)


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


def merge_files(parts):
    for output in [
        "stations.csv",
        "parsed_species_names_with_station_and_offset.csv",
        "environment_info.csv",
    ]:
        concatenated_output = []
        for part in parts:
            concatenated_output.append(
                pd.read_csv(output_path / part / "stations" / f"000_{output}")
            )
        pd.concat(concatenated_output).reset_index().to_csv(output_path / output)


def get_stations(texts_path: Path) -> pd.DataFrame:
    """ Break summary texts into sections for each station, put in dictionary"""
    stations_output_path = texts_path.parent.parent / "stations"
    stations_output_path.mkdir(exist_ok=True)

    stations = []
    previous_station = get_new_station()

    station_index = -1
    station_text = []

    found_station = False
    i = 0
    text_file = None

    def add_station():
        previous_station["end_page"] = int(text_file.stem[4:])
        previous_station["end_page_line"] = i
        previous_station["raw_text"] = station_text
        previous_station.update(get_environment_info(station_text))
        previous_station.update(parse_species_names_gnrd(station_text))
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
                    station_text = []
                previous_station["station"] = station
                previous_station["start_page"] = int(text_file.stem[4:])
                previous_station["start_page_line"] = i
            elif found_station:
                station_text.append(line_cleaned)

    if i and text_file:
        add_station()

    return pd.DataFrame(stations)


def parse_species_names_gnrd(station_text: List[str]):
    gnfinder = subprocess.run(
        ["gnfinder", "find", "-c", "-l", "eng"],
        stdout=subprocess.PIPE,
        input="\n".join(station_text),
        encoding="utf-8",
    )

    species = get_new_species()

    try:
        names = json.loads(gnfinder.stdout).get("names")
        if names:
            for name_dict in names:
                best_results = name_dict["verification"]["bestResult"]

                species.update(
                    {
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
                    }
                )
    except JSONDecodeError:
        logger.error("No json was returned by the server - skipping station!")
        pass

    return species


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
    dms_coords = line.split(";")[1]

    lat, long = dms_coords.split(",")

    (lat_coord, lat_degree, lat_minute, lat_second,) = parse_coordinate(lat)
    (long_coord, long_degree, long_minute, long_second,) = parse_coordinate(long)

    return {
        "lat_degree": lat_degree,
        "lat_minute": lat_minute,
        "lat_second": lat_second,
        "lat_coord": lat_coord,
        "long_degree": long_degree,
        "long_minute": long_minute,
        "long_second": long_second,
        "long_coord": long_coord,
    }


def parse_air_temperature(line: str) -> AirTemperature:
    # fix for line 33964 of part 1 summary
    # 5.45 p.M. made sail and proceeded towards the Crozet Islands. Temperature of air at
    if ";" in line:
        air_temp_noon = line.split(";")[0]
        air_temp_noon_degree = air_temp_noon.split(",")[1]
        air_temp_noon_degree = re.sub("[^0-9]", "", air_temp_noon_degree)
        air_temp_noon_degree = (
            air_temp_noon_degree[0:-1] + "." + air_temp_noon_degree[-1]
        )

        try:
            air_temp_daily_mean = line.split(";")[1]
            air_temp_daily_mean_degree = air_temp_daily_mean.split(",")[1]
            try:
                air_temp_daily_mean_degree = air_temp_daily_mean_degree.split(".")[0]
                air_temp_daily_mean_degree = re.sub(
                    "[^0-9]", "", air_temp_daily_mean_degree
                )
                air_temp_daily_mean_degree = (
                    air_temp_daily_mean_degree[0:-1]
                    + "."
                    + air_temp_daily_mean_degree[-1]
                )

            except Exception as e:
                logger.error(f"Parse air temperature: {e}")
                air_temp_daily_mean_degree = re.sub(
                    "[^0-9]", "", air_temp_daily_mean_degree
                )
                air_temp_daily_mean_degree = (
                    air_temp_daily_mean_degree[0:-1]
                    + "."
                    + air_temp_daily_mean_degree[-1]
                )

        except Exception as e:
            logger.error(f"Parse air temperature: {e}")
            air_temp_daily_mean = ""
            air_temp_daily_mean_degree = ""
    else:
        air_temp_noon = line
        air_temp_noon_degree = air_temp_noon.split(",")[1]
        air_temp_noon_degree = re.sub("[^0-9]", "", air_temp_noon_degree)
        air_temp_noon_degree = (
            air_temp_noon_degree[0:-1] + "." + air_temp_noon_degree[-1]
        )
        air_temp_daily_mean = ""
        air_temp_daily_mean_degree = ""

    return {
        "air_temp_noon": air_temp_noon,
        "air_temp_noon_degree": air_temp_noon_degree,
        "air_temp_daily_mean": air_temp_daily_mean,
        "air_temp_daily_mean_degree": air_temp_daily_mean_degree,
    }


def parse_water_temperature(line: str) -> WaterTemperature:
    if ";" in line:
        water_temp_surface = line.split(";")[0]
        try:
            water_temp_surface_degree = water_temp_surface.split(",")[1]
            water_temp_surface_degree = re.sub("[^0-9]", "", water_temp_surface_degree)
            water_temp_surface_degree = (
                water_temp_surface_degree[0:-1] + "." + water_temp_surface_degree[-1]
            )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            # currentWaterTempSurfaceDegree = ""
            water_temp_surface_degree = re.sub("[^0-9]", "", water_temp_surface)
            water_temp_surface_degree = (
                water_temp_surface_degree[0:-1] + "." + water_temp_surface_degree[-1]
            )

        try:
            water_temp_bottom = line.split(";")[1]
            water_temp_bottom_degree = water_temp_bottom.split(",")[1]
            try:
                water_temp_bottom_degree = water_temp_bottom_degree.split(".")[0]
                water_temp_bottom_degree = re.sub(
                    "[^0-9]", "", water_temp_bottom_degree
                )
                water_temp_bottom_degree = (
                    water_temp_bottom_degree[0:-1] + "." + water_temp_bottom_degree[-1]
                )

            except Exception as e:
                logger.error(f"Parse water temperature: {e}")
                water_temp_bottom_degree = water_temp_bottom_degree
                water_temp_bottom_degree = re.sub(
                    "[^0-9]", "", water_temp_bottom_degree
                )
                water_temp_bottom_degree = (
                    water_temp_surface_degree[0:-1] + "." + water_temp_bottom_degree[-1]
                )

        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            water_temp_bottom = ""
    else:
        water_temp_surface = line
        try:
            water_temp_surface_degree = water_temp_surface.split(",")[1]
            water_temp_surface_degree = re.sub("[^0-9]", "", water_temp_surface_degree)
            water_temp_surface_degree = (
                water_temp_surface_degree[0:-1] + "." + water_temp_surface_degree[-1]
            )
        except Exception as e:
            logger.error(f"Parse water temperature: {e}")
            water_temp_surface_degree = ""

        water_temp_bottom = ""
        water_temp_bottom_degree = ""
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
        "water_temp_surface": water_temp_surface,
        "water_temp_surface_degree": water_temp_surface_degree,
        "water_temp_bottom": water_temp_bottom,
        "water_temp_bottom_degree": water_temp_bottom_degree,
    }


def parse_density(line: str) -> Density:
    if ";" in line:
        water_density_surface = line.split(";")[0]
        water_density_surface_number = water_density_surface.split(",")[1]
        water_density_surface_number = re.sub(
            "[^0-9]", "", water_density_surface_number
        )
        water_density_surface_number = (
            water_density_surface_number[0] + "." + water_density_surface_number[1:]
        )

        water_density_bottom = line.split(";")[1]
        water_density_bottom_number = water_density_bottom.split(",")[1]
        try:
            water_density_bottom_number = water_density_bottom_number.split(".")[0]
            water_density_bottom_number = re.sub(
                "[^0-9]", "", water_density_bottom_number
            )
            water_density_bottom_number = (
                water_density_bottom_number[0] + "." + water_density_bottom_number[1:]
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            water_density_bottom_number = re.sub(
                "[^0-9]", "", water_density_bottom_number
            )
            water_density_bottom_number = (
                water_density_bottom_number[0] + "." + water_density_bottom_number[1:]
            )

    else:
        try:
            water_density_surface = line
            water_density_surface_number = water_density_surface.split(",")[1]
            water_density_surface_number = re.sub(
                "[^0-9]", "", water_density_surface_number
            )
            water_density_surface_number = (
                water_density_surface_number[0] + "." + water_density_surface_number[1:]
            )

        except Exception as e:
            logger.error(f"Parse density: {e}")
            water_density_surface = ""
            water_density_surface_number = ""

        water_density_bottom = ""
        water_density_bottom_number = ""
        # need to fix if in the form:
        # Density at 60° F. :—

        # Surface, . . . 1:02739 400 fathoms, . . 102640
        # 100 fathoms, ; 102782 500 - , . 102612
        # 200 , =. . 1:02708 Bottom, . , ; 102607
        # 300 , ~~. , 1:02672

    return {
        "water_density_surface": water_density_surface,
        "water_density_surface_number": water_density_surface_number,
        "water_density_bottom": water_density_bottom,
        "water_density_bottom_number": water_density_bottom_number,
    }


def get_environment_info(station_text: List[str]) -> EnvironmentAttributes:
    first_lat_long = True
    first_air_temp = True
    first_water_temp = True
    first_density = True

    d: EnvironmentAttributes = {
        "date": "",
        "raw_dms_coords": "",
        "lat_degree": "",
        "lat_minute": "",
        "lat_second": "",
        "lat_coord": "",
        "long_degree": "",
        "long_minute": "",
        "long_second": "",
        "long_coord": "",
        "air_temp_noon": "",
        "air_temp_noon_degree": "",
        "air_temp_daily_mean": "",
        "air_temp_daily_mean_degree": "",
        "water_temp_surface": "",
        "water_temp_surface_degree": "",
        "water_temp_bottom": "",
        "water_temp_bottom_degree": "",
        "water_density_surface": "",
        "water_density_surface_number": "",
        "water_density_bottom": "",
        "water_density_bottom_number": "",
        "line_number_of_date": "",
        "line_number_of_lat_long": "",
        "line_number_air_temp_noon": "",
        "line_number_of_air_temp_daily_mean": "",
        "line_number_of_water_temp_surface": "",
        "line_number_of_water_temp_bottom": "",
        "line_number_of_water_density_surface": "",
        "line_number_of_water_density_bottom": "",
    }

    for idx, line in enumerate(station_text):
        line_number = idx + 1
        if "lat." in line and "long." in line and first_lat_long:
            if ", 18" in line or ",18" in line:  # 18 for 1876, 1877...
                try:
                    first_lat_long = False

                    d.update(parse_coordinates(line))
                    d.update(
                        {
                            "date": line.split(";")[0],
                            "raw_dms_coords": line.split(";")[1],
                            "line_number_of_date": line_number,
                            "line_number_of_lat_long": line_number,
                        }
                    )
                except Exception as e:
                    logger.error(e)
                    d.update({"date": f"{e}, {line}", "raw_dms_coords": f"{e}, {line}"})
            else:
                lat_index = line.find("lat")
                d.update({"date": "", "raw_dms_coords": line[lat_index:]})

        if "Temperature of air" in line and first_air_temp:
            first_air_temp = False

            d.update(parse_air_temperature(line))
            d.update(
                {
                    "line_number_air_temp_noon": line_number,
                    "line_number_of_air_temp_daily_mean": line_number,
                }
            )

        if "Temperature of water" in line and first_water_temp:
            first_water_temp = False

            d.update(parse_water_temperature(line))
            d.update(
                {
                    "line_number_of_water_temp_surface": line_number,
                    "line_number_of_water_temp_bottom": line_number,
                }
            )

        if "Density" in line and first_density:
            first_density = False

            d.update(parse_density(line))
            d.update(
                {
                    "line_number_of_water_density_surface": line_number,
                    "line_number_of_water_density_bottom": line_number,
                }
            )

    return d


@click.command()
@click.option("--pdf", is_flag=True, help="Convert PDFs to text")
@click.option("--parse", is_flag=True, help="Parse data from converted text")
def main(pdf, parse):
    if not pdf and not parse:
        with click.Context(main) as ctx:
            click.echo(main.get_help(ctx))
    if pdf:
        process_pdf()
    if parse:
        parse_data()


if __name__ == "__main__":
    main()
