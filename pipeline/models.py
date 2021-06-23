from enum import Enum
from typing import Dict, List, Optional, TypedDict


class Error(str, Enum):
    DATE = "date"
    COORDS = "coords"
    AIR_TEMP_NOON = "air_temp_noon"
    AIR_TEMP_DAILY_MEAN = "air_temp_daily_mean"
    WATER_TEMP = "water_temp"
    WATER_TEMP_BOTTOM = "water_temp_bottom"
    WATER_DENSITY = "water_density"
    WATER_DENSITY_BOTTOM = "water_density_bottom"


ErrorType = Dict[Error, List[str]]


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
    volume: Optional[str]
    start_page: Optional[int]
    start_page_line: Optional[int]
    end_page: Optional[int]
    end_page_line: Optional[int]
    raw_text: List[str]
    date: Optional[str]
    errors: ErrorType
