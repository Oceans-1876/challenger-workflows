import re
from typing import Optional, Tuple

from models import (
    AirTemperature,
    Coordinates,
    Density,
    Error,
    ErrorType,
    WaterTemperature,
)


def coordinates(text: str) -> Tuple[Coordinates, ErrorType]:
    """
    Extract the explicit coordinates from the given text.

    Parameters
    ----------
    text : str

    Returns
    -------
    (Coordinates, ErrorType)
        Either item in the tuple can be empty.
        The second item is a dict of errors for Error.COORDS.
    """
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
        return {}, {Error.COORDS: ["Could not find coordinates"]}
    except Exception as e:
        return {}, {Error.COORDS: [f"line: {e.__traceback__.tb_lineno}: {e}"]}


def air_temperature(text: str) -> Tuple[AirTemperature, ErrorType]:
    """
    Extract `air_temp_noon`, `air_temp_daily_mean`, and their respective raw texts from the given text.

    Parameters
    ----------
    text : str

    Returns
    -------
    (AirTemperature, ErrorType)
        Either item in the tuple can be empty.
        The second item is a dict of errors for Error.AIR_TEMP_NOON and Error.AIR_TEMP_DAILY_MEAN.
    """
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
        errors[Error.AIR_TEMP_NOON] = ["Could not parse air temperature at noon"]

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
        errors[Error.AIR_TEMP_DAILY_MEAN] = [
            "Could not parse daily mean air temperature"
        ]

    return results, errors


def water_temperature(text: str) -> Tuple[WaterTemperature, ErrorType]:
    """
    Extract `water_temp_surface`, `water_temp_bottom`, and `raw_water_temp`.

    Parameters
    ----------
    text : str

    Returns
    -------
    (WaterTemperature, ErrorType)
        Either item in the tuple can be empty.
        The second item is a dict of errors for Error.WATER_TEMP and Error.WATER_TEMP_BOTTOM.
    """
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
            errors[Error.WATER_TEMP_BOTTOM] = [
                "Could not parse bottom water temperature"
            ]
    else:
        errors[Error.WATER_TEMP] = ["Could not parse water temperature"]

    return results, errors


def density(text: str) -> Tuple[Density, ErrorType]:
    """
    Extract `water_density_surface`, `water_density_bottom`, and `raw_water_density`.

    Parameters
    ----------
    text : str

    Returns
    -------
    (Density, ErrorType)
        Either item in the tuple can be empty.
        The second item is a dict of errors for Error.WATER_DENSITY and Error.WATER_DENSITY_BOTTOM.
    """
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
            errors[Error.WATER_DENSITY_BOTTOM] = [
                "Could not parse bottom water density"
            ]
    else:
        errors[Error.WATER_DENSITY] = ["Could not parse water density"]

    return results, errors


def date(text: str) -> Tuple[Optional[str], ErrorType]:
    """
    Extract date from the given text.

    Parameters
    ----------
    text : str

    Returns
    -------
    (str, ErrorType)
        The first item is optional and can be None.
        The second item is a dict of errors for Error.DATE.
    """

    try:
        return re.search(r"\w+ \d?\d, ?\d{4}", text).group(), {}
    except Exception as e:
        return None, {Error.DATE: [f"line: {e.__traceback__.tb_lineno}: {e}"]}
