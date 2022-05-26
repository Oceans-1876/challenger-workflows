import json
import logging
import re
import subprocess
import sys
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger("Utils")

GNAMES_VERSIONS = {"gnfinder": 0.19, "gnverifier": 1.0}


def check_gnames_app(app_name: str) -> str:
    """
    Check if the given gnames app exists on the system
    and its version is not less than min_version.
    If the app does not exist terminate the process.
    If the version is not satisfied, log a warning.

    Parameters
    ----------
    app_name: names of a Global Names (gnames) app

    Returns
    -------
    version of the existing gnames app
    """
    min_version = GNAMES_VERSIONS[app_name]

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


def camelcase_to_snakecase(name: str) -> str:
    """
    Convert a camelcase string to snakecase.
    """
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


class PydanticJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that converts pydantic models to dicts.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, BaseModel):
            return obj.dict()
        return json.JSONEncoder.default(self, obj)
