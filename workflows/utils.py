import json
import logging
import re
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger("Utils")


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
