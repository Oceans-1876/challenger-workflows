import json
import pathlib
import sys

import requests

WORK_DIR = pathlib.Path("../data")

datasource_URI = "https://verifier.globalnames.org/api/v1/data_sources"
datasource_file_path = WORK_DIR / "Oceans1876" / "data_sources.json"
url_template_file_path = WORK_DIR / "Oceans1876" / "uri_template.json"
Data = list[dict]


# Import JSON data
def import_json(filename: pathlib.Path) -> Data:
    try:
        with open(filename) as jf:
            data = json.load(jf)
        return data
    except FileNotFoundError:
        sys.exit(f"{filename} is missing")


# Export JSON data
def export_json(filename: pathlib.Path, output: Data) -> None:
    try:
        with open(filename, "w") as ojf:
            json.dump(output, ojf, indent=4)
    except Exception as e:
        print(e)


def get_data_sources_from_uri(
    datasource_URI: str, datasource_file_path: pathlib.Path
) -> None:

    data_sources = requests.get(datasource_URI)
    url_templates = import_json(url_template_file_path)

    expected_fields = [
        "id",
        "title",
        "titleShort",
        "description",
        "curation",
        "recordCount",
        "updatedAt",
        "isOutlinkReady",
        "homeURL",
        "URL_template",
    ]
    if data_sources.status_code == 200:
        data_sources_json = data_sources.json()
        for data_source in data_sources_json:
            for key in expected_fields:
                if key not in data_source.keys():
                    if key == "isOutlinkReady":
                        data_source[key] = False
                    elif key == "homeURL":
                        data_source[key] = None
                    elif key == "URL_template":
                        data_source[key] = url_templates.get(
                            str(data_source["id"]), None
                        )

            if key == "description":
                if len(data_source[key]) <= 4:
                    data_source[key] = None

            if str(data_source["id"]) not in url_templates:
                url_templates[str(data_source["id"])] = None

        export_json(datasource_file_path, data_sources_json)
    else:
        print(f"Received Status code: {data_sources.status_code} from the GNAMES API")


if __name__ == "__main__":
    get_data_sources_from_uri(datasource_URI, datasource_file_path)
