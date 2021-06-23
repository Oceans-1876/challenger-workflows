# Oceans 1876

## Data Pipeline

> Requires python > 3.9 and docker

### Setup

- Install [Poetry](https://github.com/python-poetry/poetry) on your system, either globally or in your virtual
  environments (the former is preferred).
- Run `poetry install` to install the project dependencies.
  - If you just need to run the extractor and do not need the dev dependencies, you can run `poetry install --no-dev`.
- If you are going to do development work on the extractor, run `pre-commit install` in the project root.
- Put the following PDF files in the `data/pdfs` folder (required for `--pdf` flag):
    - Challenger Summary part 1.pdf
    - Challenger Summary part 2.pdf
- Download the summary report zip files from the following urls and put them in `data/<VOLUME>/pages/texts` folders:

  | VOLUME | Url                                     |
  | ------ | --------------------------------------- |
  | `s1`   | http://hdl.handle.net/2027/uc1.c2755812 |
  | `s2`   | http://hdl.handle.net/2027/uc1.c2755813 |


### How to use

Run `parse.sh --help` to see the available actions and options:

```
Usage: [OPTIONS]

Options:
  --parse          Parse data from converted text.
  --pdf            Split PDFs into images.
  --stations TEXT  Parse the given stations and updates their json and entries
                   in CSVs.

                   Accepts a comma-separated list of stations in the following
                   format:

                   <part_number>/<station_index>

                   `station_index` is the number at the beginning of a station
                   json file in `outputs/<part_number>/stations/`. You can
                   leave out the leading zeros for index.

                   Example: --stations 1/12,1/13,2/115 (parses stations 12 and 13 of part 1 and station 115 of part 2).

  --errors TEXT    print out stations with a given error.

                   Accepts a comma-separated list of error keys.

                   Example: --errors date,lat,long (find all stations with
                   error for their date, lat, and longs fields).
```

Split PDF images are put in `<VOLUME>/pages/images` folder for each volume.

The parsed data for each station is saved in a json file under `<VOLUME>/stations` folder for each volume.
