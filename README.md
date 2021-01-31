# Oceans 1876

## Data Pipeline

> Requires python > 3.8 and docker

### Setup

- Install the requirements in `requirements.txt`
- Put the following PDF files in the `inputs` folder (required for `--pdf` flag):
    - Challenger Summary part 1.pdf
    - Challenger Summary part 2.pdf
- Download the summary report zip files for each part from the following urls and extract them in
  `inputs/part<>/pages/texts`:
    - Part 1: http://hdl.handle.net/2027/uc1.c2755812
    - Part 2: http://hdl.handle.net/2027/uc1.c2755813


### How to use

`parse.sh` accepts two flags as mentioned in its help:

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

Split PDF images are put in `images` folder for each part.

The parsed data for each station is saved in a json file under `stations` folder for each part.
