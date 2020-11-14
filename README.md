# Oceans 1876

## Data Pipeline

> Requires python > 3.8 and docker

### Setup

- Install the requirements in `requirements.txt`
- Put the following PDF files in the `inputs` folder:
    - Challenger Summary part 1.pdf
    - Challenger Summary part 2.pdf


### How to use

`parse.sh` accepts two flags as mentioned in its help:

```
Usage:  [OPTIONS]

Options:
  --pdf    Convert PDFs to text
  --parse  Parse data from converted text
  --help   Show this message and exit.
```

`--parse` flag must be used with or after calling `--pdf` flag. Converted PDFs are put in `images` and `texts` folder
for each part. Each page will be placed in its own text and image file.
The parsed data for each station is saved in a json file under `stations` folder for each part.
