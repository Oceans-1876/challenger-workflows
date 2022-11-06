import logging
from typing import Dict, List

import pandas as pd

from .gnames import GNames

logger = logging.getLogger("Plymouth")

taxa_priorities = [
    "phylum",
    "class",
    "order",
    "family",
]


def update_data() -> None:
    gnames = GNames()

    processed_stations = pd.read_json("data/Oceans1876/stations.json")

    plymouth_species = pd.read_csv(
        "data/Plymouth/summary_species.csv", keep_default_na=False
    )

    # This can be either Plymouth or Oceans1876
    plymouth_species["Source"] = "Plymouth"

    plymouth_species["WoRMS ID"] = None
    plymouth_species["WoRMS Taxa"] = None

    plymouth_species["Genus"] = plymouth_species["Genus"].str.strip().replace(" ", "")
    plymouth_species["Species"] = (
        plymouth_species["Species"].str.strip().replace(" ", "")
    )

    # The new column is used for search in processed_species
    plymouth_species["sp_concat"] = plymouth_species[["Genus", "Species"]].apply(
        lambda x: " ".join(x).lower(), axis=1
    )

    # A cache of species for each stations' group
    processed_stations_species: Dict[str, List[dict]] = {}

    # The following two are used to keep track of changes for processed_species
    added_species: Dict[str, List[str]] = {}
    deleted_species = set()

    # Update the processed species with Plymouth dataset
    for idx, species_row in plymouth_species.iterrows():
        if species_row.Station:
            taxa = species_row["Taxa"].lower()
            species = species_row["sp_concat"]

            stations_group = species_row["Stations Group"]
            station_processed_species = processed_stations_species.setdefault(
                stations_group,
                (
                    processed_stations[
                        processed_stations["Station"] == stations_group.split(",")[0]
                    ]
                    .iloc[0]
                    .Species
                )
                or [],
            )

            try:
                # Find Plymouth species in processed species
                (idx, sp) = next(
                    (
                        (idx, sp)
                        for idx, sp in enumerate(station_processed_species)
                        if (sp["name"].lower() == taxa or sp["name"].lower() == species)
                    )
                )
                if sp["name"].lower() == taxa:
                    # Delete this species from the list of species for this station
                    station_processed_species.pop(idx)
                    deleted_species.add(sp["recordId"])
            except StopIteration:
                # Plymouth species not found in processed species
                added_species.setdefault(stations_group, []).append(
                    species.capitalize()
                )

    # Update Plymouth species with processed species
    for stations_group, species in processed_stations_species.items():
        for sp in species:
            plymouth_sp = plymouth_species[
                (plymouth_species["Stations Group"] == stations_group)
                & (plymouth_species["sp_concat"] == sp["name"].lower())
            ]
            if plymouth_sp.empty:
                # Insert species in Plymouth dataset
                stations_group_rows = plymouth_species[
                    plymouth_species["Stations Group"] == stations_group
                ]
                new_row = stations_group_rows.iloc[0].copy()
                sp["Taxa"] = ""
                sp["sp_concat"] = sp["name"]
                sp_list = sp["name"].split(" ")
                new_row["Genus"] = sp_list[0]
                if len(sp_list) > 1:
                    new_row["Species"] = sp_list[1]
                new_row["Tax auth"] = ""
                new_row["Area"] = ""
                new_row["Notes"] = ""
                new_row["depth (fathoms)"] = ""
                new_row["Source"] = "Oceans1876"

                plymouth_species.loc[stations_group_rows.index[-1] + 0.5] = new_row
                plymouth_species = plymouth_species.sort_index().reset_index(drop=True)

    for idx, species_row in plymouth_species.iterrows():
        logger.info(f"Processing {idx} - {species_row['sp_concat']}")
        verified_species = gnames.verify(
            species_row["sp_concat"].capitalize(), ["9"]
        ).get("bestResult")
        if verified_species:
            species_row["WoRMS ID"] = verified_species["recordId"]
            classification_path = verified_species["classificationPath"].split("|")
            classification_ranks = verified_species["classificationRanks"].split("|")
            for tp in taxa_priorities:
                try:
                    (_rank, taxa) = next(
                        filter(
                            lambda x: tp in x[0].lower(),
                            zip(classification_ranks, classification_path),
                        )
                    )
                    species_row["WoRMS Taxa"] = taxa
                    break
                except StopIteration:
                    continue
        else:
            logger.warning(f"Species not found: {idx} - {species_row}")

    plymouth_species.drop(["sp_concat"], axis=1).to_csv(
        "data/Plymouth/summary_species_updated.csv", index=False
    )


if __name__ == "__main__":
    update_data()
