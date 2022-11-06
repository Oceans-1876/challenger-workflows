import json


def remove_invalid_species() -> None:
    with open("data/Oceans1876/invalid_species_names.json", "r") as f:
        invalid_species_names = json.load(f)

    with open("data/Oceans1876/stations.json", "r") as f:
        stations = json.load(f)

    for station in stations:
        invalid_species = []
        for species in station["Species"]:
            if species["name"] in invalid_species_names:
                invalid_species.append(species)
        for species in invalid_species:
            station["Species"].remove(species)

    with open("data/Oceans1876/stations.json", "w") as f:
        json.dump(stations, f, indent=4)

    with open("data/Oceans1876/species.json", "r") as f:
        species = json.load(f)

    invalid_species = []
    for record_id, sp in species["species"].items():
        if sp["name"] in invalid_species_names:
            invalid_species.append(record_id)
    for record_id in invalid_species:
        del species["species"][record_id]

    with open("data/Oceans1876/species.json", "w") as f:
        json.dump(species, f, indent=4)


if __name__ == "__main__":
    remove_invalid_species()
