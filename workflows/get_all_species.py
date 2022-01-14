"""
Extract all the species mentioned in the summary report index.
The index contains a list of all genus, their species, and all the pages they are
mentioned in the summary reports.
"""
import argparse
import datetime
import json
import logging
import pathlib
import re
import subprocess
import sys
import threading
import time
from enum import Enum
from typing import Dict, List, Optional, Tuple, TypedDict, Union, cast

import cv2 as cv
import numpy as np
import pytesseract
from utils import check_gnames_app

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
logger = logging.getLogger("Species Extractor")

parser = argparse.ArgumentParser()
parser.add_argument(
    "--debug",
    action="store_true",
    help="Saves processed images in `data/tmp` for visual inspection",
)
parser.add_argument(
    "--process", action="store_true", help="Process the index and extract species"
)
parser.add_argument(
    "--process-text",
    action="store_true",
    help="Process the extracted texts stored in index_species.json",
)
parser.add_argument(
    "--verify-species",
    action="store_true",
    help="Verify species with missing matches",
)

DATA_PATH = pathlib.Path("../data/HathiTrust/sec.6 v.2/images/")
OUTPUT_PATH = pathlib.Path("../data/Oceans1876/")

DEBUG_OUTPUT_PATH = pathlib.Path("../data/tmp")
if not DEBUG_OUTPUT_PATH.exists():
    DEBUG_OUTPUT_PATH.mkdir()


INDEX_PAGES = range(739, 849)

DebugDict = TypedDict(
    "DebugDict",
    {
        "page": int,
        "column": int,
        "line": int,
        "bounding_box": Tuple[int, int, int, int],
        "texts": List[str],
        "message": str,
        "need_verification": bool,
    },
)


class LineType(str, Enum):
    GENUS = "genus"
    GENUS_SYNONYM = "genus_synonym"
    SPECIES = "species"
    CONTINUATION = "continuation"
    ERROR = "error"


ProcessedLineDict = TypedDict(
    "ProcessedLineDict",
    {
        "type": LineType,
        "value": Optional[str],
        "synonym": Optional[str],
        "pages": List[int],
        "text": str,
        "need_verification": bool,
    },
)

MatchedSpeciesScoreDict = TypedDict(
    "MatchedSpeciesScoreDict",
    {
        "infraSpecificRankScore": int,
        "fuzzyLessScore": int,
        "curatedDataScore": int,
        "authorMatchScore": int,
        "acceptedNameScore": int,
        "parsingQualityScore": int,
    },
)

MatchedSpeciesDict = TypedDict(
    "MatchedSpeciesDict",
    {
        "dataSourceId": int,
        "dataSourceTitleShort": str,
        "curation": str,
        "recordId": str,
        "outlink": str,
        "entryDate": str,
        "matchedName": str,
        "matchedCardinality": int,
        "matchedCanonicalSimple": str,
        "matchedCanonicalFull": str,
        "currentRecordId": str,
        "currentName": str,
        "currentCardinality": int,
        "currentCanonicalSimple": str,
        "currentCanonicalFull": str,
        "isSynonym": bool,
        "classificationPath": str,
        "classificationRanks": str,
        "classificationIds": str,
        "editDistance": int,
        "stemEditDistance": int,
        "matchType": str,
        "scoreDetails": MatchedSpeciesScoreDict,
    },
)

GenusSynonymDict = TypedDict("GenusSynonymDict", {"genus": str, "debug": DebugDict})

SpeciesDict = TypedDict(
    "SpeciesDict",
    {
        "species": str,
        "genus_synonym": Optional[GenusSynonymDict],
        "matched_species": Optional[str],
        "pages": List[int],
        "debug": DebugDict,
    },
)

GenusDict = TypedDict(
    "GenusDict",
    {
        "genus": str,
        "synonym": Optional[str],
        "matched_species": Optional[str],
        "pages": List[int],
        "species": List[SpeciesDict],
        "debug": DebugDict,
    },
)

Point = Tuple[int, int]


class SpeciesProcessor:
    def __init__(self, debug: bool = False):
        self.gnverifier_version = check_gnames_app("gnverifier", 0.6)
        self.debug = debug
        self.species: List[GenusDict] = []
        self.species_verified: Dict[str, MatchedSpeciesDict] = {}
        self.unverified_lines: List[DebugDict] = []
        self.species_verification_threads: List[threading.Thread] = []

    def process(self) -> None:
        start_time = time.time()

        try:
            for page_number in INDEX_PAGES:
                logger.info(f"Processing page {page_number}")
                img_path = DATA_PATH / f"{page_number:08}.png"

                img = cv.imread(str(img_path))
                h, w, _ = img.shape

                # Remove part of the white space on the edges.
                img_cropped = img[350 : h - 350, 200 : w - 150]
                h_cropped, w_cropped, _ = img_cropped.shape

                # Detect edges
                img_edged = cv.Canny(img_cropped, 100, 200)

                # Use a wide kernel(W: 65, H: 20) to turn the main text
                # on the page into one big blob.
                img_dilated = cv.dilate(
                    img_edged,
                    cv.getStructuringElement(cv.MORPH_RECT, (65, 20)),
                    iterations=1,
                )

                img_contours, _ = cv.findContours(
                    img_dilated, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE
                )

                # Find the biggest contour, which should be the main text.
                max_area = 0
                max_area_idx = 0
                for idx, contour in enumerate(img_contours):
                    area = cv.contourArea(contour)
                    if area > max_area:
                        max_area = area
                        max_area_idx = idx
                img_text = img_contours[max_area_idx]

                br_x, br_y, br_width, br_height = cv.boundingRect(img_text)

                # Crop the image to the main text blob.
                img_text_cropped = img_cropped[
                    br_y : br_y + br_height, br_x : br_x + br_width
                ]

                # Detect edges on the cropped image.
                img_text_canny = cv.Canny(img_text_cropped, 100, 200)

                # Dilate the objects with a narrow and tall kernel (W: 1, H: 30)
                # to separate the columns and the lines in between them.
                img_text_dilated = cv.dilate(
                    img_text_canny,
                    cv.getStructuringElement(cv.MORPH_RECT, (1, 30)),
                    iterations=1,
                )

                # Find the contours on the dilated img_text.
                # The bottom 100 pixels are cropped to avoid merging of some texts
                # at the bottom into the columns.
                text_contours, _ = cv.findContours(
                    img_text_dilated[:-100, :], cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE
                )

                # Order the text_contours by area and remove the biggest 3,
                # which should be the main columns.
                try:
                    text_contours_without_columns = sorted(
                        text_contours, key=cv.contourArea, reverse=True
                    )[3:]
                except IndexError:
                    logger.warning(f"Less than 3 columns found on page {page_number}")
                    continue

                # Order the remaining contours by height.
                # The first two contours should be the separators.
                try:
                    separator_line_contours = sorted(
                        text_contours_without_columns,
                        key=lambda c: cv.boundingRect(c)[3],
                        reverse=True,
                    )[:2]
                except IndexError:
                    logger.warning(
                        f"Less than 2 separator lines found on page {page_number}"
                    )
                    continue

                # Use the slope between the top most and bottom most points of
                # the separator line contours to determine the orientation of the image.
                slopes = []  # holds the slopes of the two separator lines.

                separator_lines = []  # holds the two separator lines.

                for line in separator_line_contours:
                    (_, topmost, bottommost, _) = self.get_contour_extremities(line)
                    slopes.append(
                        -90
                        + np.degrees(
                            np.arctan2(
                                bottommost[1] - topmost[1], bottommost[0] - topmost[0]
                            )
                        )
                    )
                    separator_lines.append((topmost, bottommost))

                # Rotate the image by mean of the two slopes.
                rotation_matrix = cv.getRotationMatrix2D(
                    (br_width // 2, br_height // 2), np.mean(slopes), 1
                )
                img_text_rotated = cv.warpAffine(
                    img_text_cropped,
                    rotation_matrix,
                    (br_width, br_height),
                    borderValue=(255, 255, 255),
                )

                # Sort the separator lines by their x-coordinate (from left to right).
                separator_lines.sort(key=lambda l: (l[0][0], l[1][0]))

                # Rotate the separator lines by the same angle as the image.
                separator_lines_rotated = []
                for line in separator_lines:
                    coordinates = []
                    for coordinate in line:
                        coordinates.append(
                            (
                                np.dot(
                                    rotation_matrix[:, :2], coordinate
                                )  # the first 2 columns are the rotation matrix
                                + rotation_matrix[
                                    :, 2
                                ]  # the last column is the translation
                            ).astype(int)
                        )
                    separator_lines_rotated.append(coordinates)

                # Crop the columns from the image.
                (left_line, right_line) = separator_lines_rotated
                # It is safe to remove the first 10 columns.
                # They are empty spaces and it reduces noise.
                columns = [
                    img_text_rotated[:, 10 : max(left_line[0][0], left_line[1][0])],
                    img_text_rotated[
                        :,
                        10
                        + max(left_line[0][0], left_line[1][0]) : max(
                            right_line[0][0], right_line[1][0]
                        ),
                    ],
                    img_text_rotated[:, 10 + max(right_line[0][0], right_line[1][0]) :],
                ]

                for idx, column in enumerate(columns):
                    self.process_column(column, page_number, idx + 1)

                if self.debug:
                    self.save_intermediate_images(
                        page_number,
                        img_cropped,
                        img_text,
                        (br_x, br_y, br_width, br_height),
                    )
        except Exception as e:
            logger.exception(e)

        self.save_species(save_errors=True)

        logger.info(f"Total processing time: {time.time() - start_time}")

    @staticmethod
    def get_contour_extremities(
        contour: np.ndarray,
    ) -> Tuple[Point, Point, Point, Point]:

        leftmost: Point = tuple(contour[contour[:, :, 0].argmin()][0])  # type: ignore
        topmost: Point = tuple(contour[contour[:, :, 1].argmin()][0])  # type: ignore
        bottommost: Point = tuple(contour[contour[:, :, 1].argmax()][0])  # type: ignore
        rightmost: Point = tuple(contour[contour[:, :, 0].argmax()][0])  # type: ignore
        return leftmost, topmost, bottommost, rightmost

    def process_column(
        self, column: np.ndarray, page_number: int, column_number: int
    ) -> None:
        logger.info(f"\tProcessing column {column_number}")

        # Reduce noise.
        column_denoised = cv.fastNlMeansDenoising(column, None, 7, 21)

        # Detect edges.
        column_edges = cv.Canny(column_denoised, 100, 200)

        # Find the first column that has text.
        start_column = (
            cv.reduce(column_edges, 0, cv.REDUCE_SUM, dtype=cv.CV_32S) > 2000
        ).argmax()

        h, w, _ = column.shape

        # Dilated the image with a rectangle of size (w / 5, h)
        # to discover the lines with text. w/5 is a safe value for this purpose.
        column_dilated = cv.dilate(
            column_edges,
            cv.getStructuringElement(cv.MORPH_RECT, (column.shape[1] // 5, 1)),
            iterations=1,
        )

        # Find the contours of the text lines. Only do this on the left side
        # of the image to avoid some noises on the right side.
        contours, _ = cv.findContours(
            column_dilated[:, : column.shape[1] // 2],
            cv.RETR_EXTERNAL,
            cv.CHAIN_APPROX_SIMPLE,
        )

        # Sort the contours by their y-coordinate (from top to bottom).
        contours = sorted(contours, key=lambda c: cv.boundingRect(c)[1])

        img_debug = column_denoised.copy() if self.debug else None

        current_genus = None
        current_genus_synonym = None
        for idx, contour in enumerate(contours):
            c_x, c_y, c_w, c_h = cv.boundingRect(contour)

            # Add 1px buffer to the top and bottom of the contour.
            c_y = c_y - 2
            c_h = c_h + 2

            if c_h < 10:
                # Contours with height less than 10 are noise.
                continue

            debug_info = DebugDict(
                texts=[],
                page=page_number,
                column=column_number,
                line=idx + 1,
                bounding_box=(c_x, c_y, c_w, c_h),
                message="",
                need_verification=False,
            )

            if c_x <= (2 * start_column // 3):
                if self.debug:
                    cv.rectangle(
                        img_debug, (c_x, c_y), (c_x + c_w, c_y + c_h), (0, 0, 255), 2
                    )

                # If the contour start x is less than 2/3 of the start column,
                # it's a genus. 2/3 is a safe value for this purpose.
                extracted_text = self.process_line(column_denoised[c_y : c_y + c_h, :])
                processed_line = self.process_text(extracted_text, LineType.GENUS)

                debug_info["texts"].append(processed_line["text"])
                debug_info["message"] = "genus"
                debug_info["need_verification"] = processed_line["need_verification"]

                if processed_line["type"] == LineType.GENUS:
                    if current_genus:
                        # Save the previous genus and its species and start a new one.
                        self.species.append(current_genus)

                    genus_value = cast(str, processed_line["value"])
                    current_genus = GenusDict(
                        genus=genus_value,
                        synonym=processed_line["synonym"],
                        matched_species=None,
                        pages=processed_line["pages"],
                        species=[],
                        debug=debug_info,
                    )

                    # Start genus verification in a separate thread.
                    thread = threading.Thread(
                        target=self.verify_species,
                        args=(current_genus["genus"], current_genus),
                    )
                    thread.start()
                    self.species_verification_threads.append(thread)

                    current_genus_synonym = None

                elif processed_line["type"] == LineType.CONTINUATION:
                    debug_info["message"] = "genus_continuation"
                    if current_genus:
                        current_genus["pages"].extend(processed_line["pages"])
                        current_genus["debug"]["texts"].append(processed_line["text"])
                    else:
                        # This should never happen.
                        logger.warning(
                            "Genus continuation without current genus: "
                            f"{processed_line}"
                        )

                elif processed_line["type"] == LineType.GENUS_SYNONYM:
                    debug_info["message"] = "genus_synonym"
                    genus_value = cast(str, processed_line["value"])
                    current_genus_synonym = GenusSynonymDict(
                        genus=genus_value, debug=debug_info
                    )

                else:
                    logger.warning(f"\t\tLine is not a genus: {processed_line['text']}")
                    debug_info["message"] = "invalid genus line"
                    self.unverified_lines.append(debug_info)

            else:
                if self.debug:
                    cv.rectangle(
                        img_debug, (c_x, c_y), (c_x + c_w, c_y + c_h), (0, 255, 0), 2
                    )

                # Otherwise, it's a species, or continuation of previous genus.
                extracted_text = self.process_line(column_denoised[c_y : c_y + c_h, :])
                processed_line = self.process_text(extracted_text, LineType.SPECIES)

                debug_info["texts"].append(processed_line["text"])
                debug_info["message"] = "species"
                debug_info["need_verification"] = processed_line["need_verification"]

                if processed_line["type"] == LineType.SPECIES:
                    species_value = cast(str, processed_line["value"])
                    species = SpeciesDict(
                        species=species_value,
                        matched_species=None,
                        pages=processed_line["pages"],
                        genus_synonym=current_genus_synonym,
                        debug=debug_info,
                    )
                    if current_genus:
                        current_genus["species"].append(species)

                        # Start species verification in a separate thread.
                        thread = threading.Thread(
                            target=self.verify_species,
                            args=(
                                f"{current_genus['genus']} {species['species']}",
                                species,
                            ),
                        )
                        thread.start()
                        self.species_verification_threads.append(thread)
                    else:
                        # This should never happen.
                        logger.warning(
                            "Current genus species without current genus: "
                            f"{processed_line}"
                        )

                elif processed_line["type"] == LineType.CONTINUATION:
                    if current_genus:
                        if current_genus["species"]:
                            current_genus_last_species: SpeciesDict = current_genus[
                                "species"
                            ][-1]
                            current_genus_last_species["pages"].extend(
                                processed_line["pages"]
                            )
                            current_genus_last_species["debug"]["texts"].append(
                                processed_line["text"]
                            )
                        else:
                            # This is a continuation of the previous genus
                            # (still need to verify).
                            current_genus["pages"].extend(processed_line["pages"])
                            current_genus["debug"]["texts"].append(
                                processed_line["text"]
                            )
                    else:
                        # This should never happen.
                        logger.warning(
                            "Genus/species continuation without current genus: "
                            f"{processed_line}"
                        )

                elif processed_line["type"] == LineType.GENUS_SYNONYM:
                    debug_info["message"] = "genus_synonym"
                    genus_value = cast(str, processed_line["value"])
                    current_genus_synonym = GenusSynonymDict(
                        genus=genus_value, debug=debug_info
                    )

                else:
                    logger.warning(
                        f"\t\tLine {idx+1} is not a species: {processed_line['text']}"
                    )
                    debug_info["message"] = "invalid species line"
                    self.unverified_lines.append(debug_info)

        if current_genus:
            # Save the previous genus and its species and start a new one.
            self.species.append(current_genus)

        if self.debug:
            cv.imwrite(
                str(DEBUG_OUTPUT_PATH / f"{page_number:08}-{column_number}.png"),
                img_debug,
            )

    def process_line(self, line: np.ndarray) -> str:
        text: str = pytesseract.image_to_string(line, config="--psm 7")

        # Clean up the text.
        # Replace em-dashes with hyphens.
        # Remove left and right single quotes.
        return (
            text.strip()
            .replace("\u2014", "-")
            .replace("\u2018", "")
            .replace("\u2019", "")
        )

    def process_text(self, text: str, line_type: LineType) -> ProcessedLineDict:
        processed_line = ProcessedLineDict(
            type=line_type,
            value=None,
            synonym=None,
            pages=[],
            text=text,
            need_verification=False,
        )

        if not text:
            processed_line["type"] = LineType.ERROR
            return processed_line

        # Double-check the given line type by looking at the first character.
        # Genus starts with an uppercase letter,
        # while species starts with a lowercase letter.
        if line_type == LineType.GENUS:
            first_char_is_verified = text[0].isupper()
        elif line_type == LineType.SPECIES:
            first_char_is_verified = text[0].islower()
            if not first_char_is_verified and text[0].isupper() and len(text) > 1:
                # It's possible that the first character was recognized incorrectly
                # as a different letter in capitalized form.
                # We can lower it and use gnverifier to see
                # if it finds a match, either exact or fuzzy.
                text = text[0].lower() + text[1:]
                first_char_is_verified = True
                processed_line["text"] = text
                processed_line["need_verification"] = True
        else:
            raise ValueError(f"Unknown line type: {line_type}")

        if first_char_is_verified:
            # Found a genus or species.
            line_matches = re.search(
                r"(?P<value>[a-z\u00C0-\u024F]+)[,.]?\s*(?P<rest>.*)",
                text,
                re.I,
            )
            if line_matches:
                processed_line["type"] = line_type
                processed_line["value"] = line_matches.group("value")
                rest = line_matches.group("rest")
                if rest:
                    rest_matches = re.search(
                        r"\(see (?P<synonym>[a-z\u00C0-\u024F]*)\)", rest, re.I
                    )
                    if rest_matches:
                        processed_line["synonym"] = rest_matches.group("synonym")

                    processed_line["pages"] = re.findall(r"\d+", rest)

                return processed_line

        if line_type == LineType.SPECIES and text[0].isupper():
            return processed_line

        elif text[0].isdigit():
            # The line is continuation of pages for the previous value.
            processed_line["type"] = LineType.CONTINUATION
            processed_line["pages"] = re.findall(r"\d+", text)
            return processed_line

        elif text[0] == "(" and len(text) > 1 and text[1].isupper():
            # This might be a synonym for the current genus.
            # Check for the value in parentheses.
            line_matches = re.search(r"\((?P<value>[A-Z][a-z\u00C0-\u024F]+)\)", text)
            if line_matches:
                processed_line["type"] = LineType.GENUS_SYNONYM
                processed_line["value"] = line_matches.group("value")
                return processed_line

        processed_line["type"] = LineType.ERROR
        return processed_line

    def retry_text_processing(self) -> None:
        self.load_species()

        for genus in self.species:
            debug_info: DebugDict = genus["debug"]
            genus["pages"] = []

            for extracted_text in debug_info["texts"]:
                logger.info(f"Processing genus text: {extracted_text}")
                processed_line = self.process_text(extracted_text, LineType.GENUS)

                debug_info["message"] = "genus"
                debug_info["need_verification"] = processed_line["need_verification"]

                if processed_line["type"] == LineType.GENUS:
                    genus_value = cast(str, processed_line["value"])
                    genus["genus"] = genus_value
                    genus["synonym"] = processed_line["synonym"]
                    genus["matched_species"] = None
                    genus["pages"] = processed_line["pages"]

                    self.verify_species(genus["genus"], genus)

                elif processed_line["type"] == LineType.CONTINUATION:
                    debug_info["message"] = "genus_continuation"
                    genus["pages"].extend(processed_line["pages"])

                elif processed_line["type"] == LineType.GENUS_SYNONYM:
                    debug_info["message"] = "genus_synonym"
                    genus["synonym"] = processed_line["synonym"]

                else:
                    logger.warning(f"\t\tLine is not a genus: {processed_line['text']}")
                    debug_info["message"] = "invalid genus line"

            current_genus_synonym = None

            for species in genus["species"]:
                species_debug_info: DebugDict = species["debug"]
                species["pages"] = []

                for extracted_text in species_debug_info["texts"]:
                    logger.info(f"\tProcessing species text: {extracted_text}")
                    processed_line = self.process_text(extracted_text, LineType.SPECIES)

                    species_debug_info["message"] = "species"
                    species_debug_info["need_verification"] = processed_line[
                        "need_verification"
                    ]

                    if processed_line["type"] == LineType.SPECIES:
                        species_value = cast(str, processed_line["value"])
                        species["species"] = species_value
                        species["matched_species"] = None
                        species["pages"] = processed_line["pages"]
                        species["genus_synonym"] = current_genus_synonym

                        self.verify_species(
                            f"{genus['genus']} {species['species']}", species
                        )

                    elif processed_line["type"] == LineType.CONTINUATION:
                        species["pages"].extend(processed_line["pages"])

                    elif processed_line["type"] == LineType.GENUS_SYNONYM:
                        species_debug_info["message"] = "genus_synonym"
                        current_genus_synonym_value = cast(str, processed_line["value"])
                        current_genus_synonym = GenusSynonymDict(
                            genus=current_genus_synonym_value, debug=species_debug_info
                        )

                    else:
                        logger.warning(
                            f"\t\tLine is not a species: {processed_line['text']}"
                        )
                        species_debug_info["message"] = "invalid species line"

        self.save_species()

    def verify_species(self, name: str, species: Union[GenusDict, SpeciesDict]) -> None:
        with subprocess.Popen(
            ["gnverifier", "-f", "compact", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as gnverifier_proc:
            if gnverifier_proc.stdout:
                verified_species = json.loads(gnverifier_proc.stdout.read())
                result = verified_species.get("bestResult")
                if result:
                    record_id = result.get("recordId")
                    if record_id:
                        species["matched_species"] = record_id  # type: ignore
                        self.species_verified[record_id] = result

    def retry_missing_verifications(self) -> None:
        self.load_species()

        total_processed = 0

        for genus in self.species:
            if not genus["matched_species"]:
                logger.info(f"Verifying {genus['genus']}")
                self.verify_species(genus["genus"], genus)
                if genus["matched_species"]:
                    total_processed += 1

                for species in genus["species"]:
                    if not species["matched_species"]:
                        logger.info(f"Verifying {genus['genus']} {species['species']}")
                        self.verify_species(
                            f"{genus['genus']} {species['species']}", species
                        )
                        if species["matched_species"]:
                            total_processed += 1

        logger.info(f"Retried {total_processed} species.")

        self.save_species()

    def load_species(self) -> None:
        if not (OUTPUT_PATH / "index_species.json").exists():
            sys.exit(
                "index_species.json does not exist. "
                "Try running the script with --process."
            )

        with open(OUTPUT_PATH / "index_species.json", "r") as f:
            self.species = json.load(f)["species"]

    def save_species(self, save_errors: bool = False) -> None:
        for thread in self.species_verification_threads:
            thread.join()

        metadata = {
            "gnverifier": self.gnverifier_version,
            "date": str(datetime.datetime.now()),
        }

        with open(OUTPUT_PATH / "index_species.json", "w") as f:
            json.dump({"metadata": metadata, "species": self.species}, f, indent=2)

        with open(OUTPUT_PATH / "index_species_verified.json", "w") as f:
            json.dump(
                {"metadata": metadata, "species": self.species_verified}, f, indent=2
            )

        if save_errors:
            with open(OUTPUT_PATH / "index_species_errors.json", "w") as f:
                json.dump(self.unverified_lines, f, indent=2)

    def save_intermediate_images(
        self,
        page_number: int,
        img: np.ndarray,
        text_contour: np.ndarray,
        text_contour_bbox: Tuple[int, int, int, int],
    ) -> None:
        img_debug = img.copy()

        # Draw the bounding rectangle around the main text blob (red).
        br_x, br_y, br_width, br_height = text_contour_bbox
        cv.rectangle(
            img_debug,
            (br_x, br_y),
            (br_x + br_width, br_y + br_height),
            (0, 0, 255),
            3,
        )

        # Draw the convex hull around the main text blob (green).
        hull = cv.convexHull(text_contour)
        cv.drawContours(img_debug, [hull], -1, (0, 255, 0), 3)

        # Add circles to contour extremities (blue).
        (leftmost, topmost, bottommost, rightmost) = self.get_contour_extremities(
            text_contour
        )
        cv.circle(img_debug, leftmost, 15, (255, 0, 0), -1)
        cv.circle(img_debug, rightmost, 15, (255, 0, 0), -1)
        cv.circle(img_debug, topmost, 15, (255, 0, 0), -1)
        cv.circle(img_debug, bottommost, 15, (255, 0, 0), -1)

        cv.imwrite(str(DEBUG_OUTPUT_PATH / f"{page_number:08}.png"), img_debug)


if __name__ == "__main__":
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not args.process and not args.process_text and not args.verify_species:
        parser.print_help()
    else:
        if args.process:
            SpeciesProcessor(args.debug).process()

        if args.process_text:
            SpeciesProcessor(args.debug).retry_text_processing()

        if args.verify_species:
            SpeciesProcessor(args.debug).retry_missing_verifications()
