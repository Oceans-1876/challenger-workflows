"""
Extract all the species mentioned in the summary report index.
The index contains a list of all genus, their species, and all the pages they are
mentioned in the summary reports.
"""
import argparse
import json
import logging
import pathlib
import re
from enum import Enum
from typing import List, Optional, Tuple, TypedDict

import cv2 as cv
import numpy as np
import pytesseract

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--debug",
    action="store_true",
    help="Saves processed images in `data/tmp` for visual inspection",
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
        "texts": List[str],
        "message": str,
    },
)


class LineType(str, Enum):
    GENUS = "genus"
    SPECIES = "species"
    CONTINUATION = "continuation"
    ERROR = "error"


ProcessedLineDict = TypedDict(
    "ProcessedLineDict",
    {
        "type": LineType,
        "value": Optional[str],
        "pages": List[int],
        "text": str,
        "need_verification": bool,
    },
)

SpeciesDict = TypedDict(
    "SpeciesDict",
    {"species": str, "pages": List[int], "debug": DebugDict},
)

GenusDict = TypedDict(
    "GenusDict",
    {
        "genus": str,
        "pages": List[int],
        "species": List[SpeciesDict],
        "debug": DebugDict,
    },
)


class SpeciesProcessor:
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.species: List[GenusDict] = []
        self.unverified_lines: List[DebugDict] = []

    def process(self):
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

        with open(OUTPUT_PATH / "all_species.json", "w") as f:
            json.dump(self.species, f, indent=2)

        with open(OUTPUT_PATH / "all_species_unverified.json", "w") as f:
            json.dump(self.unverified_lines, f, indent=2)

    @staticmethod
    def get_contour_extremities(
        contour: np.ndarray,
    ) -> Tuple[Tuple[int, int], Tuple[int, int], Tuple[int, int], Tuple[int, int]]:
        leftmost = tuple(contour[contour[:, :, 0].argmin()][0])
        topmost = tuple(contour[contour[:, :, 1].argmin()][0])
        bottommost = tuple(contour[contour[:, :, 1].argmax()][0])
        rightmost = tuple(contour[contour[:, :, 0].argmax()][0])
        return leftmost, topmost, bottommost, rightmost

    def process_column(
        self, column: np.ndarray, page_number: int, column_number: int
    ) -> None:
        logger.info(f"\tProcessing column {column_number}")

        # Reduce noise.
        column_denoised = cv.fastNlMeansDenoisingColored(column, None, 20, 10, 7, 21)

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

        img_debug = column.copy() if self.debug else None

        current_genus = None
        for idx, contour in enumerate(contours):
            c_x, c_y, c_w, c_h = cv.boundingRect(contour)

            if c_h < 10:
                # Contours with height less than 10 are noise.
                continue

            debug_info = DebugDict(
                texts=[],
                page=page_number,
                column=column_number,
                line=idx + 1,
                message="",
            )

            if c_x <= (2 * start_column // 3):
                if self.debug:
                    cv.rectangle(
                        img_debug, (c_x, c_y), (c_x + c_w, c_y + c_h), (0, 0, 255), 2
                    )

                # If the contour start x is less than 2/3 of the start column,
                # it's a genus. 2/3 is a safe value for this purpose.
                processed_line = self.process_line(
                    column[c_y : c_y + c_h, :], LineType.GENUS
                )
                debug_info["texts"].append(processed_line["text"])
                debug_info["message"] = "genus"

                if processed_line["need_verification"]:
                    debug_info["message"] = "genus"
                    self.unverified_lines.append(debug_info)

                elif processed_line["type"] == LineType.GENUS:
                    if current_genus:
                        # Save the previous genus and its species and start a new one.
                        self.species.append(current_genus)

                    current_genus = GenusDict(
                        genus=processed_line["value"],
                        pages=processed_line["pages"],
                        species=[],
                        debug=debug_info,
                    )

                elif processed_line["type"] == LineType.CONTINUATION:
                    current_genus["pages"].extend(processed_line["pages"])
                    current_genus["debug"]["texts"].extend(processed_line["text"])

                else:
                    logger.warning(f"Line is not a genus: {processed_line['text']}")
                    debug_info["message"] = "invalid line"
                    self.unverified_lines.append(debug_info)

            else:
                if self.debug:
                    cv.rectangle(
                        img_debug, (c_x, c_y), (c_x + c_w, c_y + c_h), (0, 255, 0), 2
                    )

                # Otherwise, it's a species, or continuation of previous genus.
                processed_line = self.process_line(
                    column[c_y : c_y + c_h, :], LineType.SPECIES
                )
                debug_info["texts"].append(processed_line["text"])
                debug_info["message"] = "species"

                if processed_line["need_verification"]:
                    debug_info["message"] = "species"
                    self.unverified_lines.append(debug_info)

                elif processed_line["type"] == LineType.SPECIES:
                    species = SpeciesDict(
                        species=processed_line["value"],
                        pages=processed_line["pages"],
                        debug=debug_info,
                    )
                    current_genus["species"].append(species)

                elif processed_line["type"] == LineType.CONTINUATION:
                    if current_genus["species"]:
                        species: SpeciesDict = current_genus["species"][-1]
                        species["pages"].extend(processed_line["pages"])
                        species["debug"]["texts"].extend(processed_line["text"])
                    else:
                        # This is a continuation of the previous genus
                        # (still need to verify).
                        current_genus["pages"].extend(processed_line["pages"])
                        current_genus["debug"]["texts"].extend(processed_line["text"])

                else:
                    logger.warning(
                        f"Line {idx+1} is not a species: {processed_line['text']}"
                    )
                    debug_info["message"] = "invalid line"
                    self.unverified_lines.append(debug_info)

        if self.debug:
            cv.imwrite(
                str(DEBUG_OUTPUT_PATH / f"{page_number:08}-{column_number}.png"),
                img_debug,
            )

    def process_line(self, line: np.ndarray, line_type: LineType) -> ProcessedLineDict:
        text: str = pytesseract.image_to_string(line, config="--psm 7")

        # Clean up the text.
        # Replace em-dashes with hyphens.
        # Remove left and right single quotes.
        line_cleaned = (
            text.strip()
            .replace("\u2014", "-")
            .replace("\u2018", "")
            .replace("\u2019", "")
        )

        processed_line = ProcessedLineDict(
            type=line_type,
            value=None,
            pages=[],
            text=line_cleaned,
            need_verification=False,
        )

        if not line_cleaned:
            processed_line["type"] = LineType.ERROR
            return processed_line

        # Double-check the given line type by looking at the first character.
        # Genus starts with an uppercase letter,
        # while species starts with a lowercase letter.
        first_character_verification = (
            line_cleaned[0].isupper()
            if line_type == LineType.GENUS
            else line_cleaned[0].islower()
        )

        if first_character_verification:
            # Found a genus or species.
            line_matches = re.search(
                r"(?P<value>[A-Za-z]+)[,.]?\s*(?P<rest>.*)", line_cleaned
            )
            if line_matches:
                processed_line["type"] = line_type
                processed_line["value"] = line_matches.group("value")
                rest = line_matches.group("rest")
                if rest:
                    processed_line["pages"] = re.findall(r"\d+", rest)

                return processed_line

        elif line_cleaned[0].isdigit():
            # The line is continuation of pages for the previous value.
            processed_line["type"] = LineType.CONTINUATION
            processed_line["pages"] = re.findall(r"\d+", line_cleaned)
            return processed_line

        elif line_cleaned[0].isalpha():
            processed_line["type"] = line_type
            processed_line["need_verification"] = True
            return processed_line

        processed_line["type"] = LineType.ERROR
        return processed_line

    def save_intermediate_images(
        self,
        page_number: int,
        img: np.ndarray,
        text_contour: np.ndarray,
        text_contour_bbox: Tuple[int, int, int, int],
    ):
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

    SpeciesProcessor(args.debug).process()
