"""
Extract all the species mentioned in the summary report index.
The index contains a list of all genus, their species, and all the pages they are
mentioned in the summary reports.
"""
import argparse
import logging
import pathlib

import cv2 as cv
import numpy as np

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--debug",
    action="store_true",
    help="Saves processed images in `data/tmp` for visual inspection",
)

DATA_PATH = pathlib.Path("../data/HathiTrust/sec.6 v.2/images/")

DEBUG_OUTPUT_PATH = pathlib.Path("../data/tmp")
if not DEBUG_OUTPUT_PATH.exists():
    DEBUG_OUTPUT_PATH.mkdir()


INDEX_PAGES = range(739, 849)


def get_contour_extremities(contour):
    leftmost = tuple(contour[contour[:, :, 0].argmin()][0])
    topmost = tuple(contour[contour[:, :, 1].argmin()][0])
    bottommost = tuple(contour[contour[:, :, 1].argmax()][0])
    rightmost = tuple(contour[contour[:, :, 0].argmax()][0])
    return leftmost, topmost, bottommost, rightmost


def main(debug=False):
    for page_number in INDEX_PAGES:
        img_path = DATA_PATH / f"{page_number:08}.png"

        img = cv.imread(str(img_path))
        h, w, _ = img.shape

        # Remove part of the white space on the edges.
        img_cropped = img[350 : h - 350, 200 : w - 150]
        h_cropped, w_cropped, _ = img_cropped.shape

        # Detect edges.
        img_canny = cv.Canny(img_cropped, 100, 200)

        # Dilate the objects with a wide kernel (W: 65, H: 20)
        # to turn the main text on the page into one big blob.
        img_dilated = cv.dilate(
            img_canny, cv.getStructuringElement(cv.MORPH_RECT, (65, 20)), iterations=1
        )

        # Find the contours on the dilated image.
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
        text_contour = img_contours[max_area_idx]  # The main text blob

        br_x, br_y, br_width, br_height = cv.boundingRect(text_contour)

        # Crop the image to the main text blob.
        img_text = img_cropped[br_y : br_y + br_height, br_x : br_x + br_width]

        # Find the separating lines between columns.

        # Detect the edges on img_text.
        img_text_canny = cv.Canny(img_text, 100, 200)

        # Dilate the objects with a narrow and tall kernel (W: 1, H: 30)
        # to separate the columns and the lines in between them.
        img_text_dilated = cv.dilate(
            img_text_canny,
            cv.getStructuringElement(cv.MORPH_RECT, (1, 30)),
            iterations=1,
        )

        # Find the contours on the dilated img_text. The bottom 100 pixels are cropped
        # to avoid merging of some texts at the bottom into the columns.
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
            logger.warning(f"Less than 2 separator lines found on page {page_number}")
            continue

        # Use the slope between the top most and bottom most points of
        # the separator line contours to determine the orientation of the image.
        angles = []  # holds two angles for the two separator lines.
        separator_lines = []  # holds the two separator lines.
        for line in separator_line_contours:
            (_, topmost, bottommost, _) = get_contour_extremities(line)
            angles.append(
                -90
                + np.degrees(
                    np.arctan2(bottommost[1] - topmost[1], bottommost[0] - topmost[0])
                )
            )
            separator_lines.append((topmost, bottommost))

        # Rotate the image by mean of the two angles.
        rotation_matrix = cv.getRotationMatrix2D(
            (br_width // 2, br_height // 2), np.mean(angles), 1
        )
        img_text_rotated = cv.warpAffine(
            img_text, rotation_matrix, (br_width, br_height)
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
                        np.dot(rotation_matrix[:, :2], coordinate)
                        + rotation_matrix[:, 2]
                    ).astype(int)
                )
            separator_lines_rotated.append(coordinates)

        # Crop the columns from the image.
        (left_line, right_line) = separator_lines_rotated
        first_column = img_text_rotated[:, 0 : max(left_line[0][0], left_line[1][0])]
        second_column = img_text_rotated[
            :,
            min(left_line[0][0], left_line[1][0]) : max(
                right_line[0][0], right_line[1][0]
            ),
        ]
        third_column = img_text_rotated[:, min(right_line[0][0], right_line[1][0]) :]

        cv.imwrite(str(DEBUG_OUTPUT_PATH / f"{page_number:08}-01.png"), first_column)
        cv.imwrite(str(DEBUG_OUTPUT_PATH / f"{page_number:08}-02.png"), second_column)
        cv.imwrite(str(DEBUG_OUTPUT_PATH / f"{page_number:08}-03.png"), third_column)

        if debug:
            img_debug = img_cropped.copy()

            # Draw the bounding rectangle around the main text blob (red).
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
            (leftmost, topmost, bottommost, rightmost) = get_contour_extremities(
                text_contour
            )
            cv.circle(img_debug, leftmost, 15, (255, 0, 0), -1)
            cv.circle(img_debug, rightmost, 15, (255, 0, 0), -1)
            cv.circle(img_debug, topmost, 15, (255, 0, 0), -1)
            cv.circle(img_debug, bottommost, 15, (255, 0, 0), -1)

            cv.imwrite(str(DEBUG_OUTPUT_PATH / f"{page_number:08}.png"), img_debug)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.debug)
