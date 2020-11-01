from pathlib import Path

import pdf2image
import pytesseract


def process_pdf(pdf_file: Path, output_path: Path):
    for parent_path in output_path.parents:
        if not parent_path.exists():
            parent_path.mkdir()

    if not output_path.exists():
        output_path.mkdir()
        (output_path / Path("images")).mkdir()
        (output_path / Path("texts")).mkdir()
        print(f"Created '{output_path}'")

    print(f"Processing {pdf_file}...")
    images = pdf2image.convert_from_path(pdf_file)
    print(f"Converted {pdf_file} to image")

    for idx, image in enumerate(images):
        image.save(f"{output_path}/images/{idx+1}.png")
        print(f"Saved {idx+1}.png")
        text = pytesseract.image_to_string(image)
        with open(f"{output_path}/texts/{idx+1}.txt", "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved {idx+1}.txt")


if __name__ == "__main__":
    process_pdf(
        Path("./inputs/Challenger Summary part 1.pdf"), Path("./outputs/part1/")
    )
    process_pdf(
        Path("./inputs/Challenger Summary part 2.pdf"), Path("./outputs/part2/")
    )
