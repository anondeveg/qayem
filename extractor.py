import os
import logging
import shutil
import uuid
import time
from pathlib import Path
import fitz  # PyMuPDF
from PIL import Image
from io import BytesIO

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Set the upload folder where files will be stored
highlight_dir = "highlights"
try:
    shutil.rmtree(highlight_dir)
except:
    pass
os.makedirs(highlight_dir, exist_ok=True)


class HighlightExtractor:
    def __init__(self, book_name: str, highlight_dir: str):
        # Create a unique identifier for each book
        self.temp_uuid = str(uuid.uuid4())
        self.output_dir = (
            Path(highlight_dir) / self.temp_uuid
        )  # Folder will be inside uploads with a unique UUID
        os.makedirs(self.output_dir, exist_ok=True)
        self.book_pdf_path = (
            self.output_dir / f"{book_name}.pdf"
        )  # Path to save the uploaded PDF
        self.creation_time = time.time()  # Track folder creation time

    def process_pdf(self, pdf_path: Path, first_page_image: Image) -> list:
        """Extract highlighted areas and combine with the first page image."""
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            logger.error(f"Error opening PDF file {pdf_path}: {str(e)}")
            return []

        highlight_images = []

        for page_num in range(len(doc)):
            try:
                page = doc.load_page(page_num)
            except Exception as e:
                logger.error(
                    f"Error loading page {page_num} from PDF {
                        pdf_path}: {str(e)}"
                )
                continue

            highlights = []  # Store highlight rectangles
            for annot in page.annots():
                rect = annot.rect
                if rect.width > 0 and rect.height > 0:
                    highlights.append(rect)

            if highlights:
                try:
                    ZOOM = 4.0
                    mat = fitz.Matrix(ZOOM, ZOOM)
                    pix = page.get_pixmap(matrix=mat)
                    img = Image.frombytes(
                        "RGB", [pix.width, pix.height], pix.samples)
                    img = img.convert("RGB")  # Ensure the image is in RGB mode
                except Exception as e:
                    logger.error(
                        f"Error generating image for page {page_num}: {str(e)}"
                    )
                    continue

                try:
                    # first_page_resized = first_page_image.resize(
                    #    (int((0.15 * img.width)), int((0.15 * img.height)))
                    # )
                    # put first page on the top left of the highlight
                    # img.paste(first_page_resized, (0, 0))
                    combined_image = img
                except Exception as e:
                    logger.error(
                        f"Error combining images for page {page_num}: {str(e)}"
                    )
                    continue

                # Saving the combined image to a file using a BytesIO buffer
                combined_image_filename = (
                    f"highlight_{page_num + 1}_{self.temp_uuid}.png"
                )
                combined_image_path = self.output_dir / combined_image_filename

                try:
                    image_buffer = BytesIO()
                    combined_image.save(image_buffer, format="PNG")
                    image_buffer.seek(0)

                    # Write the image buffer content to the file
                    with open(str(combined_image_path), "wb") as f:
                        f.write(image_buffer.read())
                except Exception as e:
                    logger.error(
                        f"Error saving image {
                            combined_image_filename}: {str(e)}"
                    )
                    continue
                p = (str(combined_image_path.absolute()), page_num)
                print(p)
                highlight_images.append(p)

        return highlight_images

    def get_first_page_image(self, pdf_path: Path) -> Image:
        """Extracts the first page as an image."""
        try:
            doc = fitz.open(pdf_path)
            first_page = doc.load_page(0)
            pix = first_page.get_pixmap(dpi=500)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img = img.convert("RGB")  # Ensure the image is in RGB mode
            return img
        except Exception as e:
            logger.error(
                f"Error extracting the first page image from {
                    pdf_path}: {str(e)}"
            )
            raise


def extract_highlights_from_pdf(pdf_path: str) -> list:
    """Main function to process PDF file and extract highlights as images.
    returns highlights path in a list"""
    book_name = Path(pdf_path).stem
    extractor = HighlightExtractor(book_name, highlight_dir)

    # Save the PDF to the unique folder
    pdf_path = Path(pdf_path)
    try:
        first_page_image = extractor.get_first_page_image(pdf_path)
        highlight_images = extractor.process_pdf(pdf_path, first_page_image)
    except Exception as e:
        logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
        return

    if not highlight_images:
        logger.warning(f"No highlighted areas found in the PDF {pdf_path}")
        return

    # Return the paths of the images as a list
    logger.info(f"Highlight images saved to: {extractor.output_dir}")
    return highlight_images


if __name__ == "__main__":
    shutil.rmtree(highlight_dir)
    pdf_file_path = input("Enter the path to the PDF file: ")
    extracted_images = extract_highlights_from_pdf(pdf_file_path)
    if extracted_images:
        print(
            f"Highlight images saved in folder: {
                str(Path(highlight_dir) / Path(pdf_file_path).stem)}"
        )
        for image in extracted_images:
            print(f"Saved: {image}")
    else:
        print("No highlight images were extracted.")
