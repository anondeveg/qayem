# Use official lightweight Python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    tesseract-ocr \
    tesseract-ocr-ara \
    tesseract-ocr-eng \
    # OpenCV dependencies
    libgl1-mesa-glx \
    libglib2.0-0 \
    # Cleanup
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch CPU-only first to prevent downloading the massive CUDA version (conserves image size)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

# Copy requirements file
COPY requirements.txt /app/

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download and cache EasyOCR model weights (Arabic and English) inside the image
# This avoids run-time downloads and allows completely offline container usage
RUN python3 -c "import easyocr; easyocr.Reader(['ar', 'en'])"

# Copy the rest of the application files
COPY . /app/

# Create folders for uploads and highlights
RUN mkdir -p /app/uploads /app/highlights

# Expose Web UI port
EXPOSE 5000

# Start Flask server
CMD ["python3", "app.py"]
