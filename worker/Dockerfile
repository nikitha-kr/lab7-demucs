FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1
ENV PATH="/usr/src/app/.local/bin:$PATH"

# Install system dependencies
# ffmpeg is required by Demucs to process MP3 files
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /usr/src/app

# Install python dependencies. demucs pulls in PyTorch/TensorFlow, which is massive.
# This step will take the longest amount of time during the build.
RUN pip install --no-cache-dir \
    demucs \
    redis \
    minio \
    jsonpickle \
    requests

# Copy the worker script
COPY worker-server.py .

# Command to run the worker loop
CMD ["python3", "worker-server.py"]

