FROM debian:bookworm-slim AS builder

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    libgdal-dev \
    libgeos-dev \
    python3 \
    python3-venv \
    python3-pip \
    --fix-missing

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    CMAKE_PREFIX_PATH='/usr/include/gdal;/usr/include/geos'

# Create a virtual environment, which allows poetry itself to be installed
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Poetry configuration
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

RUN pip install poetry==1.8.3

# Set working directory
WORKDIR /app

# Install dependencies using poetry
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --without dev && rm -rf $POETRY_CACHE_DIR

# The runtime image, used to just run the code provided its virtual environment
FROM python:3.11-slim-bullseye AS runtime

# Install GDAL runtime libraries
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal28 \
    libgeos-c1v5 \
    --fix-missing

# Copy the virtual environment from the builder stage to the runtime stage
COPY --from=builder /app/.venv /app/.venv

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/.venv/lib/python3.11/site-packages"

# Set working directory
WORKDIR /app

COPY run.py \
    pipeline.py \
    process_climate.py \ 
    generate_geotiff.py \
    utils.py \
    infra_intersection.py \
    infra_intersection_load.py ./

CMD ["/bin/bash", "-c", "source /app/.venv/bin/activate && python run.py"]