# The capture service: joins Nextcloud Talk calls and streams their audio to a
# meeting gateway. It carries no recognition stack — none of that happens here.
#
#   docker build -t nextcloud-talk-capture .
FROM python:3.12-slim

# Runtime libraries aiortc links against. No compiler and no -dev packages:
# every dependency resolves to a manylinux wheel. If that stops being true the
# build fails here rather than quietly growing the image.
#
# The vpx package carries its soname in the name, so it moves with the base
# image (libvpx7 on bookworm, libvpx9 on trixie). A build failing with "Unable
# to locate package libvpxN" means exactly that; find the new one with
# `apt-cache search '^libvpx[0-9]'` inside the base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 \
    libvpx9 \
    libsrtp2-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# The service entry point is not part of the installed package: the package is
# the library, this is one way to run it.
COPY src/main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/main.py"]
