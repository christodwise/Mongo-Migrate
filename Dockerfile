# Use a stable Debian Bookworm based image
FROM python:3.11-slim-bookworm

# Install dependencies and MongoDB Database Tools directly
RUN apt-get update && apt-get install -y wget && \
    ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then \
    TOOLS_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-arm64-100.14.0.deb"; \
    else \
    TOOLS_URL="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.14.0.deb"; \
    fi && \
    wget -q $TOOLS_URL -O tools.deb && \
    apt-get install -y ./tools.deb && \
    rm tools.deb && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Make port 5001 available to the world outside this container
EXPOSE 5001

# Run the application
CMD ["python", "app.py"]
