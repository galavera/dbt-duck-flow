# Stage 1: Base
ARG PLATFORM=amd64
FROM --platform=Linux/${PLATFORM} python:3.11

# Set working directory
WORKDIR /workspaces/dbt-duckdb

# Install dependencies and tools
RUN apt-get update && apt-get install -y vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install snowflake-snowpark-python poetry --no-cache-dir

# Default command --runs when opening project
#CMD ["python", "your_script.py"]
CMD ["sleep", "infinity"]