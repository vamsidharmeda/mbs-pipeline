# Mortgage Data Pipeline

## Overview

This project implements a robust and scalable data pipeline for processing mortgage data from various sources such as Freddie Mac, Fannie Mae, and Ginnie Mae. 
It uses Apache Spark for large-scale data processing, Google Cloud Storage (GCS) for data storage, and BigQuery for data warehousing.

## Features

- Asynchronous data scraping from mortgage data sources
- Dynamic schema definition using JSON configuration
- Configurable data transformations
- Scalable data processing with Apache Spark
- Integration with Google Cloud Storage and BigQuery
- Modular and extensible design
- Containerized deployment with Docker
- Kubernetes-ready for orchestration on EKS or GKE

## Prerequisites

- Docker
- Python 3.8+
- Poetry (Python package manager)
- Google Cloud Platform account with GCS and BigQuery enabled
- Service account with necessary permissions for GCS and BigQuery

## Local Development Setup

1. Clone the repository:
   ```
   git clone https://github.com/vamsidharmeda/mbs-pipeline.git
   cd mbs-pipeline
   ```

2. Install Poetry if you haven't already:
   ```
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. Install the project dependencies:
   ```
   poetry install
   ```

4. Set up your Google Cloud service account key:
   - Download your service account key JSON file

## Configuration

1. Update `settings.py` with your specific configuration.
2. Modify `config/{partner}/schema.json` to match your data schema.
3. Adjust `config/{partner}/schema_mapper.json` for your specific data transformation needs.

## Running with Docker

1. Build the Docker image:
   ```
   docker build -t mbs_pipeline .
   ```

2. Run the container:
   ```
   docker run -v /path/to/your/service-account-key.json:/app/service-account-key.json \
              -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
              mbs_pipeline
   ```

   Replace `/path/to/your/service-account-key.json` with the actual path to your service account key.

## Usage

For local development with Poetry:

```
poetry shell
python pipeline.py
```

## Project Structure

- `Dockerfile`: Defines the Docker image for the project
- `pyproject.toml`: Poetry configuration file defining project dependencies 
- mbs_pipeline/
  - `pipeline.py`: Main script orchestrating the entire pipeline
  - `scraper.py`: Contains logic for scraping data from various sources
  - `transform.py`: Handles data transformations
  - `load.py`: Manages loading data into Spark DataFrames
  - `writer.py`: Handles writing data to Parquet and BigQuery
  - `schema.py`: Manages dynamic schema creation
  - `enums.py`: Contains enums used throughout the project
  - `utils.py`: Generic utilities and helpers
- config/{partner}/
  - `pipeline_config.yaml`: All config related to the partner
  - `schema`: schema for the inputs
  - `schema_mapper`: logic to map each column from source -> target
- `settings.py`: Stores configuration settings
- `schema.json`: Defines the data schema
- `transformations.json`: Specifies data transformations

## Adding or Updating Dependencies

To add a new dependency:

```
poetry add package-name
```

To update dependencies:

```
poetry update
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.