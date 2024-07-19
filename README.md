
# dbt-duck-flow

## Overview

`dbt-duck-flow` is a data pipeline project that leverages Python libraries such as `pandas` and `duckdb` to ingest data from various sources and upload it to your cloud storage provider (i.e. AWS S3, Azure, etc.). The data is then transformed and tested locally and exported using `dbt` (Data Build Tool) on top of duckdb. For the demonstration, I used Snowflake and AWS.

This project is designed to be a template for building data pipelines that can be easily extended to support additional data sources, transformations, and destinations. It is ideal for small to medium-sized data projects that require a flexible and scalable data pipeline.

## Dataset

The project uses the [Redfin Weekly Housing Market Dataset](https://www.redfin.com/news/data-center/) for demonstration purposes. The dataset contains information about housing markets in the United States, including property details, location, and pricing information. Redfin release a new up-to-date dataset each week (typically Thursdays). The data is available in CSV format and can be downloaded from the Redfin website.

## Analysis

The project includes a [jupyter notebook](https://github.com/galavera/dbt-duck-flow/blob/main/3_analysis/notebook/eda.ipynb) with some exploratory data analysis and visualizations of the housing market dataset. Can also see the notebook on Kaggle [here](https://www.kaggle.com/code/mikegalindo/housing-market-data-eda).

![image](https://imgur.com/wvXcJaH.png)

## Release Content
I have provided the parquet files generated from running the dbt models in the Releases section. You can download the parquet files and use them locally or in your data warehouse or data lake.

## Features

- **Data Ingestion:** Efficient data ingestion and manipulation using `pandas` and `duckdb`. Uses pydantic models with pytest alongside pyarrow for data type inference and schema validation.
- **Efficient Storage:** Exports ingested data as a parquet file for efficient storage and processing.
- **Data Transformation:** Utilizes `dbt` models to transform data.
- **Data Loading:** Tests and exports transformed data to DW or DL of choice (I used Snowflake and AWS for this project).

- **Makefile:** Provides a set of commands for running the pipeline, including data ingestion, transformation, and loading.

## Requirements

- Python 3.11
- `pandas`
- `duckdb`
- `pyarrow`
- `fire`
- `loguru`
- `pydantic`
- `python-dotenv`
- `tqdm`
- `dbt-duckdb`
- `snowflake-snowpark-python`
- `pytest` (for development)
- `ruff` (for development)

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/galavera/dbt-duck-flow.git
    cd dbt-duck-flow
    ```

2. Setup a virtual environment:

    ```sh
    python -m venv venv
    source venv/bin/activate
    ```

2. Install the required Python packages from pyproject.toml using Poetry:

    ```sh
    pip install poetry
    poetry install
    ```

## Configuration

1. **AWS Configuration:** Set up your AWS credentials to allow the script to upload data to your S3 bucket. This can be done by configuring the AWS CLI or setting environment variables.

2. **dbt Configuration:** Configure your `dbt` profiles. Refer to the [dbt documentation](https://docs.getdbt.com/docs/configure-your-profile) for setting up your profiles for different data warehouses or lakes.

## Usage

1. **Set Environment Variables:**

    Rename the `template.env` to `.env` in the root directory and add your environment variables.


2. **Ingest Data:**

    Use the provided Python scripts in the `ingestion` folder to ingest and export data.  

    Run `pipeline.py` using the following command:

    ```sh
    make rf-ingest
    ```

3. **Run dbt Models:**

    Run your `dbt` models to transform and load the data into your datawarehouse/datalake. Another option is to export to cloud storage and load the models into your database from cloud. 

    ```sh
    make rf-transform
    ```
    **example:**
    ![example](https://imgur.com/NeBCwdO.png)
    

## Project Structure

```
dbt-duck-flow/
├── ingestion
|     └── pipeline.py            # main ingestion script
|
├── transform/housing_market/    # dbt folder
|    └── macros/                 # macros
|     └── models/                # models
│                   
├── Makefile                     # Project Makefile                      
├── README.md                    # Project README
└── pyproject.toml               # Project configuration file

```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
