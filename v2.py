"""
PADAP - Secure Agricultural Data Pipeline
==========================================
Automates the collection, processing, and export of agricultural
pricing data from a configured external API and CONAB (Brazilian
National Supply Company).

Setup
-----
1. Create a `.env` file in the project root with the variables below.
2. Never commit the `.env` file — add it to `.gitignore`.

Required environment variables::

    API_URL=https://your-api.com/data
    API_TOKEN=your_secret_token
    CONAB_URL=https://dados.conab.gov.br/file.txt

Dependencies
------------
Install via pip::

    pip install requests pandas python-dotenv
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------

load_dotenv()

API_URL: str | None = os.getenv("API_URL")
API_TOKEN: str | None = os.getenv("API_TOKEN")
CONAB_URL: str | None = os.getenv("CONAB_URL")

if not API_URL or not API_TOKEN:
    raise ValueError(
        "Required environment variables are not configured. "
        "Ensure API_URL and API_TOKEN are defined in the .env file."
    )


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def fetch_api_data() -> list[dict]:
    """Fetch data from the configured external API.

    Sends an authenticated GET request using the Bearer token defined
    in the environment. Raises an HTTPError if the request fails.

    Returns
    -------
    list[dict]
        Parsed JSON response body from the API.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx HTTP status code.
    """
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    response = requests.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_conab_data() -> str:
    """Download the CONAB weekly municipal pricing file.

    Retrieves the raw file from the URL defined in CONAB_URL and
    persists it locally for subsequent processing.

    Returns
    -------
    str
        Local file path where the downloaded content was saved.

    Raises
    ------
    requests.HTTPError
        If the CONAB endpoint returns a non-2xx HTTP status code.
    """
    response = requests.get(CONAB_URL, timeout=30)
    response.raise_for_status()

    file_path = "PrecosSemanalMunicipio.txt"
    with open(file_path, "wb") as f:
        f.write(response.content)

    return file_path


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------

def process_data(api_data: list[dict], conab_file: str) -> pd.DataFrame:
    """Transform and merge API and CONAB datasets.

    Performs the following steps:

    - Normalises column names to lowercase.
    - Parses the ``data`` column to ``datetime``, coercing invalid values.
    - Reads and normalises the CONAB CSV file (Latin-1, semicolon-delimited).
    - Merges both datasets on the ``municipio`` key when available.
    - Derives a ``valor_kg`` metric from the ``valor`` column when present.

    Parameters
    ----------
    api_data : list[dict]
        Raw JSON payload returned by :func:`fetch_api_data`.
    conab_file : str
        Local path to the CONAB pricing file returned by
        :func:`fetch_conab_data`.

    Returns
    -------
    pd.DataFrame
        Processed and (optionally) merged dataset ready for export.

    Notes
    -----
    The merge key ``municipio`` and the metric column ``valor`` are
    expected field names. Adjust these to match the actual schema of
    your data sources.
    """
    # Build DataFrame from API response and normalise column names
    df_api = pd.DataFrame(api_data)
    df_api.columns = [col.lower() for col in df_api.columns]

    # Parse date column if present
    if "data" in df_api.columns:
        df_api["data"] = pd.to_datetime(df_api["data"], errors="coerce")

    # Read CONAB file and normalise column names
    df_conab = pd.read_csv(conab_file, sep=";", encoding="latin1")
    df_conab.columns = [col.lower() for col in df_conab.columns]

    # Merge datasets on the municipal identifier when both contain the key
    if "municipio" in df_api.columns and "municipio" in df_conab.columns:
        df = pd.merge(df_api, df_conab, on="municipio", how="left")
    else:
        df = df_api.copy()

    # Derive price-per-kilogram metric
    if "valor" in df.columns:
        df["valor_kg"] = df["valor"] / 1000

    return df


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def save_outputs(df: pd.DataFrame) -> None:
    """Persist the processed dataset and derived reference tables to disk.

    Exports the following files to the ``output/`` directory:

    - ``data_tratada.csv``  — full processed dataset.
    - ``datas_tratadas.csv`` — deduplicated list of dates (if column exists).
    - ``municipios.csv``    — deduplicated list of municipalities (if column exists).

    Parameters
    ----------
    df : pd.DataFrame
        Processed dataset returned by :func:`process_data`.
    """
    os.makedirs("output", exist_ok=True)

    df.to_csv("output/data_tratada.csv", index=False)

    if "data" in df.columns:
        df[["data"]].drop_duplicates().to_csv(
            "output/datas_tratadas.csv", index=False
        )

    if "municipio" in df.columns:
        df[["municipio"]].drop_duplicates().to_csv(
            "output/municipios.csv", index=False
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Execute the full PADAP data pipeline.

    Orchestrates data collection from the external API and CONAB,
    applies transformation and merging logic, and writes the resulting
    datasets to the ``output/`` directory.
    """
    print("Starting PADAP pipeline...")

    api_data = fetch_api_data()
    conab_file = fetch_conab_data()
    df = process_data(api_data, conab_file)
    save_outputs(df)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
