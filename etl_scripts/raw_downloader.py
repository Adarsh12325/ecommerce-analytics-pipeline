"""
Raw Data Ingestion Module for E-Commerce Batch Pipeline.

Fetches the raw external e-commerce dataset via HTTP/HTTPS, verifies HTTP status,
handles streaming chunk writes for memory efficiency, and stages the data in data/raw/.
"""

import logging
import os
import sys
import requests

# Configure structured logging with standard formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def download_raw_data(
    url: str,
    dest_path: str,
    timeout: int = 60,
    chunk_size: int = 1024 * 1024,
) -> None:
    """
    Download raw CSV dataset from a remote URL to the local raw storage layer.

    Args:
        url (str): The HTTP/HTTPS endpoint of the raw CSV dataset.
        dest_path (str): Destination filesystem path for saving the CSV.
        timeout (int): Socket connection/read timeout in seconds.
        chunk_size (int): Size of chunks to stream from response in bytes (1MB default).

    Raises:
        ValueError: If url or dest_path are empty or invalid.
        requests.exceptions.RequestException: If the HTTP request fails or status is not 200 OK.
        IOError: If saving to disk encounters permission or storage failure.
    """
    logger.info("================================================================================")
    logger.info("TASK START: download_raw_data")
    logger.info(f"Target URL: {url}")
    logger.info(f"Target Destination: {dest_path}")
    logger.info("================================================================================")

    if not url or not isinstance(url, str):
        err_msg = f"Invalid download URL provided: '{url}'"
        logger.error(err_msg)
        raise ValueError(err_msg)

    if not dest_path or not isinstance(dest_path, str):
        err_msg = f"Invalid destination path provided: '{dest_path}'"
        logger.error(err_msg)
        raise ValueError(err_msg)

    # Ensure parent destination directory exists
    dest_dir = os.path.dirname(os.path.abspath(dest_path))
    try:
        os.makedirs(dest_dir, exist_ok=True)
        logger.info(f"Verified/created destination directory: {dest_dir}")
    except OSError as e:
        logger.error(f"Failed to create directory {dest_dir}: {str(e)}", exc_info=True)
        raise

    # Execute HTTP GET request with streaming
    logger.info(f"Initiating HTTP GET request with {timeout}s timeout...")
    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            logger.info(f"Received HTTP response status code: {response.status_code}")
            # Raise exception immediately if HTTP status is 4xx or 5xx (Circuit Breaker)
            response.raise_for_status()

            # Stream chunks to local file
            total_bytes_written = 0
            temp_dest_path = f"{dest_path}.tmp"
            logger.info(f"Writing incoming stream to temporary buffer: {temp_dest_path}")

            with open(temp_dest_path, "wb") as f_out:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f_out.write(chunk)
                        total_bytes_written += len(chunk)

            # Atomic rename after successful full stream
            if os.path.exists(dest_path):
                logger.info(f"Overwriting pre-existing file at: {dest_path}")
                os.remove(dest_path)
            os.rename(temp_dest_path, dest_path)

            file_size_mb = total_bytes_written / (1024 * 1024)
            logger.info(f"Successfully downloaded {total_bytes_written:,} bytes ({file_size_mb:.2f} MB)")
            logger.info(f"Final dataset confirmed at: {dest_path}")
            logger.info("TASK COMPLETE: download_raw_data successfully finished.")

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP Error encountered during download: {http_err}", exc_info=True)
        if os.path.exists(f"{dest_path}.tmp"):
            os.remove(f"{dest_path}.tmp")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error while contacting URL '{url}': {conn_err}", exc_info=True)
        if os.path.exists(f"{dest_path}.tmp"):
            os.remove(f"{dest_path}.tmp")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout ({timeout}s) exceeded while contacting URL '{url}': {timeout_err}", exc_info=True)
        if os.path.exists(f"{dest_path}.tmp"):
            os.remove(f"{dest_path}.tmp")
        raise
    except Exception as general_err:
        logger.error(f"Unexpected error in download_raw_data: {general_err}", exc_info=True)
        if os.path.exists(f"{dest_path}.tmp"):
            os.remove(f"{dest_path}.tmp")
        raise


if __name__ == "__main__":
    # Test or standalone execution
    sample_url = os.getenv(
        "DATA_SOURCE_URL",
        "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv",
    )
    sample_dest = os.getenv("RAW_DATA_PATH", "data/raw/ecommerce_data.csv")
    download_raw_data(sample_url, sample_dest)
