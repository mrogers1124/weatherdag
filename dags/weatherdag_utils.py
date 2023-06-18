import logging
import requests
import json
import os
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)


# Request observation data from a NWS station during a specified time period
# All datetime values assumed to be UTC
def get_from_nws_api(stationId: str, start: datetime, end: datetime) -> requests.Response:
    url = f'https://api.weather.gov/stations/{stationId}/observations'
    params = {'start': start.strftime('%Y-%m-%dT%H:%M:%SZ'), 'end': end.strftime('%Y-%m-%dT%H:%M:%SZ')}
    logger.info(f'Sending GET request to API endpoint {url} with parameters {params}')
    response = requests.get(url, params)
    logger.info(f'Received response, status code {response.status_code}')
    response.raise_for_status()
    return response


# Parse the API response, returning a list of observations as JSON formatted strings
def parse_nws_api_response(response: requests.Response) -> List[str]:
    logger.info(f'Extracting observation data from response')
    content_str = response.content.decode()
    content = json.loads(content_str)
    observations = [json.dumps(observation) for observation in content['features']]
    logger.info(f'Found {len(observations)} observations')
    return observations


# Save the list of observations to disk, to be ingested into database in next stage
def save_observations(observations: List[str], out_dir: str):
    # If observations is an empty list, skip this step
    if not observations:
        logger.info('No observations were returned, nothing to save')
        return

    if not os.path.isdir(out_dir):
        logger.info(f'Creating directory {out_dir}')
        os.makedirs(out_dir)

    pad_length = len(str(len(observations)-1))
    for n, observation in enumerate(observations):
        fn = f'observation_{n:0{pad_length}}.json'
        fp = os.path.join(out_dir, fn)
        logger.info(f'Saving data to {fp}')
        with open(fp, 'w') as f:
            f.write(observation)


def load_observations(in_dir: str, db_path: str, run_id: str, sys_load_time: str):
    # If in_dir does not exist, skip this step and return None
    if not os.path.isdir(in_dir):
        logger.info(f"Directory does not exist, skipping: {in_dir}")
        return

    # Get list of JSON files in in_dir
    files = [os.path.join(in_dir, fn) for fn in os.listdir(in_dir) if os.path.splitext(fn)[1] == '.json']

    # Create database connection
    logger.info(f"Opening SQLite database connection: {db_path}")
    with sqlite3.connect(db_path) as con:

        # Load raw data into db
        for fp in files:
            logger.info(f"Loading data from {fp} into raw layer")
            with open(fp, 'r') as file:
                observation = file.read()
                con.execute(
                    "INSERT INTO RAW_OBSERVATIONS(RUN_ID, SYS_LOAD_TIME, FILENAME, OBSERVATION) VALUES(?, ?, ?, ?)",
                    (run_id, sys_load_time, fp, observation)
                )

    # Close database connection
    logger.info("Closing database connection")
    con.close()


def create_or_update_db(db_path: str):
    # Create directory where SQLite database is saved, if it doesn't exist
    db_dir = os.path.split(db_path)[0]
    if not os.path.exists(db_dir):
        logger.info(f"Creating directory {db_dir}")
        os.makedirs(db_dir)

    sql_path = os.path.join(os.path.split(__file__)[0], 'weatherdag_ddl.sql')
    with open(sql_path, 'r') as f:
        logger.info(f"Reading DDL from file {sql_path}")
        ddl = f.read()

    logger.info(f"Opening SQLite database connection: {db_path}")
    with sqlite3.connect(db_path) as con:
        logger.info(f"Executing DDL")
        con.executescript(ddl)

    logger.info("Closing database connection")
    con.close()


def update_viz(db_path: str, viz_path: str):
    logger.info(f"Opening SQLits database connection: {db_path}")
    with sqlite3.connect(db_path) as con:
        logger.info("Querying observation properties from database")
        df = pd.read_sql_query("SELECT * FROM VW_PROPERTIES", con)

    logger.info("Closing database connection")
    con.close()

    if not os.path.isdir(viz_path):
        logger.info(f"Creating visualization directory: {viz_path}")
        os.makedirs(viz_path)

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])

    logger.info("Creating visualiation")
    fig, ax = plt.subplots(figsize=(8, 6))
    line_temp, = ax.plot(df["TIMESTAMP"], df["TEMPERATURE"])
    line_temp.set_label("Temperature")
    line_dew, = ax.plot(df["TIMESTAMP"], df["DEWPOINT"])
    line_dew.set_label("Dewpoint")
    ax.legend()

    plt.xlabel("Observation Date")
    plt.ylabel("Value (°C)")
    plt.title("Temperature and Dewpoint Observations")

    fig.autofmt_xdate()

    fp = os.path.join(viz_path, "fig.png")
    logger.info(f"Saving visualiation to {fp}")
    plt.savefig(fp)
