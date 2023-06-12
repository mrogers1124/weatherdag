import logging
import requests
import json
import os
from datetime import datetime
from typing import List

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
