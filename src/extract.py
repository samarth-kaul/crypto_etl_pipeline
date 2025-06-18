# import requests
# import logging
# from time import sleep

# def extract_data(api_config):
#     """Extract data from CoinGecko API."""
#     try:
#         url = f"{api_config['base_url']}{api_config['endpoint']}"
#         response = requests.get(url, params=api_config['params'], timeout=10)
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Failed to fetch data from API: {str(e)}")
#         raise


import requests
import logging
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataExtractor:
    """Handles data extraction from the CoinGecko API."""
    
    def __init__(self, api_config: Dict):
        self.base_url = api_config['base_url']
        self.endpoint = api_config['endpoint']
        self.params = api_config.get('params', {})
    
    def extract(self) -> List[Dict]:
        """Fetch data from the API."""
        url = f"{self.base_url}{self.endpoint}"
        try:
            response = requests.get(url, params=self.params, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully extracted {len(data)} records from {url}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to extract data: {e}")
            raise