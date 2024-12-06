# Here's a Python script that extracts JSON data from the specified URLs using the `requests` library:
'''
This script:

1. Creates a `TabDataExtractor` class to handle the data extraction process.
2. Uses the `requests` library to fetch JSON data from the specified URLs.
3. Includes error handling for network requests and file operations.
4. Saves the extracted data to a JSON file.
5. Provides a summary of the extracted data.

To use this script, you'll need to install the `requests` library first:

```bash
pip install requests
```

The script will:
- Fetch data from all three endpoints
- Save the combined data to a file named "tab_data.json"
- Print a summary of the extracted data

Features:
- Error handling for network requests
- Type hints for better code readability
- Follows black formatting style
- Modular design with separate methods for different functionalities
- Easy to extend for additional endpoints

The output JSON file will contain a dictionary with three keys ("schedule", "odds", "results"), each containing the respective JSON data from the endpoints.

Note: Make sure you have proper permissions to access these URLs and comply with any terms of service or rate limiting requirements from the API provider.
'''

import requests
import json
from typing import Dict, Optional

class TabDataExtractor:
    def __init__(self):
        self.base_url = "https://json.tab.co.nz"
        self.endpoints = {
            "schedule": "/schedule/",
            "odds": "/odds/",
            "results": "/results/",
        }

    def fetch_json_data(self, url: str) -> Optional[Dict]:
        """
        Fetch JSON data from the specified URL.

        Args:
            url (str): The URL to fetch data from

        Returns:
            Optional[Dict]: JSON response data or None if request fails
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {str(e)}")
            return None

    def get_all_data(self) -> Dict[str, Optional[Dict]]:
        """
        Fetch data from all endpoints.

        Returns:
            Dict[str, Optional[Dict]]: Dictionary containing data from all endpoints
        """
        data = {}
        for endpoint_name, endpoint_path in self.endpoints.items():
            url = self.base_url + endpoint_path
            data[endpoint_name] = self.fetch_json_data(url)
        return data
    
    def get_odds_data(self) -> Dict[str, Optional[Dict]]:
        """
        Fetches on odds endpoint data
        
        Returns:
            Dict[str, Optional[Dict]]: Dictionary containing data from odds endpoint
        """
        data = {}
        url = self.base_url + self.endpoints["odds"]
        data = self.fetch_json_data(url)
        return data

    def get_results_data(self) -> Dict[str, Optional[Dict]]:
        """
        Fetches on results endpoint data
        
        Returns:
            Dict[str, Optional[Dict]]: Dictionary containing data from results endpoint
        """
        data = {}
        url = self.base_url + self.endpoints["results"]
        data = self.fetch_json_data(url)
        return data
    
    def combine_data(self, data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[Dict]]:
        """
        Combine data from all endpoints into a single dictionary.

        Args:
            data (Dict[str, Optional[Dict]]): Data to combine

        Returns:
            Dict: Combined data
        """
        combined_data = {}
        for endpoint_name, endpoint_data in data.items():
            if endpoint_data is not None:
                combined_data[endpoint_name] = endpoint_data
        return combined_data

    def save_to_file(self, data: Dict[str, Optional[Dict]], filename: str) -> None:
        """
        Save the extracted data to a JSON file.

        Args:
            data (Dict[str, Optional[Dict]]): Data to save
            filename (str): Name of the output file
        """
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print(f"Data successfully saved to {filename}")
        except IOError as e:
            print(f"Error saving data to file: {str(e)}")

def main():
    # Create an instance of TabDataExtractor
    extractor = TabDataExtractor()

    # Fetch data from all endpoints
    print("Fetching data from endpoints...")
    data = extractor.get_all_data()

    # Save the data to a JSON file
    output_file = "tab_data.json"
    extractor.save_to_file(data, output_file)

    # Print summary of extracted data
    for endpoint, endpoint_data in data.items():
        if endpoint_data is not None:
            print(f"\nData retrieved from {endpoint} endpoint:")
            print(f"Number of items: {len(endpoint_data)}")
        else:
            print(f"\nFailed to retrieve data from {endpoint} endpoint")


if __name__ == "__main__":
    main()
