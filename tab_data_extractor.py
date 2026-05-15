"""TAB Affiliates v1 data extractor.

Migrated 2026-05-15 from legacy json.tab.co.nz endpoints (retired when Entain
acquired TAB NZ and moved it onto the Neds platform). Source endpoints now
live at https://api.tab.co.nz/affiliates/v1/ — public, unauthenticated, with a
30-second cache.
"""

import json
import requests
from typing import Dict, Optional

USER_AGENT = "Mozilla/5.0"


class TabDataExtractor:
    def __init__(self):
        self.base_url = "https://api.tab.co.nz"
        self.endpoints = {
            "schedule": "/affiliates/v1/racing/meetings",
            "event": "/affiliates/v1/racing/events/{race_id}",
        }
        self._headers = {"User-Agent": USER_AGENT}

    def fetch_json_data(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Fetch JSON from URL with User-Agent header. Returns None on error."""
        try:
            response = requests.get(url, headers=self._headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {str(e)}")
            return None

    @staticmethod
    def _unwrap(payload: Optional[Dict]) -> Optional[Dict]:
        """Strip the Affiliates v1 envelope and return inner `data` dict."""
        if payload is None:
            return None
        return payload.get("data")

    def get_schedule_data(self, date: str = "today") -> Optional[Dict]:
        """List all race meetings for `date` (default today). Returns dict with `meetings`."""
        url = self.base_url + self.endpoints["schedule"]
        return self._unwrap(self.fetch_json_data(url, params={"date": date}))

    def get_event_data(self, race_id: str) -> Optional[Dict]:
        """Fetch a single race event (runners + odds + results)."""
        url = self.base_url + self.endpoints["event"].format(race_id=race_id)
        return self._unwrap(self.fetch_json_data(url))

    def save_to_file(self, data: Dict, filename: str) -> None:
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print(f"Data successfully saved to {filename}")
        except IOError as e:
            print(f"Error saving data to file: {str(e)}")


def main():
    extractor = TabDataExtractor()
    schedule = extractor.get_schedule_data()
    if schedule is None:
        print("Schedule fetch failed")
        return
    meetings = schedule.get("meetings", [])
    print(f"Meetings today: {len(meetings)}")
    if meetings and meetings[0].get("races"):
        race_id = meetings[0]["races"][0]["id"]
        event = extractor.get_event_data(race_id)
        print(f"Event runners: {len(event.get('runners', [])) if event else 'n/a'}")


if __name__ == "__main__":
    main()
