import requests
from typing import Any, Dict, List, Optional

class IntercomClient:
    BASE_URL = "https://api.intercom.io"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_tickets(self, starting_after: Optional[str] = None) -> Dict[str, Any]:
        params = {"per_page": 50}
        if starting_after:
            params["starting_after"] = starting_after
        
        response = requests.get(f"{self.BASE_URL}/conversations", headers=self._headers, params=params)
        response.raise_for_status()
        return response.json()
