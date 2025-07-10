# 1. Imports are organized and corrected
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Iterator

import requests

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import (
    GenerateDocumentsOutput,
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
)
# 2. Import ConnectorMissingCredentialError for robust error handling
from onyx.connectors.models import Document, TextSection, BasicExpertInfo, ConnectorMissingCredentialError
from onyx.utils.logger import setup_logger

logger = setup_logger()

# Defined as a module-level constant
BASE_URL = "https://api.intercom.io"
INTERCOM_ID_PREFIX = "intercom_"

class IntercomConnector(LoadConnector, PollConnector):
    def __init__(self, batch_size: int = INDEX_BATCH_SIZE):
        self.batch_size = batch_size
        self.intercom_api_token: Optional[str] = None

    def load_credentials(self, credentials: dict[str, Any]) -> None:
        # 3. Added robust credential validation, similar to FreshdeskConnector
        intercom_api_token = credentials.get("intercom_api_token")
        if not intercom_api_token or not isinstance(intercom_api_token, str):
            raise ConnectorMissingCredentialError("Missing or invalid 'intercom_api_token' for Intercom connector.")
        self.intercom_api_token = intercom_api_token

    ERROR:    07/10/2025 02:23:12 PM                run_indexing.py  665: [CC Pair: 4] [Index Attempt: 11] Connector run exceptioned after elapsed time: 8.401466180002899 seconds
Traceback (most recent call last):
  File "/app/onyx/background/indexing/run_indexing.py", line 479, in _run_indexing
    for document_batch, failure, next_checkpoint in connector_runner.run(
  File "/app/onyx/connectors/connector_runner.py", line 163, in run
    for document_batch in self.connector.poll_source(
  File "/app/onyx/connectors/intercom/connector.py", line 130, in poll_source
    yield from self._fetch_tickets(start_time=start_time)
  File "/app/onyx/connectors/intercom/connector.py", line 107, in _fetch_tickets
    doc_batch.append(self._ticket_to_document(ticket))
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/app/onyx/connectors/intercom/connector.py", line 65, in _ticket_to_document
    return Document(
           ^^^^^^^^^
  File "/usr/local/lib/python3.11/site-packages/pydantic/main.py", line 193, in __init__
    self.__pydantic_validator__.validate_python(data, self_instance=self)
pydantic_core._pydantic_core.ValidationError: 4 validation errors for Document
metadata.assignee_id.str
  Input should be a valid string [type=string_type, input_value=7843941, input_type=int]
    For further information visit https://errors.pydantic.dev/2.8/v/string_type
metadata.assignee_id.list[str]
  Input should be a valid list [type=list_type, input_value=7843941, input_type=int]
    For further information visit https://errors.pydantic.dev/2.8/v/list_type
metadata.team_assignee_id.str
  Input should be a valid string [type=string_type, input_value=645700, input_type=int]
    For further information visit https://errors.pydantic.dev/2.8/v/string_type
metadata.team_assignee_id.list[str]
  Input should be a valid list [type=list_type, input_value=645700, input_type=int]
    For further information visit https://errors.pydantic.dev/2.8/v/list_type


    def _get_tickets(self, starting_after: Optional[str] = None) -> Dict[str, Any]:
        if not self.intercom_api_token:
            raise ConnectorMissingCredentialError("Intercom API token is not loaded.")

        params = {"per_page": 50}
        if starting_after:
            params["starting_after"] = starting_after
        
        # 5. Fixed: Use module-level BASE_URL directly, not self.BASE_URL
        response = requests.get(f"{BASE_URL}/conversations", headers={
            "Authorization": f"Bearer {self.intercom_api_token}",
            "Accept": "application/json",
            "Intercom-Version": "2.9" # It's good practice to specify API version
        }, params=params)
        response.raise_for_status()
        return response.json()

    def _fetch_tickets(self, start_time: Optional[datetime] = None) -> GenerateDocumentsOutput:
        doc_batch: List[Document] = []
        starting_after = None
        
        while True:
            response = self._get_tickets(starting_after=starting_after)
            tickets = response.get("conversations", [])
            
            for ticket in tickets:
                updated_at = datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc)
                if start_time and updated_at < start_time:
                    # If results are sorted by update time desc, we could break here.
                    # Assuming we must iterate through all for safety.
                    continue
                
                doc_batch.append(self._ticket_to_document(ticket))

                if len(doc_batch) >= self.batch_size:
                    yield doc_batch
                    doc_batch = []
            
            # 6. Fixed: Safe dictionary access for pagination
            next_page_info = response.get("pages", {}).get("next")
            if next_page_info and "starting_after" in next_page_info:
                starting_after = next_page_info["starting_after"]
            else:
                break

        if doc_batch:
            yield doc_batch

    def load_from_state(self) -> GenerateDocumentsOutput:
        yield from self._fetch_tickets()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        start_time = datetime.fromtimestamp(start, tz=timezone.utc)
        yield from self._fetch_tickets(start_time=start_time)