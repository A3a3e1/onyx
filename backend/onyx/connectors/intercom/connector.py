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

    def _ticket_to_document(self, ticket: Dict[str, Any]) -> Document:
        source = ticket.get("source", {})
        author = source.get("author", {})
    
        primary_owners = [BasicExpertInfo(display_name=author.get("name"), email=author.get("email"))] if author and author.get("email") else []

        sections = []
        if source.get("body"):
            sections.append(TextSection(text=source["body"]))
        
        conversation_parts = ticket.get("conversation_parts", {}).get("conversation_parts", [])
        for part in conversation_parts:
            if part.get("body"):
                sections.append(TextSection(text=part["body"]))
        
        # --- FIX IS HERE ---
        # Get the ID values first
        assignee_id = ticket.get("admin_assignee_id")
        team_assignee_id = ticket.get("team_assignee_id")

        metadata = {
            "created_at": datetime.fromtimestamp(ticket["created_at"], tz=timezone.utc).isoformat(),
            "state": ticket.get("state"),
            # Convert IDs to strings if they exist
            "assignee_id": str(assignee_id) if assignee_id is not None else None,
            "team_assignee_id": str(team_assignee_id) if team_assignee_id is not None else None,
            "tags": [tag['name'] for tag in ticket.get("tags", {}).get("tags", [])],
            "priority": ticket.get("priority", "not_prioritized"),
            "source_type": source.get("type"),
        }

        return Document(
            id=f"{INTERCOM_ID_PREFIX}{ticket['id']}",
            source=DocumentSource.INTERCOM,
            semantic_identifier=ticket.get("title") or f"Conversation {ticket['id']}",
            doc_updated_at=datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc),
            primary_owners=primary_owners,
            sections=sections,
            metadata={k: v for k, v in metadata.items() if v is not None and v != []},
        )

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