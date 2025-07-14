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
from onyx.connectors.models import (
    Document,
    TextSection,
    BasicExpertInfo,
    ConnectorMissingCredentialError,
)
# Import the HTML parsing utility
from onyx.file_processing.html_utils import parse_html_page_basic
from onyx.utils.logger import setup_logger

logger = setup_logger()

# Module-level constants for configuration and clarity
BASE_URL = "https://api.intercom.io"
INTERCOM_ID_PREFIX = "intercom_"
APP_URL_PREFIX = "https://app.intercom.com/a/apps/"


class IntercomConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        batch_size: int = INDEX_BATCH_SIZE,
        workspace_id: Optional[str] = None,
    ):
        self.batch_size = batch_size
        self.intercom_api_token: Optional[str] = None
        self.workspace_id = workspace_id

    def load_credentials(self, credentials: dict[str, Any]) -> None:
        """
        Loads and validates the API token from the credentials dictionary.
        """
        intercom_api_token = credentials.get("intercom_api_token")
        if not intercom_api_token or not isinstance(intercom_api_token, str):
            raise ConnectorMissingCredentialError(
                "Missing or invalid 'intercom_api_token' for Intercom connector."
            )
        self.intercom_api_token = intercom_api_token

        self.workspace_id = credentials.get("workspace_id")
        if not self.workspace_id:
            raise ConnectorMissingCredentialError(
                "Missing or invalid 'workspace_id' for Intercom connector."
            )

    def _ticket_to_document(self, ticket: Dict[str, Any]) -> Document:
        """
        Transforms a single Intercom ticket (conversation) into a Document object.
        """
        source = ticket.get("source", {})
        author = source.get("author", {})

        # Create primary_owners list only if an author with an email exists
        primary_owners = (
            [BasicExpertInfo(display_name=author.get("name"), email=author.get("email"))]
            if author and author.get("email")
            else []
        )

        # Combine the initial message and all subsequent parts into sections
        sections = []
        # Use the parser to clean the HTML from the ticket body
        if source.get("body"):
            cleaned_text = parse_html_page_basic(source["body"])
            if cleaned_text:
                sections.append(TextSection(text=cleaned_text))

        conversation_parts = ticket.get("conversation_parts", {}).get(
            "conversation_parts", []
        )
        for part in conversation_parts:
            # Use the parser here as well for all subsequent conversation parts
            if part.get("body"):
                cleaned_text = parse_html_page_basic(part["body"])
                if cleaned_text:
                    sections.append(TextSection(text=cleaned_text))

        # Get and convert numeric IDs to strings to prevent validation errors
        assignee_id = ticket.get("admin_assignee_id")
        team_assignee_id = ticket.get("team_assignee_id")

        metadata = {
            "created_at": datetime.fromtimestamp(
                ticket["created_at"], tz=timezone.utc
            ).isoformat(),
            "state": ticket.get("state"),
            "assignee_id": str(assignee_id) if assignee_id is not None else None,
            "team_assignee_id": str(team_assignee_id)
            if team_assignee_id is not None
            else None,
            "tags": [tag["name"] for tag in ticket.get("tags", {}).get("tags", [])],
            "priority": ticket.get("priority", "not_prioritized"),
            "source_type": source.get("type"),
        }

        return Document(
            id=f"{INTERCOM_ID_PREFIX}{ticket['id']}",
            source=DocumentSource.INTERCOM,
            semantic_identifier=ticket.get("title") or f"Conversation {ticket['id']}",
            link=self.get_source_link(ticket["id"]),
            doc_updated_at=datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc),
            primary_owners=primary_owners,
            sections=sections,
            # Clean up metadata by removing keys with None or empty values
            metadata={k: v for k, v in metadata.items() if v is not None and v != []},
        )

    def _get_tickets(self, starting_after: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches a single page of conversations from the Intercom API.
        """
        if not self.intercom_api_token:
            raise ConnectorMissingCredentialError("Intercom API token is not loaded.")

        params = {"per_page": 50}
        if starting_after:
            params["starting_after"] = starting_after

        response = requests.get(
            f"{BASE_URL}/conversations",
            headers={
                "Authorization": f"Bearer {self.intercom_api_token}",
                "Accept": "application/json",
                "Intercom-Version": "2.9",  # Specify API version for stability
            },
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def _fetch_tickets(
        self, start_time: Optional[datetime] = None
    ) -> GenerateDocumentsOutput:
        """
        Continuously fetches batches of tickets from Intercom, handling pagination.
        """
        doc_batch: List[Document] = []
        starting_after = None

        while True:
            response = self._get_tickets(starting_after=starting_after)
            tickets = response.get("conversations", [])

            for ticket in tickets:
                updated_at = datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc)
                if start_time and updated_at < start_time:
                    continue

                doc_batch.append(self._ticket_to_document(ticket))

                if len(doc_batch) >= self.batch_size:
                    yield doc_batch
                    doc_batch = []

            # Safely get the next page cursor
            next_page_info = response.get("pages", {}).get("next")
            if next_page_info and "starting_after" in next_page_info:
                starting_after = next_page_info["starting_after"]
            else:
                break  # No more pages

        if doc_batch:
            yield doc_batch

    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Loads all documents from the source.
        """
        yield from self._fetch_tickets()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        """
        Polls the source for documents updated since the start time.
        """
        start_time = datetime.fromtimestamp(start, tz=timezone.utc)
        yield from self._fetch_tickets(start_time=start_time)

    def get_source_link(self, doc_id: str, **kwargs: Any) -> Optional[str]:
        if not self.workspace_id:
            return None

        # doc_id from the index has a prefix, remove it for the URL
        conversation_id = doc_id.replace(INTERCOM_ID_PREFIX, "")
        return f"{APP_URL_PREFIX}{self.workspace_id}/conversations/{conversation_id}"