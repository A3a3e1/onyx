from datetime import datetime, timezone
from typing import Any, Dict, List

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import (
    GenerateDocumentsOutput,
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
)
from onyx.connectors.models import Document, TextSection, BasicExpertInfo
from onyx.connectors.intercom.client import IntercomClient
from onyx.utils.logger import setup_logger

logger = setup_logger()


class IntercomConnector(LoadConnector, PollConnector):
    def __init__(self, batch_size: int = INDEX_BATCH_SIZE):
        self.batch_size = batch_size
        self._client: IntercomClient | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self._client = IntercomClient(api_key=credentials["intercom_api_token"])
        return None

    @property
    def client(self) -> IntercomClient:
        if self._client is None:
            raise Exception("Intercom client not initialized")
        return self._client

    def _ticket_to_document(self, ticket: Dict[str, Any]) -> Document:
        source = ticket.get("source", {})
        author = source.get("author", {})
        
        primary_owners = [BasicExpertInfo(display_name=author.get("name"), email=author.get("email"))] if author else []

        sections = [TextSection(text=source.get("body", ""))]
        
        if "conversation_parts" in ticket and ticket["conversation_parts"]["conversation_parts"]:
            for part in ticket["conversation_parts"]["conversation_parts"]:
                sections.append(TextSection(text=part.get("body", "")))

        return Document(
            id=f"intercom_{ticket['id']}",
            source=DocumentSource.INTERCOM,
            semantic_identifier=ticket.get("title") or f"Conversation {ticket['id']}",
            doc_updated_at=datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc),
            primary_owners=primary_owners,
            sections=sections,
            metadata={
                "created_at": datetime.fromtimestamp(ticket["created_at"], tz=timezone.utc).isoformat(),
                "state": ticket.get("state"),
                "assignee": ticket.get("assignee", {}).get("name"),
            },
        )

    def _fetch_tickets(self, start_time: datetime | None = None) -> GenerateDocumentsOutput:
        doc_batch: List[Document] = []
        starting_after = None
        while True:
            response = self.client.get_tickets(starting_after=starting_after)
            tickets = response.get("conversations", [])
            
            for ticket in tickets:
                updated_at = datetime.fromtimestamp(ticket["updated_at"], tz=timezone.utc)
                if start_time and updated_at < start_time:
                    continue
                
                doc_batch.append(self._ticket_to_document(ticket))

                if len(doc_batch) >= self.batch_size:
                    yield doc_batch
                    doc_batch = []

            if not response.get("pages", {}).get("next"):
                break
            starting_after = response["pages"]["next"]["starting_after"]

        if doc_batch:
            yield doc_batch

    def load_from_state(self) -> GenerateDocumentsOutput:
        yield from self._fetch_tickets()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        start_time = datetime.fromtimestamp(start, tz=timezone.utc)
        yield from self._fetch_tickets(start_time=start_time)
