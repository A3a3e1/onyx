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
    
    # --- THIS IS THE CRITICAL FIX ---
    # Get the ID values first
    assignee_id = ticket.get("admin_assignee_id")
    team_assignee_id = ticket.get("team_assignee_id")

    metadata = {
        "created_at": datetime.fromtimestamp(ticket["created_at"], tz=timezone.utc).isoformat(),
        "state": ticket.get("state"),
        # Convert IDs to strings if they exist
        "assignee_id": str(assignee_id) if assignee_id is not None else None,
        "team_assignee_id": str(team_assignee_id) if team_assignee_id is not None else None,
        "tags": [tag['name'] for tag in ticket.get("tags", {},).get("tags", [])],
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