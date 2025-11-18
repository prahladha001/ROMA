"""External Observer Event model for pluggable observability."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any


def _utc_now() -> datetime:
    """Get current UTC time with timezone awareness."""
    return datetime.now(timezone.utc)


@dataclass
class ExternalObserverEvent:
    """
    Event emitted to external observers for custom observability integrations.

    This allows pluggable observability without tight coupling to specific backends.
    Observers can subscribe via callback functions to receive events.
    """

    execution_id: str
    event_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_utc_now)
