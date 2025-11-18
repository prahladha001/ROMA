"""UI Event model for frontend visualization."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any


def _utc_now() -> datetime:
    """Get current UTC time with timezone awareness."""
    return datetime.now(timezone.utc)


@dataclass
class UIEvent:
    """Simple UI event for streaming to frontend."""

    execution_id: str
    event_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_utc_now)
