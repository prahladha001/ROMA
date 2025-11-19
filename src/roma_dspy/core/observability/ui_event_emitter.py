"""UI Event Emitter for streaming events to PostgreSQL."""

from typing import Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime, timezone
from loguru import logger
import asyncio

if TYPE_CHECKING:
    from roma_dspy.core.storage.postgres_storage import PostgresStorage


class UIEventEmitter:
    """
    Emits UI events to PostgreSQL for frontend streaming.

    This is completely separate from EventScheduler (task orchestration).
    Purely for UI visualization - writes events directly to PostgreSQL.

    Features:
    - Synchronous emit_sync() for synchronous contexts (DAG operations)
    - Asynchronous emit() for async contexts
    - Direct write to PostgreSQL (no queue)

    Usage:
        emitter = UIEventEmitter(postgres_storage)
        emitter.emit_sync(execution_id="abc", event_type="my_event", data={...})
        # or
        await emitter.emit(execution_id="abc", event_type="my_event", data={...})
    """

    def __init__(
        self,
        postgres_storage: Optional["PostgresStorage"] = None,
        enabled: bool = True
    ):
        """
        Initialize UI event emitter.

        Args:
            postgres_storage: PostgreSQL storage instance
            enabled: Whether to emit events (default: True)
        """
        self._storage = postgres_storage
        self._enabled = enabled and postgres_storage is not None

    def emit_sync(
        self,
        execution_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Emit a UI event synchronously (from non-async code).

        This blocks until the event is written to PostgreSQL to preserve order.

        Args:
            execution_id: Unique execution identifier
            event_type: Event type (user-defined string)
            data: Event data (any JSON-serializable dict)

        Example:
            emitter.emit_sync(
                execution_id="abc-123",
                event_type="node_added",
                data={"task_id": "xyz", "goal": "Process data"}
            )
        """
        if not self._enabled:
            logger.debug(f"UI event emitter disabled, skipping '{event_type}'")
            return

        try:
            # Try to get the running event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context with a running loop
                # Create task without blocking (events may arrive slightly out of order)
                logger.debug(f"Scheduling UI event '{event_type}' in existing event loop")
                loop.create_task(self._save_event(execution_id, event_type, data or {}))
            except RuntimeError:
                # No event loop running, create one and run (blocks)
                logger.debug(f"Creating new event loop for UI event '{event_type}'")
                asyncio.run(self._save_event(execution_id, event_type, data or {}))
        except Exception as e:
            logger.warning(f"Failed to emit UI event '{event_type}': {e}")

    async def emit(
        self,
        execution_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Emit a UI event asynchronously.

        This writes the event directly to PostgreSQL.

        Args:
            execution_id: Unique execution identifier
            event_type: Event type (user-defined string)
            data: Event data (any JSON-serializable dict)

        Example:
            await emitter.emit(
                execution_id="abc-123",
                event_type="task_started",
                data={"task_id": "xyz", "message": "Starting task"}
            )
        """
        if not self._enabled:
            return

        try:
            await self._save_event(execution_id, event_type, data or {})
        except Exception as e:
            logger.debug(f"Failed to emit UI event '{event_type}': {e}")

    async def _save_event(
        self,
        execution_id: str,
        event_type: str,
        data: Dict[str, Any]
    ) -> None:
        """Save event to PostgreSQL."""
        try:
            # Ensure PostgresStorage is initialized for this async context
            await self._storage.initialize()

            logger.debug(f"Saving UI event '{event_type}' to PostgreSQL for execution: {execution_id}")
            await self._storage.save_ui_event(
                execution_id=execution_id,
                event_type=event_type,
                data=data,
                timestamp=datetime.now(timezone.utc)
            )
            logger.debug(f"Successfully saved UI event '{event_type}'")
        except Exception as e:
            logger.warning(f"Failed to persist UI event '{event_type}': {e}")

    def is_enabled(self) -> bool:
        """Check if UI event emission is enabled."""
        return self._enabled

    def enable(self) -> None:
        """Enable UI event emission."""
        if self._storage:
            self._enabled = True

    def disable(self) -> None:
        """Disable UI event emission."""
        self._enabled = False
