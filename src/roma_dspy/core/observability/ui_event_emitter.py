"""UI Event Emitter for streaming events to PostgreSQL."""

from typing import Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime, timezone
from loguru import logger
import asyncio
from dataclasses import dataclass

if TYPE_CHECKING:
    from roma_dspy.core.storage.postgres_storage import PostgresStorage


@dataclass
class QueuedEvent:
    """Event queued for persistence."""
    execution_id: str
    event_type: str
    data: Dict[str, Any]
    timestamp: datetime


class UIEventEmitter:
    """
    Emits UI events to PostgreSQL for frontend streaming.

    This is completely separate from EventScheduler (task orchestration).
    Purely for UI visualization - writes events to PostgreSQL via a queue.

    Features:
    - Synchronous emit_sync() for synchronous contexts (DAG operations)
    - Asynchronous emit() for async contexts
    - Internal queue with background worker for PostgreSQL writes
    - Guarantees event ordering through FIFO queue

    Usage:
        emitter = UIEventEmitter(postgres_storage)
        await emitter.start()  # Start background worker
        emitter.emit_sync(execution_id="abc", event_type="my_event", data={...})
        await emitter.stop()  # Stop background worker
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
        self._queue: Optional[asyncio.Queue[QueuedEvent]] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the background worker for processing events."""
        if not self._enabled or self._running:
            return

        self._running = True
        self._queue = asyncio.Queue()
        self._worker_task = asyncio.create_task(self._process_queue())
        logger.debug("UIEventEmitter background worker started")

    async def stop(self) -> None:
        """
        Stop the background worker and flush remaining events.

        Ensures all queued events are persisted to PostgreSQL before shutdown.
        """
        if not self._running:
            return

        # Wait for queue to be empty BEFORE stopping worker
        # This ensures all events are processed
        if self._queue:
            await self._queue.join()

        # Now signal worker to stop
        self._running = False

        # Cancel worker task (will trigger flush of any remaining events)
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        logger.debug("UIEventEmitter background worker stopped")

    async def _process_queue(self) -> None:
        """Background worker that processes queued events."""
        try:
            while self._running:
                try:
                    event = await self._queue.get()

                    try:
                        await self._storage.save_ui_event(
                            execution_id=event.execution_id,
                            event_type=event.event_type,
                            data=event.data,
                            timestamp=event.timestamp
                        )
                    except Exception as e:
                        logger.debug(f"Failed to persist UI event '{event.event_type}': {e}")
                    finally:
                        self._queue.task_done()

                except asyncio.CancelledError:
                    # Graceful shutdown - process remaining events
                    logger.debug("Worker cancelled, flushing remaining events")
                    while not self._queue.empty():
                        try:
                            event = self._queue.get_nowait()
                            await self._storage.save_ui_event(
                                execution_id=event.execution_id,
                                event_type=event.event_type,
                                data=event.data,
                                timestamp=event.timestamp
                            )
                            self._queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                        except Exception as e:
                            logger.debug(f"Failed to flush event: {e}")
                            self._queue.task_done()
                    raise  # Re-raise to exit

                except Exception as e:
                    logger.error(f"Unexpected error in UI event worker: {e}")

        except asyncio.CancelledError:
            logger.debug("UI event worker gracefully shut down")
            raise

    def _create_event(
        self,
        execution_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None
    ) -> QueuedEvent:
        """Create event object with timestamp."""
        return QueuedEvent(
            execution_id=execution_id,
            event_type=event_type,
            data=data or {},
            timestamp=datetime.now(timezone.utc)
        )

    def emit_sync(
        self,
        execution_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Emit a UI event synchronously (from non-async code).

        This adds the event to an internal queue without blocking.
        A background worker processes the queue asynchronously.

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
        if not self._enabled or not self._running or not self._queue:
            return

        try:
            event = self._create_event(execution_id, event_type, data)
            self._queue.put_nowait(event)
        except Exception as e:
            logger.debug(f"Failed to queue UI event '{event_type}': {e}")

    async def emit(
        self,
        execution_id: str,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Emit a UI event asynchronously.

        This adds the event to an internal queue. A background worker
        processes the queue and persists to PostgreSQL.

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
        if not self._enabled or not self._running or not self._queue:
            return

        try:
            event = self._create_event(execution_id, event_type, data)
            await self._queue.put(event)
        except Exception as e:
            logger.debug(f"Failed to queue UI event '{event_type}': {e}")

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
