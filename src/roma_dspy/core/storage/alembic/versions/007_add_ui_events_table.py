"""Add ui_events table for frontend visualization

Revision ID: 007_add_ui_events
Revises: 006_add_experiment_name_and_fix_profile
Create Date: 2025-11-18

This migration adds the ui_events table for streaming execution events to the UI.
This is separate from event_traces (task orchestration) - purely for visualization.

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '007_add_ui_events'
down_revision: Union[str, None] = '006_add_experiment_name_and_fix_profile'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add ui_events table for frontend streaming."""

    # Create ui_events table
    op.create_table(
        'ui_events',
        sa.Column('id', sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column('execution_id', sa.String(length=255), nullable=False),
        sa.Column('event_type', sa.String(length=128), nullable=False),
        sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'),
        sa.Column('timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for efficient streaming queries
    op.create_index(
        'idx_ui_events_stream',
        'ui_events',
        ['execution_id', 'id'],
        unique=False,
        postgresql_using='btree'
    )
    op.create_index(
        'idx_ui_events_execution_id',
        'ui_events',
        ['execution_id'],
        unique=False
    )
    op.create_index(
        'idx_ui_events_type',
        'ui_events',
        ['event_type'],
        unique=False
    )
    op.create_index(
        'idx_ui_events_timestamp',
        'ui_events',
        ['timestamp'],
        unique=False
    )


def downgrade() -> None:
    """Remove ui_events table."""

    # Drop indexes
    op.drop_index('idx_ui_events_timestamp', table_name='ui_events')
    op.drop_index('idx_ui_events_type', table_name='ui_events')
    op.drop_index('idx_ui_events_execution_id', table_name='ui_events')
    op.drop_index('idx_ui_events_stream', table_name='ui_events')

    # Drop table
    op.drop_table('ui_events')
