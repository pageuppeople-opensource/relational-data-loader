"""normalize schema

Revision ID: 00f2b412576b
Revises: 955122a76711
Create Date: 2019-05-15 21:46:42.147590

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "00f2b412576b"
down_revision = "955122a76711"
branch_labels = None
depends_on = None


def upgrade():
    # mark existing tables with old revision
    op.execute("ALTER TABLE rdl.execution RENAME TO execution_955122a76711")
    op.execute("ALTER TABLE rdl.execution_model RENAME TO execution_model_955122a76711")

    # create new schema tables
    op.create_table(
        "execution",
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "status", sa.String(length=50), server_default="Started", nullable=False
        ),
        sa.Column(
            "started_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_on", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_time_s", sa.BigInteger(), nullable=True),
        sa.Column("rows_processed", sa.BigInteger(), nullable=True),
        sa.Column("models_processed", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("execution_id"),
        schema="rdl",
    )
    op.create_table(
        "execution_model",
        sa.Column("execution_model_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("model_name", sa.String(length=250), nullable=False),
        sa.Column(
            "status", sa.String(length=50), server_default="Started", nullable=False
        ),
        sa.Column("last_sync_version", sa.BigInteger(), nullable=False),
        sa.Column("sync_version", sa.BigInteger(), nullable=False),
        sa.Column("is_full_refresh", sa.Boolean(), nullable=False),
        sa.Column("full_refresh_reason", sa.String(length=100), nullable=False),
        sa.Column(
            "started_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_on", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_time_ms", sa.BigInteger(), nullable=True),
        sa.Column("rows_processed", sa.BigInteger(), nullable=True),
        sa.Column("model_checksum", sa.String(length=100), nullable=False),
        sa.Column("failure_reason", sa.String(length=1000), nullable=True),
        sa.ForeignKeyConstraint(["execution_id"], ["rdl.execution.execution_id"]),
        sa.PrimaryKeyConstraint("execution_model_id"),
        schema="rdl",
    )

    # move data from old tables to new tables
    op.execute(
        """
        INSERT INTO rdl.execution (
            execution_id, created_on, updated_on,
            status, started_on, completed_on,
            execution_time_s, rows_processed, models_processed
        )
        SELECT
            id, execution_started, COALESCE(execution_ended, execution_started),
            status, execution_started, execution_ended,
            execution_time_s, total_rows_processed, total_models_processed
        FROM rdl.execution_955122a76711
        """
    )
    op.execute(
        """
        INSERT INTO rdl.execution_model (
            execution_model_id, execution_id, created_on, updated_on,
            model_name, status, started_on, completed_on, failure_reason,
            last_sync_version, sync_version, is_full_refresh, full_refresh_reason,
            execution_time_ms, rows_processed, model_checksum
        )
        SELECT
            uuid_generate_v4(), execution_id, started_on, COALESCE(completed_on, started_on),
            model_name, status, started_on, completed_on, failure_reason,
            last_sync_version, sync_version, is_full_refresh, full_refresh_reason,
            execution_time_ms, rows_processed, model_checksum
        FROM rdl.execution_model_955122a76711
        """
    )

    # drop old tables
    op.drop_table("execution_model_955122a76711", schema="rdl")
    op.drop_table("execution_955122a76711", schema="rdl")


def downgrade():
    # mark existing tables with new revision
    op.execute("ALTER TABLE rdl.execution RENAME TO execution_00f2b412576b")
    op.execute("ALTER TABLE rdl.execution_model RENAME TO execution_model_00f2b412576b")

    # create old revision tables
    op.create_table(
        "execution",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "status", sa.String(length=50), server_default="Started", nullable=False
        ),
        sa.Column(
            "execution_started",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("execution_ended", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_time_s", sa.BigInteger(), nullable=True),
        sa.Column("total_rows_processed", sa.BigInteger(), nullable=True),
        sa.Column("total_models_processed", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema="rdl",
    )
    op.create_table(
        "execution_model",
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("model_name", sa.String(length=250), nullable=False),
        sa.Column("status", sa.String(length=25), nullable=False),
        sa.Column("last_sync_version", sa.BigInteger(), nullable=False),
        sa.Column("sync_version", sa.BigInteger(), nullable=False),
        sa.Column("is_full_refresh", sa.Boolean(), nullable=False),
        sa.Column("full_refresh_reason", sa.String(length=100), nullable=False),
        sa.Column(
            "started_on",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_on", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_time_ms", sa.Integer(), nullable=True),
        sa.Column("rows_processed", sa.Integer(), nullable=True),
        sa.Column("model_checksum", sa.String(length=100), nullable=False),
        sa.Column("failure_reason", sa.String(length=1000), nullable=True),
        # sa.ForeignKeyConstraint(['execution_id'], ['rdl.execution.id'], ),
        # sa.PrimaryKeyConstraint('execution_id', 'model_name'),
        schema="rdl",
    )
    op.create_primary_key(
        "pk_data_load_execution",
        "execution_model",
        ["execution_id", "model_name"],
        schema="rdl",
    )
    op.create_foreign_key(
        "data_load_execution_execution_id_fkey",
        "execution_model",
        "execution",
        ["execution_id"],
        ["id"],
        source_schema="rdl",
        referent_schema="rdl",
    )

    # move data from new revision tables to old revision tables
    op.execute(
        """
        INSERT INTO rdl.execution (
            id,
            status, execution_started, execution_ended,
            execution_time_s, total_rows_processed, total_models_processed
        )
        SELECT
            execution_id,
            status, started_on, completed_on,
            execution_time_s, rows_processed, models_processed
        FROM rdl.execution_00f2b412576b
        """
    )
    op.execute(
        """
        INSERT INTO rdl.execution_model (
            execution_id,
            model_name, status, started_on, completed_on, failure_reason,
            last_sync_version, sync_version, is_full_refresh, full_refresh_reason,
            execution_time_ms, rows_processed, model_checksum
        )
        SELECT
            execution_id,
            model_name, status, started_on, completed_on, failure_reason,
            last_sync_version, sync_version, is_full_refresh, full_refresh_reason,
            execution_time_ms, rows_processed, model_checksum
        FROM rdl.execution_model_00f2b412576b
        """
    )

    # drop new revision tables
    op.drop_table("execution_model_00f2b412576b", schema="rdl")
    op.drop_table("execution_00f2b412576b", schema="rdl")
