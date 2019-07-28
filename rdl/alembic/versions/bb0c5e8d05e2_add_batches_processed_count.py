"""add batches_processed count

Revision ID: bb0c5e8d05e2
Revises: 00f2b412576b
Create Date: 2019-07-26 13:56:06.412042

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bb0c5e8d05e2'
down_revision = '00f2b412576b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('execution', sa.Column('batches_processed', sa.Integer(), nullable=True), schema='rdl')
    op.add_column('execution_model', sa.Column('batches_processed', sa.Integer(), nullable=True), schema='rdl')


def downgrade():
    op.drop_column('execution_model', 'batches_processed', schema='rdl')
    op.drop_column('execution', 'batches_processed', schema='rdl')
