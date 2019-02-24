APP_NAME = 'Relational Data Loader'
DATA_PIPELINE_EXECUTION_SCHEMA_NAME = 'data_pipeline'
AUDIT_COLUMN_PREFIX = f'{DATA_PIPELINE_EXECUTION_SCHEMA_NAME}_'


class FullRefreshReason:
    NOT_APPLICABLE = 'N/A'
    USER_REQUESTED = 'User Requested'
    DESTINATION_TABLE_ABSENT = 'Destination table does not exist'
    FIRST_EXECUTION = 'First Execution'
    MODEL_CHANGED = 'Model Changed'
    INVALID_CHANGE_TRACKING = 'Change Tracking Invalid'


class ExecutionStatus:
    NOT_STARTED = 'Not Started'
    EXTRACT_COMPLETED_SUCCESSFULLY = 'Extract Completed Successfully'
    LOAD_COMPLETED_SUCCESSFULLY = 'Load Completed Successfully'
    SKIPPED_AS_ZERO_ROWS = 'Skipped - Zero Rows'
    COMPLETED_SUCCESSFULLY = 'Completed Successfully'


class AuditColumnNames:
    TIMESTAMP = f'{AUDIT_COLUMN_PREFIX}timestamp'
    IS_DELETED = f'{AUDIT_COLUMN_PREFIX}is_deleted'
    CHANGE_VERSION = f'{AUDIT_COLUMN_PREFIX}change_version'