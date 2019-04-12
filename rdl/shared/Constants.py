APP_NAME = 'Relational Data Loader'
DATA_PIPELINE_EXECUTION_SCHEMA_NAME = 'rdl'


class FullRefreshReason:
    NOT_APPLICABLE = 'N/A'
    USER_REQUESTED = 'User Requested'
    DESTINATION_TABLE_ABSENT = 'Destination table does not exist'
    FIRST_EXECUTION = 'First Execution'
    MODEL_CHANGED = 'Model Changed'
    INVALID_CHANGE_TRACKING = 'Change Tracking Invalid'


class ExecutionStatus:
    STARTED = 'Started'
    FAILED = 'Failed'
    UNKNOWN = 'UNKNOWN'
    SUCCESSFUL = 'Successful'


class ExecutionModelStatus:
    STARTED = 'Started'
    FAILED = 'Failed'
    SUCCESSFUL = 'Successful'


class BatchExecutionStatus:
    STARTED = 'Started'
    EXTRACT_COMPLETED_SUCCESSFULLY = 'Extract Successful'
    LOAD_COMPLETED_SUCCESSFULLY = 'Load Successful'
    SKIPPED_AS_ZERO_ROWS = 'Skipped - Zero Rows'
