APP_NAME = "Relational Data Loader"
DATA_PIPELINE_EXECUTION_SCHEMA_NAME = "rdl"
MAX_AWS_LAMBDA_INVOKATION_ATTEMPTS = 4 # 1 + 3 retries
AWS_LAMBDA_RETRY_DELAY_SECONDS = 4 # 10 ^ retry attempt, so retry attempt 3 is delayed 64 seconds

class FullRefreshReason:
    NOT_APPLICABLE = "N/A"
    USER_REQUESTED = "User Requested"
    DESTINATION_TABLE_ABSENT = "Destination table does not exist"
    FIRST_EXECUTION = "First Execution"
    MODEL_CHANGED = "Model Changed"
    INVALID_CHANGE_TRACKING = "Change Tracking Invalid"


class IncrementalSkipReason:
    NOT_APPLICABLE = "N/A"
    SYNC_VERSIONS_ARE_EQUAL = "last_sync_version is the same as sync_version"
    NO_DATA_CHANGED = "No data has changed since last sync"


class ExecutionStatus:
    STARTED = "Started"
    FAILED = "Failed"
    UNKNOWN = "UNKNOWN"
    SUCCESSFUL = "Successful"


class ExecutionModelStatus:
    STARTED = "Started"
    FAILED = "Failed"
    SUCCESSFUL = "Successful"


class BatchExecutionStatus:
    STARTED = "Started"
    EXTRACT_COMPLETED_SUCCESSFULLY = "Extract Successful"
    LOAD_COMPLETED_SUCCESSFULLY = "Load Successful"
    SKIPPED_AS_ZERO_ROWS = "Skipped - Zero Rows"
