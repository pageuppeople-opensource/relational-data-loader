class Constants:
    APP_NAME = 'relational-data-loader'
    DATA_PIPELINE_EXECUTION_SCHEMA_NAME = 'data_pipeline'

    class FullRefreshReason:
        NOT_APPLICABLE = 'N/A'
        USER_REQUESTED = 'User Requested'  # previously known as 'Command Line Argument'
        DESTINATION_TABLE_ABSENT = 'Destination table does not exist'
        FIRST_EXECUTION = 'First Execution'
        MODEL_CHANGED = 'Model Changed'
        INVALID_CHANGE_TRACKING = 'Change Tracking Invalid'
