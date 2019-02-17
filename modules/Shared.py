import importlib


class Constants:
    APP_NAME = 'Relational Data Loader'
    DATA_PIPELINE_EXECUTION_SCHEMA_NAME = 'data_pipeline'

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
        TIMESTAMP = "data_pipeline_timestamp"
        IS_DELETED = "data_pipeline_is_deleted"
        CHANGE_VERSION = "data_pipeline_change_version"


class Utils:
    @staticmethod
    def create_type_instance(type_name):
        module = importlib.import_module(type_name)
        class_ = getattr(module, type_name)
        instance = class_()
        return instance
