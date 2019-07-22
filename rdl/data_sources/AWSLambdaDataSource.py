import logging
import pandas
import json
import boto3

from rdl.data_sources.ChangeTrackingInfo import ChangeTrackingInfo
from rdl.data_sources.SourceTableInfo import SourceTableInfo
from rdl.shared import Providers
from rdl.shared.Utils import prevent_senstive_data_logging


class AWSLambdaDataSource(object):
    # 'aws-lambda://tenant=543_dc2;function=123456789012:function:my-function;'
    CONNECTION_STRING_PREFIX = 'aws-lambda://'
    CONNECTION_STRING_GROUP_SEPARATOR = ';'
    CONNECTION_STRING_KEY_VALUE_SEPARATOR = '='

    def __init__(self, connection_string, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        if not AWSLambdaDataSource.can_handle_connection_string(connection_string):
            raise ValueError(connection_string)
        self.connection_string = connection_string
        self.connection_data = dict(kv.split(AWSLambdaDataSource.CONNECTION_STRING_KEY_VALUE_SEPARATOR) for kv in
                                    self.connection_string
                                    .lstrip(AWSLambdaDataSource.CONNECTION_STRING_PREFIX)
                                    .rstrip(AWSLambdaDataSource.CONNECTION_STRING_GROUP_SEPARATOR)
                                    .split(AWSLambdaDataSource.CONNECTION_STRING_GROUP_SEPARATOR))
        self.aws_lambda_client = boto3.client('lambda')

    @staticmethod
    def can_handle_connection_string(connection_string):
        return connection_string.startswith(AWSLambdaDataSource.CONNECTION_STRING_PREFIX)

    @staticmethod
    def connection_string_prefix():
        return AWSLambdaDataSource.CONNECTION_STRING_PREFIX

    def get_table_info(self, table_config, last_known_sync_version):
        column_names, last_sync_version, sync_version, full_refresh_required, data_changed_since_last_sync = \
            self.__get_table_info(table_config, last_known_sync_version)
        columns_in_database = column_names
        change_tracking_info = ChangeTrackingInfo(
            last_sync_version=last_sync_version,
            sync_version=sync_version,
            force_full_load=full_refresh_required,
            data_changed_since_last_sync=data_changed_since_last_sync)
        source_table_info = SourceTableInfo(columns_in_database, change_tracking_info)
        return source_table_info

    @prevent_senstive_data_logging
    def get_table_data_frame(self, table_config, columns, batch_config, batch_tracker, batch_key_tracker,
                             full_refresh, change_tracking_info):
        self.logger.debug(f"Starting read data from lambda.. : \n{None}")
        column_names, data = self.__get_table_data(table_config, batch_config, change_tracking_info, full_refresh, columns, batch_key_tracker)
        self.logger.debug(f"Finished read data from lambda.. : \n{None}")
        # should we log size of data extracted?
        data_frame = pandas.DataFrame(data=data, columns=column_names)
        batch_tracker.extract_completed_successfully(len(data_frame))
        return data_frame

    def __get_table_info(self, table_config, last_known_sync_version):
        pay_load = {
            "command": "GetTableInfo",
            "tenantId": self.connection_data['tenant'],
            "table": {
                "schema": table_config['schema'],
                "name": table_config['name']
            },
            "commandPayload": {
                "lastSyncVersion": last_known_sync_version,
            }
        }

        result = self.__invoke_lambda(pay_load)

        return result['ColumnNames'], result['Data']

    def __get_table_data(self, table_config, batch_config, change_tracking_info, full_refresh, columns, batch_key_tracker):
        pay_load = {
            "command": "GetTableData",
            "tenantId": 543,  # self.connection_string.tenant.split('_')[0] as int
            "table": {
                "schema": table_config['schema'],
                "name": table_config['name']
            },
            "commandPayload": {
                "auditColumnNameForChangeVersion": Providers.AuditColumnsNames.CHANGE_VERSION,
                "auditColumnNameForDeletionFlag": Providers.AuditColumnsNames.IS_DELETED,
                "batchSize": batch_config['size'],
                "lastSyncVersion": change_tracking_info.last_sync_version,
                "fullRefresh": full_refresh,
                "columnNames": columns,
                "primaryKeyColumnNames": table_config['primary_keys'],
                "lastBatchPrimaryKeys": [{k: v} for k, v in batch_key_tracker.bookmarks.items()]
            }
        }

        result = self.__invoke_lambda(pay_load)

        return result['ColumnNames'], result['Data']

    def __invoke_lambda(self, pay_load):
        lambda_response = self.aws_lambda_client.invoke(
            FunctionName=self.connection_data['function'],
            InvocationType='RequestResponse',
            LogType='None',  # |'Tail', Set to Tail to include the execution log in the response
            Payload=json.dump(pay_load).encode()
        )
        result = json.loads(lambda_response.Payload.read())  # .decode()
        return result

