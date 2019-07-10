from rdl.data_sources.ChangeTrackingInfo import ChangeTrackingInfo


class SourceTableInfo:
    def __init__(self, column_names: [],
                 last_sync_version: str, sync_version: str,
                 force_full_load: str, data_changed_since_last_sync: str):
        self.column_names = column_names
        self.last_sync_version = last_sync_version
        self.sync_version = sync_version
        self.force_full_load = force_full_load
        self.data_changed_since_last_sync = data_changed_since_last_sync

    @classmethod
    def from_change_tracking_info(cls, column_names: [], change_tracking_info: ChangeTrackingInfo):
        return cls(column_names,
                   change_tracking_info.last_sync_version, change_tracking_info.sync_version,
                   change_tracking_info.force_full_load, change_tracking_info.data_changed_since_last_sync)
