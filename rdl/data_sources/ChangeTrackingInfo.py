class ChangeTrackingInfo:
    def __init__(self, last_sync_version, sync_version, force_full_load, data_changed_since_last_sync):
        self.last_sync_version = last_sync_version
        self.sync_version = sync_version
        self.force_full_load = force_full_load
        self.data_changed_since_last_sync = data_changed_since_last_sync
