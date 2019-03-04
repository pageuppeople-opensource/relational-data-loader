class ChangeTrackingInfo:
    def __init__(self, last_sync_version, sync_version, force_full_load):
        self.last_sync_version = last_sync_version
        self.sync_version = sync_version
        self.force_full_load = force_full_load
