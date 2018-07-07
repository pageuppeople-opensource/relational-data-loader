
class ChangeTrackingInfo:
    this_sync_version = 0
    next_sync_version = 0

    def __init__(self, this_sync_version, next_sync_version):
        self.this_sync_version = this_sync_version
        self.next_sync_version = next_sync_version

    def force_full_load(self):
        return bool(self.this_sync_version == 0 or self.next_sync_version == 0)


