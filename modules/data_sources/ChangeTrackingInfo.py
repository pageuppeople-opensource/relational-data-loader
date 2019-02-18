
class ChangeTrackingInfo:
    this_sync_version = 0  # TODO rename `this_sync_version` to `last_sync_version`
    next_sync_version = 0  # TODO rename `next_sync_version` to `this?current?new?_sync_version`
    force_full_load = 0

    def __init__(self, this_sync_version, next_sync_version, force_full_load):
        self.this_sync_version = this_sync_version
        self.next_sync_version = next_sync_version
        self.force_full_load = force_full_load
