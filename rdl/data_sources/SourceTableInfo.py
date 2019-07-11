from rdl.data_sources.ChangeTrackingInfo import ChangeTrackingInfo


class SourceTableInfo:
    def __init__(self, column_names: [], change_tracking_info: ChangeTrackingInfo):
        self.column_names = column_names
        self.change_tracking_info = change_tracking_info
