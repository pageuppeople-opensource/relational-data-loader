class BatchKeyTracker(object):
    def __init__(self, primary_keys):
        self.primary_keys = primary_keys
        self.has_more_data = True
        self.bookmarks = {}

        for primary_key in primary_keys:
            self.bookmarks[primary_key] = 0

    def set_bookmark(self, key, value):
        self.bookmarks[key] = value
