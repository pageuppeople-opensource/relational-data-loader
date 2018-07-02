import os
import json
import logging
from modules.BatchDataLoader import BatchDataLoader
from modules.DestinationTableManager import DestinationTableManager
from modules.DataLoadTracker import DataLoadTracker



class BatchExecutionManager(object):
    def __init__(self, configuration_path, data_source, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.data_source = data_source
