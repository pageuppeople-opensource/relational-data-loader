import test_MsSqlDataSource
import test_DataLoadTrackerRepository
import unittest
import sys

TEST_VERBOSITY_LEVEL = 2  # Verbose, see https://stackoverflow.com/a/1322648/8030743

for module in [test_DataLoadTrackerRepository, test_MsSqlDataSource]:
    suite = unittest.TestLoader().loadTestsFromModule(module)
    result = unittest.TextTestRunner(verbosity=TEST_VERBOSITY_LEVEL).run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
