from modules.tests import test_MsSqlDataSource
import unittest
import sys

TEST_VERBOSITY_LEVEL = 2  # Verbose, see https://stackoverflow.com/a/1322648/8030743

suite = unittest.TestLoader().loadTestsFromModule(test_MsSqlDataSource)
result = unittest.TextTestRunner(verbosity=TEST_VERBOSITY_LEVEL).run(suite)
sys.exit(not result.wasSuccessful())
