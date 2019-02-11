from modules.tests import test_MsSqlDataSource
import unittest

suite = unittest.TestLoader().loadTestsFromModule(test_MsSqlDataSource)
# https://stackoverflow.com/a/1322648/8030743
unittest.TextTestRunner(verbosity=2).run(suite)
