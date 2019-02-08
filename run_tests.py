from modules.tests import test_MsSqlDataSource
import unittest

suite = unittest.TestLoader().loadTestsFromModule(test_MsSqlDataSource)
unittest.TextTestRunner(verbosity=2).run(suite)
