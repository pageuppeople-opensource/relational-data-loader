import unittest

from rdl.data_sources.AWSLambdaDataSource import AWSLambdaDataSource


class TestAWSLambdaDataSource(unittest.TestCase):
    data_source = None
    table_configs = []

    @classmethod
    def setUpClass(cls):
        TestAWSLambdaDataSource.data_source = AWSLambdaDataSource(
            "aws-lambda://tenant=543_dc2;function=123456789012:function:my-function;"
        )

    @classmethod
    def tearDownClass(cls):
        TestAWSLambdaDataSource.data_source = None

    def test_can_handle_valid_connection_string(self):
        self.assertTrue(
            self.data_source.can_handle_connection_string(
                "aws-lambda://tenant=543_dc2;function=123456789012:function:my-function;"
            )
        )

    def test_can_handle_invalid_connection_string(self):
        self.assertFalse(
            self.data_source.can_handle_connection_string(
                "lambda-aws://tenant=543_dc2;function=123456789012:function:my-function;"
            )
        )


if __name__ == "__main__":
    unittest.main()
