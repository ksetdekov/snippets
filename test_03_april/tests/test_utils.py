import pandas as pd
import unittest
from model.utils.utils import input_json_splitter

class TestUtils(unittest.TestCase):
    """Test the input_json_splitter function."""
    def test_input_json_splitter(self):
        input_dict = {
            'params': {'param1': 10, 'param2': 'abc'},
            'dataframe': {
                'timestamp': ['2022-01-01', '2022-01-02', '2022-01-03'],
                'value': [1, 2, 3]
            }
        }

        expected_params = {'param1': 10, 'param2': 'abc'}
        expected_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2022-01-01', '2022-01-02', '2022-01-03']),
            'value': [1, 2, 3]
        })

        params, df = input_json_splitter(input_dict)

        self.assertEqual(params, expected_params)
        self.assertTrue(df.equals(expected_df))

if __name__ == '__main__':
    unittest.main()