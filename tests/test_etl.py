# tests/test_etl.py

import json
import unittest
from unittest.mock import Mock
import pandas as pd
import sys
import os

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl_module.main import load_raw_data, process_to_staging

class TestETL(unittest.TestCase):

    def setUp(self):
        # Set up any test data or state before each test
        self.raw_data = [
            {"client_id": 1, "data": json.dumps({"first_name": "John", "last_name": "Doe", "email": "johndoe@example.com", "phone": "123-456-7890", "created_at": "2023-01-15 08:30:00"})},
            {"client_id": 2, "data": json.dumps({"first_name": "Jane", "last_name": "Smith", "email": "janesmith@example.com", "phone": "234-567-8901", "created_at": "2023-01-16 09:00:00"})},
            {"client_id": 3, "data": json.dumps({"first_name": "Bob", "last_name": "Brown", "email": "bobbrown@example.com", "phone": "345-678-9012", "created_at": "2023-01-17 10:15:00"})}
        ]
        self.expected_data = pd.DataFrame([
            {"client_id": 1, "first_name": "John", "last_name": "Doe", "full_name": "John Doe"},
            {"client_id": 2, "first_name": "Jane", "last_name": "Smith", "full_name": "Jane Smith"},
            {"client_id": 3, "first_name": "Alice", "last_name": "Johnson", "full_name": "Alice Johnson"}
        ])

    def test_extract(self):
        # Test the extract function
        df = load_raw_data("docs/test_client_data.csv")
        pd.testing.assert_frame_equal(df, pd.DataFrame(self.raw_data))

    def test_transform(self):
        # Test the transform function
        df = pd.DataFrame(self.raw_data)
        conn = Mock()
        transformed_df = process_to_staging(conn, df)
        pd.testing.assert_frame_equal(transformed_df, self.expected_data)

if __name__ == '__main__':
    unittest.main()
