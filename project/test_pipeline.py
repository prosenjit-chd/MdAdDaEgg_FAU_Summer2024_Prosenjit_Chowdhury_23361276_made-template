import unittest
import os
import sqlite3
from pipeline import main

class DataPipelineTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Prepare the test environment before any tests are executed."""
        cls.database_path = '../data/MADE.sqlite'
        
        # Remove the existing database file before running the tests
        if os.path.exists(cls.database_path):
            os.remove(cls.database_path)
        
        # Execute the main function to run the data pipeline
        main()
        
        # Initialize the database connection and cursor for use in tests
        cls.connection = sqlite3.connect(cls.database_path)
        cls.cursor = cls.connection.cursor()

    def test_pipeline_execution(self):
        """Verify that the data pipeline runs successfully."""
        # Ensure the database file is created
        self.assertTrue(os.path.exists(self.database_path), "Database file was not generated.")

        # Verify the traffic table is created
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traffic';")
        traffic_table = self.cursor.fetchone()
        self.assertIsNotNone(traffic_table, "Traffic table was not found in the database.")

        # Verify the weather table is created
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather';")
        weather_table = self.cursor.fetchone()
        self.assertIsNotNone(weather_table, "Weather table was not found in the database.")

    def test_table_schemas(self):
        """Ensure the tables have the expected schema definitions."""
        # Check the schema of the traffic table
        self.cursor.execute("PRAGMA table_info(traffic);")
        traffic_schema = self.cursor.fetchall()
        expected_traffic_schema = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'month', 'TEXT', 0, None, 0),
            (2, 'traffics', 'INTEGER', 0, None, 0)
        ]
        self.assertEqual(traffic_schema, expected_traffic_schema, "Traffic table schema is incorrect.")

        # Check the schema of the weather table
        self.cursor.execute("PRAGMA table_info(weather);")
        weather_schema = self.cursor.fetchall()
        expected_weather_schema = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'month', 'TEXT', 0, None, 0),
            (2, 'tavg', 'REAL', 0, None, 0),
            (3, 'snow', 'REAL', 0, None, 0),
            (4, 'prcp', 'REAL', 0, None, 0),
            (5, 'wspd', 'REAL', 0, None, 0)
        ]
        self.assertEqual(weather_schema, expected_weather_schema, "Weather table schema is incorrect.")

    def test_data_presence(self):
        """Check that the tables contain data."""
        # Verify that the traffic table contains data
        self.cursor.execute("SELECT COUNT(*) FROM traffic;")
        traffic_count = self.cursor.fetchone()[0]
        self.assertGreater(traffic_count, 0, "Traffic table is empty.")

        # Verify that the weather table contains data
        self.cursor.execute("SELECT COUNT(*) FROM weather;")
        weather_count = self.cursor.fetchone()[0]
        self.assertGreater(weather_count, 0, "Weather table is empty.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests have been executed."""
        cls.connection.close()
        # Remove the database file after tests are complete
        if os.path.exists(cls.database_path):
            os.remove(cls.database_path)

if __name__ == "__main__":
    unittest.main()
