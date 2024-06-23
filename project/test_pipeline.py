import unittest
import os
import sqlite3
from project.pipeline import execute_pipeline  # Import the main function from your pipeline script

class DataPipelineUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the environment before any test cases are executed."""
        cls.db_path = '../data/MADE.sqlite'
        
        # Remove the existing database file before running the tests
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        
        # Execute the pipeline
        execute_pipeline()
        
        # Connect to the database and initialize cursor
        cls.conn = sqlite3.connect(cls.db_path)
        cls.cursor = cls.conn.cursor()

    def test_database_creation(self):
        """Check if the database file has been created."""
        self.assertTrue(os.path.exists(self.db_path), "Database file was not created.")

    def test_traffic_table_creation(self):
        """Verify the creation of the traffic table."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traffic';")
        traffic_table = self.cursor.fetchone()
        self.assertIsNotNone(traffic_table, "Traffic table was not found in the database.")

    def test_weather_table_creation(self):
        """Verify the creation of the weather table."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather';")
        weather_table = self.cursor.fetchone()
        self.assertIsNotNone(weather_table, "Weather table was not found in the database.")

    def test_traffic_table_schema(self):
        """Check if the traffic table has the correct schema."""
        self.cursor.execute("PRAGMA table_info(traffic);")
        traffic_schema = self.cursor.fetchall()
        expected_traffic_schema = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'month', 'TEXT', 0, None, 0),
            (2, 'traffics', 'INTEGER', 0, None, 0)
        ]
        self.assertEqual(traffic_schema, expected_traffic_schema, "Traffic table schema does not match the expected schema.")

    def test_weather_table_schema(self):
        """Check if the weather table has the correct schema."""
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
        self.assertEqual(weather_schema, expected_weather_schema, "Weather table schema does not match the expected schema.")

    def test_traffic_table_data(self):
        """Verify that the traffic table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM traffic;")
        traffic_count = self.cursor.fetchone()[0]
        self.assertGreater(traffic_count, 0, "Traffic table is empty.")

    def test_weather_table_data(self):
        """Verify that the weather table contains data."""
        self.cursor.execute("SELECT COUNT(*) FROM weather;")
        weather_count = self.cursor.fetchone()[0]
        self.assertGreater(weather_count, 0, "Weather table is empty.")

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests are executed."""
        cls.conn.close()
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

if __name__ == "__main__":
    unittest.main()
