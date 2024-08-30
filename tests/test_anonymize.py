import unittest
from pyspark.sql import SparkSession
from src.anonymize import anonymize_data
import os

class TestAnonymizeData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test Anonymizer").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_anonymize_data(self):
        input_data = [("John", "Doe", "1234 Main Street", "1980-01-01")]
        expected_data = [("JXXXXn", "DXXXXe", "1234 XXXXn Street", "1980-01-01")]
        
        # Create a DataFrame from the input data
        input_df = self.spark.createDataFrame(input_data, ["first_name", "last_name", "address", "date_of_birth"])

        # Write input data to a temporary CSV file
        input_path = "test_input.csv"
        input_df.write.csv(input_path, header=True, mode="overwrite")

        # Anonymize the data and write to another temporary CSV file
        output_path = "test_output.csv"
        anonymize_data(input_path, output_path)

        # Read the output data
        output_df = self.spark.read.csv(output_path, header=True)

        # Convert DataFrames to lists of rows for comparison
        output_rows = [(row.first_name, row.last_name, row.address, row.date_of_birth) for row in output_df.collect()]
        expected_rows = expected_data

        # Check that the anonymized data matches the expected output
        self.assertEqual(output_rows, expected_rows)

        # Clean up
        if os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)

if __name__ == "__main__":
    unittest.main()
