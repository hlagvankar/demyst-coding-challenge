import unittest
import os
import csv
from src.generate_csv import generate_csv

class TestGenerateCSV(unittest.TestCase):

    def test_generate_csv(self):
        output_path = "test_generated.csv"
        num_rows = 10
        
        # Generate the CSV file
        generate_csv(output_path, num_rows)
        
        # Check if the file was created
        self.assertTrue(os.path.exists(output_path))
        
        # Read the file and check the contents
        with open(output_path, mode='r') as file:
            reader = csv.reader(file)
            rows = list(reader)
        
        # Check that the correct number of rows were generated (including header)
        self.assertEqual(len(rows), num_rows + 1)
        
        # Check the header
        self.assertEqual(rows[0], ["first_name", "last_name", "address", "date_of_birth"])
        
        # Check that the data rows are not empty
        for row in rows[1:]:
            self.assertEqual(len(row), 4)
            self.assertTrue(all(field.strip() for field in row))  # Ensure no empty fields
        
        # Clean up
        if os.path.exists(output_path):
            os.remove(output_path)

if __name__ == "__main__":
    unittest.main()
