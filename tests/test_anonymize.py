import unittest
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define the function and UDF
def replace_with_random_characters(text):
    """Replace each character in the text with a random character."""
    if text is None:
        return None
    return ''.join(random.choice(string.ascii_letters) for _ in text)

replace_with_random_characters_udf = udf(replace_with_random_characters, StringType())

class TestAnonymization(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for the tests."""
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the SparkSession."""
        cls.spark.stop()

    def test_replace_with_random_characters(self):
        original_text = "James"
        anonymized_text = replace_with_random_characters(original_text)
        
        print(f"Original Text: {original_text}")
        print(f"Anonymized Text: {anonymized_text}")

        self.assertEqual(len(anonymized_text), len(original_text))
        self.assertNotEqual(anonymized_text, original_text)
        self.assertTrue(all(c.isalpha() for c in anonymized_text))  # Ensure all characters are letters

    def test_anonymize_data(self):
        # Create a sample DataFrame
        data = [("James", "Ramirez", "48849 Mark Drive Apt. 177 Port Nicholaschester, NV 57218", "2003-11-09")]
        columns = ["first_name", "last_name", "address", "date_of_birth"]
        df = self.spark.createDataFrame(data, columns)

        # Apply anonymization using UDF
        df_anonymized = df.withColumn("first_name", replace_with_random_characters_udf(df["first_name"])) \
                          .withColumn("last_name", replace_with_random_characters_udf(df["last_name"])) \
                          .withColumn("address", replace_with_random_characters_udf(df["address"]))

        # Collect results
        result = df_anonymized.collect()[0]

        print("Original Data:")
        print(f"First Name: James")
        print(f"Last Name: Ramirez")
        print(f"Address: 48849 Mark Drive Apt. 177 Port Nicholaschester, NV 57218")
        
        print("\nAnonymized Data:")
        print(f"First Name: {result['first_name']}")
        print(f"Last Name: {result['last_name']}")
        print(f"Address: {result['address']}")

        # Check that the anonymized data is different from the original
        self.assertNotEqual(result["first_name"], "James")
        self.assertNotEqual(result["last_name"], "Ramirez")
        self.assertNotEqual(result["address"], "48849 Mark Drive Apt. 177 Port Nicholaschester, NV 57218")

        # Additional checks to ensure that anonymized values are indeed random
        self.assertEqual(len(result["first_name"]), len("James"))
        self.assertEqual(len(result["last_name"]), len("Ramirez"))
        self.assertEqual(len(result["address"]), len("48849 Mark Drive Apt. 177 Port Nicholaschester, NV 57218"))

if __name__ == '__main__':
    unittest.main()
