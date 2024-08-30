from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def anonymize_data(input_path, output_path):
    spark = SparkSession.builder.appName("Data Anonymizer").getOrCreate()
    df = spark.read.csv(input_path, header=True)
    
    # Anonymize first_name, last_name, and address by adding 'XXXX' to the middle part of the string
    df = df.withColumn("first_name", expr("concat(substr(first_name, 1, 1), 'XXXX', substr(first_name, 5, length(first_name) - 4))")) \
           .withColumn("last_name", expr("concat(substr(last_name, 1, 1), 'XXXX', substr(last_name, 5, length(last_name) - 4))")) \
           .withColumn("address", expr("concat(substr(address, 1, 5), 'XXXX', substr(address, 9, length(address) - 8))"))
    
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Anonymize CSV data.")
    parser.add_argument('--input', type=str, required=True, help="Input file path.")
    parser.add_argument('--output', type=str, required=True, help="Output file path.")
    
    args = parser.parse_args()
    anonymize_data(args.input, args.output)
