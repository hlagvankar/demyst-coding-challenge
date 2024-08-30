import pandas as pd
from faker import Faker
import argparse

def generate_csv(file_path, num_rows):
    fake = Faker()

    # Generate data with Faker
    data = [(fake.first_name(), fake.last_name(), fake.address(), fake.date_of_birth().isoformat()) for _ in range(num_rows)]

    # Create a Pandas DataFrame
    df = pd.DataFrame(data, columns=["first_name", "last_name", "address", "date_of_birth"])

    # Save DataFrame as CSV
    df.to_csv(file_path, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a CSV file with fake data.")
    parser.add_argument('--output', type=str, required=True, help="Output file path.")
    parser.add_argument('--rows', type=int, required=True, help="Number of rows to generate.")

    args = parser.parse_args()
    generate_csv(args.output, args.rows)
