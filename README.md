# Data Processing Pipeline

## Overview

This project is a data processing pipeline that generates a CSV file, anonymizes sensitive columns, and ensures scalability using PySpark. The solution is containerized using Docker, with a focus on modularity, testing, and scalability.

## Project Structure

- **`data/`**: Contains input and output data files.
  - `input/`: Stores the generated CSV files.
  - `output/`: Stores the anonymized CSV files.
- **`src/`**: Contains the source code for the project.
  - `generate_csv.py`: Script to generate the initial CSV file.
  - `anonymize.py`: Script to anonymize the sensitive data.
  - `utils.py`: Utility functions shared across scripts.
- **`tests/`**: Contains unit and integration tests.
  - `test_generate_csv.py`: Unit tests for CSV generation.
  - `test_anonymize.py`: Unit tests for data anonymization.
  - `test_integration.py`: Integration tests for the entire pipeline.
- **`Dockerfile`**: Defines the Docker image for the project.
- **`docker-compose.yml`**: (Optional) Defines the setup for distributed computing using Docker Compose.
- **`requirements.txt`**: Lists the Python dependencies.
- **`README.md`**: Project documentation.

## Requirements

- Python 3.8+
- Docker
- PySpark

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/demyst-coding-challenge.git
cd demyst-coding-challenge
```

### 2. Build and Run the Docker Container
```bash
docker build -t data-pipeline .
```

To run the Docker container:
```bash
docker run -v $(pwd)/data:/app/data data-pipeline
```

### 3. Generate the CSV File
The generate_csv.py script generates a CSV file with random data:
```bash
python src/generate_csv.py --output data/input/data.csv --rows 1000000
```
This will create a file named data.csv in the data/input/ directory with the specified number of rows.

### 4. Anonymize the Data
The anonymize.py script anonymizes the first_name, last_name, and address columns:
```bash
python src/anonymize.py --input data/input/data.csv --output data/output/anonymized_data.csv
```
This will create an anonymized CSV file in the data/output/ directory.


### 5. Running the Pipeline on Large Datasets
To demonstrate scalability, use PySpark for distributed processing:
```bash
spark-submit src/anonymize.py --input data/input/data.csv --output data/output/anonymized_data.csv
```

If running on a distributed cluster, configure the spark-submit command accordingly (e.g., --master argument)

### 6. Testing
Unit Tests
To run unit tests:

```bash
pytest tests/
```

Integration Tests
```bash
Integration tests validate the entire pipeline:
```

### 7. Performance Considerations
For larger datasets, consider the following optimizations:

* Partitioning: Use PySpark's partitioning features to optimize performance.
* Memory Management: Adjust memory settings in Spark for large-scale processing.
* I/O Optimization: Consider using Parquet or ORC format for better I/O performance.