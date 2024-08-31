import random
import string

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def replace_with_random_characters(text):
    """Replace each character in the text with a random character."""
    return ''.join(random.choice(string.ascii_letters) for _ in text)

def anonymize_data(df, logger):
    logger.info(f"Starting masking input data ...")

    # UDF to anonymize data by replacing with random characters
    anonymize_udf = udf(replace_with_random_characters, StringType())

    return df.withColumn("first_name", anonymize_udf(df["first_name"])) \
            .withColumn("last_name", anonymize_udf(df["last_name"])) \
            .withColumn("address", anonymize_udf(df["address"]))

