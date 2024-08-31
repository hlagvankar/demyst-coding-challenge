from utils import read_csv

def load_data(spark, input_dir, logger):
    try:
        return read_csv(spark, input_dir)        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise