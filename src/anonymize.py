from load import *
from utils import *
from helper import anonymize_data

def main(input_path, output_path):
    spark = None
    logger = setup_logger()

    try:
        spark = create_spark_session(logger)
        df = load_data(spark, input_path, logger)
        
        logger.info(f"Input Data")
        logger.info(f"===========")
        logger.info(df.show())
 
        # Anonymize first_name, last_name, and address
        final_df = anonymize_data(df, logger)

        logger.info(f"Anonymised Data")
        logger.info("=================")
        logger.info(final_df.show())

        write_output(df=final_df, output_dir=output_path, feature_type='anonymize', logger=logger)
    
    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")
        sys.exit(1)    
    finally:
        if spark:
            logging.info("Stopping spark session ...")
            spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Anonymize CSV data.")
    parser.add_argument('--input', type=str, required=True, help="Input file path.")
    parser.add_argument('--output', type=str, required=True, help="Output file path.")
    
    args = parser.parse_args()
    main(args.input, args.output)
