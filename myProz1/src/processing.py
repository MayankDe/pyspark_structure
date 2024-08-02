import logging
import logging.config
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


logging.config.fileConfig('properties/configuration/logging.config')
logger = logging.getLogger('Processing')


def get_processed_df(df: DataFrame):
    """
    Drop null value if columns are None then it will look all dataframe
    :param df: DataFrame which we have to check null

    """
    try:
        logger.warning("the DataFrame processing.")

        clean_df = df.withColumnRenamed("admin_name","state")\
                    .drop("admin")\
                    .groupBy("state").agg(sum(col("population")))


        logger.info(f"Successfully processed: {clean_df.count()}")

    except Exception as e:
        logger.error(f"An error occurred while dropping null values: {str(e)}")
        raise
    else:
        logger.warning("df processed successfully.")

    return clean_df
