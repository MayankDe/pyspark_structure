import logging
import logging.config
from pyspark.sql import DataFrame
logging.config.fileConfig('properties/configuration/logging.config')
logger = logging.getLogger('Ingest')

def load_file(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_file method is started.....')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format) \
                .option("header", header) \
                .option("inferSchema", inferSchema) \
                .load(file_dir)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    except Exception as e:
        logger.error("An error occurred at load_file === %s", str(e))
        raise
    else:
        logger.warning("Dataframe created successfully...! and format is: %s", file_format)

    return df

def display_df(df, df_name):
    try:
        logger.warning(f"Showing Dataframe {df_name}:{df.columns}")
        df_name = df.show()

    except Exception as e:
        raise

    return df_name

def count_df(df,df_name):
    try:
        logger.warning(f"number of records in dataframe : {df_name}")
        df_records = df.count()

    except Exception as e:
        raise
    else:
        logger.warning(f"Numbers of records present in {df_name} are {df_records} ")
