import os

import get_env_variables as getenv
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_file,display_df,count_df
from processing import get_processed_df
import logging
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

def main():
    try:
        print("JSG")
        print(getenv.current)
        spark = get_spark_object(getenv.envn, getenv.appName)
        logging.info('I am in the main method')
        logging.info('Object created! %s', spark.version)
        logging.info('Validating spark object')
        get_current_date(spark)
        # print(os.listdir(getenv.src_olap))

        for file in os.listdir(getenv.src_olap):

            file_dir  = getenv.src_olap + '\\' + file
            print(f"File is {file}:{file_dir}")
            if file.endswith(".parquet"):
                file_format ='parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith(".csv"):
                file_format = 'csv'
                header = getenv.header
                inferSchema = getenv.inferSchema
        logging.info("Reading file which is of {} >".format(file_format))

        logging.info("Reading file2 which is of {} >".format(file_format))

        df_Ind = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                           inferSchema=inferSchema)
        logging.info("display df is called >".format(df_Ind))
        display_df(df_Ind, 'df_ind')
        logging.info("display df is called {} >")

        for file2 in os.listdir(getenv.src_oltp):

            file_dir = getenv.src_oltp + '\\' + file2
            print(f"File is {file2}:{file_dir}")
            if file2.endswith(".parquet"):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file2.endswith(".csv"):
                file_format = 'csv'
                header = getenv.header
                inferSchema = getenv.inferSchema


        df_fact = load_file(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                           inferSchema=inferSchema)
        logging.info("display df is called >")
        print('count df')
        display_df(df_fact, 'df_fact')
        logging.info("count df is getting called >")
        count_df(df_fact, 'df_fact')

        logging.info("get procesed df ...........>")
        get_processed_df(df_fact).show()


    except Exception as e:
        logging.error('An error occurred when calling main() method. Please check the trace: %s', str(e))

if __name__ == "__main__":
    main()
