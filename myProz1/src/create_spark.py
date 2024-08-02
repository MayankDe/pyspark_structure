from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')
logger = logging.getLogger('Create_spark')

def get_spark_object(envn, appName):
    try:
        logger.info('get_spark_object method started.....')
        if envn == 'DEV':
            master = 'local[*]'
        else:
            master = 'yarn'

        logger.info('master is {}'.format(master))
        spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
        return spark
    except Exception as e:
        logger.error('An error occurred in the get_spark_object method: %s', str(e))
        raise
    else:
        logger.info("Spark object created...!")

