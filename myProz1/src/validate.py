import datetime
import logging
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger('Validate')

def get_current_date(spark):
    try:
        loggers.warning('Started the get_current_date method.....')

        output = spark.sql("select current_date").collect()

        loggers.warning('Validating spark object with created date time: %s', str(output))

    except Exception as e:
        loggers.warning('An error occurred in get_current_date: %s', str(e))
        raise
    else:
        loggers.warning('Validation done...., go forward.....')
