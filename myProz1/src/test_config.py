import logging
import logging.config
import sys

def setup_logging():
    try:
        # Load the logging configuration
        logging.config.fileConfig('properties/configuration/logging.config')

        # Get the logger
        logger = logging.getLogger()

        # Test logging
        logger.info('Logging configured successfully.')
        logger.debug('This is a debug message.')
        logger.warning('This is a warning message.')
        logger.error('This is an error message.')
        logger.critical('This is a critical message.')

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    setup_logging()
