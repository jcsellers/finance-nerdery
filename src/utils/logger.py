import logging


def get_logger(name, log_level="INFO"):
    """
    Get a standardized logger for the pipeline.

    Args:
        name (str): Name of the logger (usually __name__).
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, log_level.upper(), "INFO"))
    return logger
