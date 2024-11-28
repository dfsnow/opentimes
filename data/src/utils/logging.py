import logging


def create_logger(name: str) -> logging.Logger:
    """
    Set up a Python logger with the same format as Valhalla logs.

    Args:
        name: Module name to use for the logger.

    Returns:
        logging.Logger: Generic logger with the same log format as Valhalla.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)06d [%(levelname)s] %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
