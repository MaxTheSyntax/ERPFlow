# Source - https://stackoverflow.com/a/56944256
# Posted by Sergey Pleshakov, modified by community. See post 'Timeline' for change history
# Retrieved 2026-02-03, License - CC BY-SA 4.0

import logging

class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    light_blue = "\x1b[94;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    log_format = (
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    )

    FORMATS = {
        logging.DEBUG: grey + log_format + reset,
        logging.INFO: light_blue + log_format + reset,
        logging.WARNING: yellow + log_format + reset,
        logging.ERROR: red + log_format + reset,
        logging.CRITICAL: bold_red + log_format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# log = None
# def initialize():
#     global log
log = logging.getLogger("My_app")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
log.addHandler(ch)

def debug(msg, **kwargs):
    log.debug(msg, stacklevel=2, **kwargs)
def info(msg, **kwargs):
    log.info(msg, stacklevel=2, **kwargs)
def warning(msg, **kwargs):
    log.warning(msg, stacklevel=2, **kwargs)
def warn(msg, **kwargs):
    log.warning(msg, stacklevel=2, **kwargs)
def error(msg, **kwargs):
    log.error(msg, stacklevel=2, **kwargs)
def critical(msg, **kwargs):
    log.critical(msg, stacklevel=2, **kwargs)
