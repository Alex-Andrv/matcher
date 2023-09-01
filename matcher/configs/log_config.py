import logging

from matcher.configs.general_bot_config import VERSION

LOG_LEVEL = logging.INFO
LOG_FILENAME = "logs/" + VERSION + "-log.log"
LOG_FILEMODE = "a"
LOG_FORMAT = "%(module)s - %(filename)s - %(funcName)s - %(lineno)d : %(name)s : %(asctime)s : %(levelname)s : %(" \
             "message)s "