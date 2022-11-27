#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /log.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 07:33:15 pm                                               #
# Modified   : Saturday November 26th 2022 11:19:20 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging

# ------------------------------------------------------------------------------------------------ #


class DebugAndInfoOnly(logging.Filter):
    """Filters all messages except Info and Debug"""

    def filter(self, record):
        if record.levelno <= logging.INFO:
            return True
        else:
            return False


class ErrorsOnly(logging.Filter):
    """Filters all messages except Error"""

    def filter(self, record):
        if record.levelno == logging.ERROR:
            return True
        else:
            return False


log_config = {
    "version": 1,
    "handlers": {
        "console_handler": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG",
        },
        "debug_info_handler": {
            "formatter": "file_formatter",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filters": ["debug_&_info"],
            "when": "midnight",
            "interval": 1,
            "backupCount": 0,
            "level": "DEBUG",
            "filename": "logs/debug_info.log",
        },
        "errors_handler": {
            "formatter": "file_formatter",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filters": ["errors"],
            "when": "midnight",
            "interval": 1,
            "backupCount": 0,
            "level": "DEBUG",
            "filename": "logs/errors.log",
        },
        "test_handler": {
            "formatter": "file_formatter",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "midnight",
            "interval": 1,
            "backupCount": 0,
            "level": "DEBUG",
            "filename": "logs/test.log",
        },
    },
    "formatters": {
        "std_out": {
            "format": "%(levelname)s : %(name)s : %(funcName)s : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        },
        "file_formatter": {
            "format": "%(levelname)s : %(asctime)s : %(name)s : %(module)s : %(funcName)s : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        },
    },
    "filters": {
        "debug_&_info": {"()": DebugAndInfoOnly},
        "errors": {"()": ErrorsOnly},
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["console_handler", "debug_info_handler", "errors_handler"],
    },
    "disable_existing_loggers": False,
}

test_log_config = {
    "version": 1,
    "handlers": {
        "console_handler": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG",
        },
        "test_handler": {
            "formatter": "file_formatter",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "midnight",
            "interval": 1,
            "backupCount": 0,
            "level": "DEBUG",
            "filename": "logs/test.log",
        },
    },
    "formatters": {
        "std_out": {
            "format": "%(levelname)s : %(name)s : %(funcName)s : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        },
        "file_formatter": {
            "format": "%(levelname)s : %(asctime)s : %(name)s : %(module)s : %(funcName)s : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        },
    },
    "filters": {
        "debug_&_info": {"()": DebugAndInfoOnly},
        "errors": {"()": ErrorsOnly},
    },
    "root": {
        "handlers": ["console_handler", "test_handler"],
        "level": "DEBUG",
    },
    "disable_existing_loggers": False,
}
