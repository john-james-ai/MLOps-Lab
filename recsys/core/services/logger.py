#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /logger.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 03:21:56 pm                                               #
# Modified   : Monday November 14th 2022 11:26:24 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import functools
import logging

from atelier.utils.datetimes import Timer

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    filename="logs/recsys.log",
    filemode="a",
    force=True,
)
# ------------------------------------------------------------------------------------------------ #


def logger(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        module = func.__module__
        classname = func.__qualname__

        try:
            timer = Timer()
            timer.start()
            log_start(module, classname, timer)
            result = func(self, *args, **kwargs)
            timer.stop()
            log_end(module, classname, timer)
            return result

        except Exception as e:
            logger = logging.getLogger(module)
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper

    def log_start(module: str, classname: str, timer: Timer):

        date = timer.started.strftime("%m/%d/%Y")
        time = timer.stopped.strftime("%H:%M:%S")

        msg = "Started {} at {} on {}".format(classname, time, date)
        logger = logging.getLogger(module)

        logger.info(msg)

    def log_end(module: str, classname: str, timer: Timer):

        date = timer.stopped.strftime("%m/%d/%Y")
        time = timer.stopped.strftime("%H:%M:%S")
        duration = timer.duration.as_string()

        msg = "Completed {} at {} on {}. Duration: {}.".format(classname, time, date, duration)
        logger = logging.getLogger(module)

        logger.info(msg)
