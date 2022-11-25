#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /profiler.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 03:21:56 pm                                               #
# Modified   : Wednesday November 23rd 2022 11:23:33 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dotenv import load_dotenv
import psutil
import functools
import logging
import wandb


from atelier.utils.datetimes import Timer

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    filename="logs/recsys.log",
    filemode="a",
    force=True,
)
# ------------------------------------------------------------------------------------------------ #


def profiler(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        def log_start(module: str, classname: str, timer: Timer):

            date = timer.started.strftime("%m/%d/%Y")
            time = timer.started.strftime("%H:%M:%S")

            msg = "Started {} at {} on {}".format(classname, time, date)
            logger = logging.getLogger(module)

            logger.info(msg)

        def log_end(module: str, classname: str, timer: Timer, ram_usage, cpu_usage):

            date = timer.stopped.strftime("%m/%d/%Y")
            time = timer.stopped.strftime("%H:%M:%S")
            duration = timer.duration.as_string()

            msg = "\n\t\tCompleted {} at {} on {}. Duration: {}.".format(
                classname, time, date, duration
            )
            msg2 = "\t\tRam used: {} %\tCPU Usage: {} %".format(ram_usage, cpu_usage)
            logger = logging.getLogger(module)

            logger.info(msg)
            logger.info(msg2)

        module = func.__module__
        classname = func.__qualname__

        load_dotenv()
        PROJECT = os.getenv("PROJECT")
        ENTITY = os.getenv("ENTITY")

        try:
            wandb_run = wandb.init(
                project=PROJECT, entity=ENTITY, name=kwargs.get("name", "Operator"), reinit=True
            )
            timer = Timer()
            timer.start()
            log_start(module, classname, timer)

            result = func(self, *args, **kwargs)

            # Obtain profiling data
            total_memory, used_memory, free_memory = map(
                int, os.popen("free -t -m").readlines()[-1].split()[1:]
            )
            load1, load5, load15 = psutil.getloadavg()
            cpu_usage = "{:,}".format(round((load1 / os.cpu_count()) * 100, 2))
            ram_usage = "{:,}".format(round((used_memory / total_memory) * 100, 2))

            timer.stop()
            log_end(
                module=module,
                classname=classname,
                timer=timer,
                ram_usage=ram_usage,
                cpu_usage=cpu_usage,
            )
            wandb.log(
                {
                    "operator": kwargs.get("name", "Operator"),
                    "started": timer.started,
                    "ended": timer.ended,
                    "duration": timer.duration.as_string(),
                    "total_memory (bytes)": "{:,}".format(total_memory),
                    "used_memory (bytes)": "{:,}".format(used_memory),
                    "free_memory (bytes)": "{:,}".format(free_memory),
                    "RAM used (%)": ram_usage,
                    "CPU Usage (%)": cpu_usage,
                }
            )
            wandb_run.finish()
            return result

        except Exception as e:
            logger = logging.getLogger(module)
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper
