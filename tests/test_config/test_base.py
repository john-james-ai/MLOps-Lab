#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_base.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 26th 2022 02:15:34 am                                             #
# Modified   : Saturday November 26th 2022 03:44:04 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
from dataclasses import dataclass
from recsys.config.log import test_log_config
from recsys.config.log import DebugAndInfoOnly, ErrorsOnly

# ------------------------------------------------------------------------------------------------ #
from logging import config

config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Record:
    levelno: int


@pytest.mark.config
class TestConfig:
    # ============================================================================================ #
    def test_base_config(self, cfg, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        c = cfg.as_dict()
        assert len(c) == 5
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_config_for_log(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        record = Record(20)
        dni = DebugAndInfoOnly()
        eo = ErrorsOnly()
        assert dni.filter(record) is True
        assert eo.filter(record) is False

        record = Record(31)
        dni = DebugAndInfoOnly()
        eo = ErrorsOnly()
        assert dni.filter(record) is False
        assert eo.filter(record) is True

        record = Record(52)
        dni = DebugAndInfoOnly()
        eo = ErrorsOnly()
        assert dni.filter(record) is False
        assert eo.filter(record) is False

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )
