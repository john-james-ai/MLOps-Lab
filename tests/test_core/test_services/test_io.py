#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_io.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 29th 2022 08:25:15 pm                                              #
# Modified   : Tuesday November 29th 2022 11:56:09 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
from datetime import datetime
import pytest
import logging
from logging import config

from recsys.core.services.io import IOService

from recsys.config.log import test_log_config

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.io
class TestIO:  # pragma: no cover
    # ============================================================================================ #
    def test_csv(self, ratings, caplog):
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
        FILEPATH = "data/working/test/io/ratings.csv"
        IOService.write(filepath=FILEPATH, data=ratings)
        df = IOService.read(filepath=FILEPATH)
        logger.debug("\n\nRatings")
        logger.debug(ratings.info())
        logger.debug(ratings.head())
        logger.debug("\n\nDataFrame")
        logger.debug(df.info())
        logger.debug(df.head())
        assert df.equals(ratings)
        assert os.path.exists(FILEPATH)

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
    def test_pickle(self, ratings, caplog):
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
        FILEPATH = "data/working/test/io/ratings.pkl"
        IOService.write(filepath=FILEPATH, data=ratings)
        df = IOService.read(filepath=FILEPATH)
        assert df.equals(ratings)
        assert os.path.exists(FILEPATH)

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
    def test_yaml(self, caplog):
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
        d = {"some": "dictionary", "for": "testing", "yaml": "io"}
        FILEPATH = "data/working/test/io/yaml_test.yml"
        IOService.write(filepath=FILEPATH, data=d)
        d2 = IOService.read(filepath=FILEPATH)
        assert d == d2
        assert os.path.exists(FILEPATH)
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
    def test_io_service_methods(self, caplog):
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
        formats = IOService().file_formats
        assert isinstance(formats, list)
        FILEPATH = "data/working/test/io/yaml_test.234"
        with pytest.raises(TypeError):
            IOService().read(filepath=FILEPATH)
        FILEPATH = None
        with pytest.raises(TypeError):
            IOService().read(filepath=FILEPATH)
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
