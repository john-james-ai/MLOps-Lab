#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_services/test_io.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 10th 2022 09:17:57 pm                                             #
# Modified   : Tuesday January 24th 2023 08:13:53 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from mlops_lab.core.service.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
CSV_FILEPATH = "tests/data/movielens25m/raw/movies.csv"
CSV_FILEPATH2 = "tests/data/movielens25m/raw/movies2.csv"
EXCEL_FILEPATH = "tests/data/initialize.xlsx"
EXCEL_FILEPATH2 = "tests/data/initialize2.xlsx"
PICKLE_FILEPATH = "tests/data/movielens25m/raw/movies.pkl"
PICKLE_FILEPATH2 = "tests/data/movielens25m/raw/movies2.pkl"
YAML_FILEPATH = "tests/data/config.yml"
YAML_FILEPATH2 = "tests/data/config2.yml"

BAD_CSV = "tests/data/bad.csv"
BAD_YAML = "tests/data/bad.yml"
BAD_EXCEL = "tests/data/bad.xlsx"
BAD_PICKLE = "tests/data/bad.pkl"


@pytest.mark.io
class TestIO:  # pragma: no cover

    # ============================================================================================ #
    def test_csv(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        data = IOService.read(CSV_FILEPATH)
        IOService.write(filepath=CSV_FILEPATH2, data=data)
        data2 = IOService.read(CSV_FILEPATH2)
        assert data.equals(data2)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_yaml(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        data = IOService.read(YAML_FILEPATH)
        IOService.write(filepath=YAML_FILEPATH2, data=data)
        data2 = IOService.read(YAML_FILEPATH2)
        assert data == data2
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_pickle(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        data = IOService.read(PICKLE_FILEPATH)
        IOService.write(filepath=PICKLE_FILEPATH2, data=data)
        data2 = IOService.read(PICKLE_FILEPATH2)
        assert data.equals(data2)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_excel(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        data = IOService.read(EXCEL_FILEPATH, sheet_name="datasource")
        IOService.write(filepath=EXCEL_FILEPATH2, data=data, sheet_name="datasource")
        data2 = IOService.read(EXCEL_FILEPATH2, sheet_name="datasource")
        assert data.equals(data2)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_exceptions(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = IOService.read(filepath=None)

        with pytest.raises(ValueError):
            _ = IOService.read(filepath="tests/data/some.jpg")
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
