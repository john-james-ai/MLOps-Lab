#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_services/test_ validator.py                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 27th 2022 03:32:56 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:53 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from mlops_lab.core.entity.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.validator
class TestValidator:  # pragma: no cover
    # ============================================================================================ #
    def test_validator(self, caplog):
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
            d = Dataset(
                name=inspect.stack()[0][3],
                datasource="spoti",
                stage="extract",
                description="Test Dataset for " + inspect.stack()[0][3],
            )

        with pytest.raises(ValueError):
            d = Dataset(
                name=inspect.stack()[0][3],
                datasource="spotify",
                stage="ext33",
                description="Test Dataset for " + inspect.stack()[0][3],
            )

        d = Dataset(
            name=inspect.stack()[0][3],
            datasource="spotify",
            stage="interim",
            description="Test Dataset for " + inspect.stack()[0][3],
        )
        assert isinstance(d, Dataset)

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
