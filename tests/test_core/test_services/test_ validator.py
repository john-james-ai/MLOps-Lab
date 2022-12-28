#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_services/test_ validator.py                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 27th 2022 03:32:56 pm                                              #
# Modified   : Tuesday December 27th 2022 05:28:38 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.dataset import Dataset
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
                datasource='spoti',
                stage='extract',
                description='Test Dataset for ' + inspect.stack()[0][3],
            )

        with pytest.raises(ValueError):
            d = Dataset(
                name=inspect.stack()[0][3],
                datasource='spotify',
                stage='ext33',
                description='Test Dataset for ' + inspect.stack()[0][3],
            )

        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Test Dataset for ' + inspect.stack()[0][3],
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
