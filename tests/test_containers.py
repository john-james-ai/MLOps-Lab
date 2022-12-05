#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_containers.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 04:43:50 pm                                                #
# Modified   : Sunday December 4th 2022 05:19:41 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from dependency_injector import containers, providers

from recsys.containers import CoreContainer, DataLayerContainer, DataAccessLayerContainer, Recsys
import tests.containers  # noqa F401

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.containers
class TestContainers:  # pragma: no cover
    # ============================================================================================ #
    def test_containers(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        recsys = Recsys()
        recsys.core.init_resources()
        assert isinstance(recsys.core(), containers.DynamicContainer)
        assert isinstance(recsys.data(), containers.DynamicContainer)
        assert isinstance(recsys.dal(), containers.DynamicContainer)

        # ---------------------------------------------------------------------------------------- #
        core = CoreContainer()
        assert isinstance(core.logging, providers.Resource)

        # ---------------------------------------------------------------------------------------- #
        data = DataLayerContainer()
        assert isinstance(data.connection, providers.Singleton)
        assert isinstance(data.database, providers.Singleton)

        # ---------------------------------------------------------------------------------------- #
        dal = DataAccessLayerContainer()
        assert isinstance(dal.database, providers.Dependency)
        assert isinstance(dal.dataset_table, providers.Factory)
        assert isinstance(dal.dataset_dao, providers.Singleton)
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
