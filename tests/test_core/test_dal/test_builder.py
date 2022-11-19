#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_builder.py                                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 15th 2022 01:05:26 pm                                              #
# Modified   : Friday November 18th 2022 08:23:37 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from recsys.core.dal.registry import FileBasedRegistry
from recsys.core.dal.config import DatasetRepoConfigFR
from recsys.core.dal.repo import DatasetRepo, DatasetRepoBuilderFR, DatasetRepoDirector
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    force=True,
)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.builder
class TestRepoBuilder:
    def test_dataset_repo_builder(self, caplog):
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

        b = DatasetRepoBuilderFR()
        b.build_config(config=DatasetRepoConfigFR(test=True))
        b.build_registry()
        b.build_repo()
        repo = b.repo
        assert isinstance(repo, DatasetRepo)
        assert isinstance(repo.io, type(IOService))
        assert isinstance(repo.registry, FileBasedRegistry)

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


@pytest.mark.builder
class TestRepoDirector:
    def test_director(self, caplog):
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

        config = DatasetRepoConfigFR()
        builder = DatasetRepoBuilderFR()
        director = DatasetRepoDirector(config, builder)
        director.build_dataset_repo_with_file_registry()
        repo = builder.repo
        assert isinstance(repo, DatasetRepo)
        assert isinstance(repo.io, type(IOService))
        assert isinstance(repo.registry, FileBasedRegistry)

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
