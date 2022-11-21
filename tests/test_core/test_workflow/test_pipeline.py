#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_pipeline.py                                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 19th 2022 07:42:55 pm                                             #
# Modified   : Sunday November 20th 2022 12:45:40 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pandas as pd
import pytest
import logging
import shutil

from recsys.core.dal.dataset import get_id, Dataset
from recsys.core.dal.repo import DatasetRepo
from recsys.core.services.io import IOService
from recsys.core.dal.registry import FileBasedRegistry
from recsys.core.workflow.pipeline import PipelineBuilder, PipelineDirector, Pipeline, Context


# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    force=True,
)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
CONFIG_FILEPATH = "tests/config/process.yaml"
DIRECTORY = "tests/data/workflow"
BUILDER = PipelineBuilder()
DIRECTOR = PipelineDirector(config_filepath=CONFIG_FILEPATH, builder=BUILDER)
PIPELINE = BUILDER.pipeline
REGISTRY = FileBasedRegistry(directory=DIRECTORY, io=IOService)
REPO = DatasetRepo()
REPO.registry = REGISTRY
REPO.io = IOService


@pytest.mark.pipe
class TestPipeDirectorBuilder:
    # ============================================================================================ #
    def test_setup(self, caplog):
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
        shutil.rmtree(DIRECTORY, ignore_errors=True)
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

    # ================================================================================================ #
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
        DIRECTOR.build_pipeline()
        assert isinstance(BUILDER.config, dict)
        assert isinstance(BUILDER.context, Context)
        assert isinstance(BUILDER.steps, dict)

        PIPELINE = BUILDER.pipeline
        assert isinstance(PIPELINE, Pipeline)
        assert PIPELINE.steps == DIRECTOR.builder.steps
        assert PIPELINE.context == DIRECTOR.builder.context
        assert len(PIPELINE.steps) == 1

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

    # ================================================================================================ #
    def test_teardown(self, caplog):
        # Enter teardown activities here
        pass


@pytest.mark.pipe
class TestPipeline:
    DIRECTORY = "tests/data/workflow"
    REGISTRY = FileBasedRegistry(directory=DIRECTORY, io=IOService)
    REPO = DatasetRepo()
    REPO.registry = REGISTRY
    REPO.io = IOService

    # ================================================================================================ #
    def test_create_rating_dataset(self, caplog):
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
        builder = DIRECTOR.builder
        pipeline = builder.pipeline
        pipeline.run()
        data = pipeline.data
        assert isinstance(data, Dataset)
        assert isinstance(data.data, pd.DataFrame)
        id = get_id(name="rating", env="test", version=1, stage="interim")
        assert REPO.exists(id)

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
    # def test_split(self, caplog):
    #     start = datetime.now()
    #     logger.info(
    #         "\n\tStarted {} {} at {} on {}".format(
    #             self.__class__.__name__,
    #             inspect.stack()[0][3],
    #             start.strftime("%H:%M:%S"),
    #             start.strftime("%m/%d/%Y"),
    #         )
    #     )
    #     # ---------------------------------------------------------------------------------------- #
    #     id1 = get_id(name="train", env="test", stage="interim", version=1)
    #     id2 = get_id(name="test", env="test", stage="interim", version=1)
    #     assert REPO.exists(id1)
    #     assert REPO.exists(id2)
    #     # ---------------------------------------------------------------------------------------- #
    #     end = datetime.now()
    #     duration = round((end - start).total_seconds(), 1)

    #     logger.info(
    #         "\n\tCompleted {} {} in {} seconds at {} on {}".format(
    #             self.__class__.__name__,
    #             inspect.stack()[0][3],
    #             duration,
    #             end.strftime("%H:%M:%S"),
    #             end.strftime("%m/%d/%Y"),
    #         )
    #     )

    # ================================================================================================ #
    def test_teardown(self, caplog):
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
        pass
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
