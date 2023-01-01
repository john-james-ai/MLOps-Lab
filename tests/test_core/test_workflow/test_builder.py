#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_workflow/test_builder.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 07:21:43 pm                                               #
# Modified   : Saturday December 31st 2022 08:47:36 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.workflow.builder import Director, PipelineBuilder
from recsys.core.workflow.pipeline import Pipeline
from recsys.core.dal.uow import UnitOfWork

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
CONFIG_FILEPATH = "recsys/setup/config.yml"


@pytest.mark.builder
class TestBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_build(self, caplog):
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
        uow = UnitOfWork()
        director = Director(uow=uow)
        director.builder = PipelineBuilder()
        director.build_pipeline(config_filepath=CONFIG_FILEPATH)
        pipeline = director._pipeline
        assert isinstance(director.builder, PipelineBuilder)
        assert isinstance(director, Director)
        assert isinstance(pipeline, Pipeline)

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
    def test_pipeline(self, container, caplog):
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
        container.table.job().reset()
        uow = UnitOfWork(context=container.context.context, repos=container.repos)
        logger.debug(uow)
        logger.debug(uow.job)
        director = Director(uow=uow)
        director.builder = PipelineBuilder()
        pipeline = director.build_pipeline(config_filepath=CONFIG_FILEPATH)
        pipeline.run()

        assert len(uow.current_job) == 3

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
