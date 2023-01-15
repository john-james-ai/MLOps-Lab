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
# Modified   : Saturday January 14th 2023 09:05:12 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.job import Job, Task
from recsys.core.workflow.orchestrator import Orchestrator
from recsys.core.workflow.operator.base import Operator
from recsys.core.factory.base import Director, JobBuilder

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"
config_filepath = "recsys/data/etl/datasources.yml"


@pytest.mark.dsb
class TestDataSourceBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dba = container.dba.job()
        dba.reset()
        dba = container.dba.task()
        dba.reset()
        dba = container.dba.event()
        dba.reset()
        dba = container.dba.object()
        dba.reset()
        dba = container.dba.datasource()
        dba.reset()
        dba = container.dba.datasource_url()
        dba.reset()

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
        logger.info(single_line)

    # ============================================================================================ #
    def test_dsb(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Test Build
        builder = JobBuilder()
        dir = Director()
        dir.builder = builder
        job = dir.build_job()
        assert isinstance(job, Job)
        assert len(job) == 3
        for task in job.tasks.values():
            assert isinstance(task, Task)
            assert isinstance(task.operator, Operator)
            assert task.operator.name == "DataSourceLoader"
        # ---------------------------------------------------------------------------------------- #
        # Test Execution
        uow = container.work.unit()
        pipe = Orchestrator(uow=uow)
        pipe.job = job
        pipe.run()
        repo = uow.get_repo("datasource")
        logger.debug(repo.print())
        # ---------------------------------------------------------------------------------------- #
        dir.builder.reset()
        assert dir.builder.job is None
        builder = dir.builder
        assert isinstance(builder, JobBuilder)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\nCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)
