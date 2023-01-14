#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_workflow/test_orchestrator.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 13th 2023 08:05:33 am                                                #
# Modified   : Saturday January 14th 2023 04:53:43 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.workflow.orchestrator import Orchestrator
from recsys.core.entity.job import Job

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.pipe
class TestOrchestrator:  # pragma: no cover
    # ============================================================================================ #
    def reset_db(self, container) -> None:
        dba = container.dba.job()
        dba.reset()
        dba = container.dba.task()
        dba.reset()
        dba = container.dba.event()
        dba.reset()
        dba = container.dba.object()
        dba.reset()

    def test_setup(self, mockjob, container, caplog):
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
        self.reset_db(container)
        uow = container.work.unit()
        o = Orchestrator(uow=uow)
        o.job = mockjob
        assert isinstance(o.job, Job)
        assert o.job.id is not None
        assert o.job.state == "LOADED"

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

    # ============================================================================================ #
    def test_run(self, jobs, container, caplog):
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
        self.reset_db(container)
        uow = container.work.unit()
        o = Orchestrator(uow=uow)
        for i, job in enumerate(jobs, start=1):
            o.job = job
            o.data = i
            assert o.data == i
            data = o.run()
            assert data == i or (5 + i)
            repo = uow.get_repo("event")
            logger.debug(repo.print())
            repo = uow.get_repo("job")
            logger.debug(repo.print())
            assert isinstance(o.job, Job)
            o.reset()
            assert o.job is None

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
