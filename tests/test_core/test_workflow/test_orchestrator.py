#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_workflow/test_orchestrator.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 13th 2023 08:05:33 am                                                #
# Modified   : Tuesday January 24th 2023 08:13:53 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from mlops_lab.core.workflow.orchestrator import Orchestrator
from mlops_lab.core.workflow.dag import DAG

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.pipe
class TestOrchestrator:  # pragma: no cover
    # ============================================================================================ #
    def reset_db(self, container) -> None:
        dba = container.dba.dag()
        dba.reset()
        dba = container.dba.task()
        dba.reset()
        dba = container.dba.event()
        dba.reset()
        dba = container.dba.object()
        dba.reset()

    def test_setup(self, mockdag, container, caplog):
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
        o.dag = mockdag
        assert isinstance(o.dag, DAG)
        assert o.dag.id is not None
        assert o.dag.state == "LOADED"

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
    def test_run(self, dags, container, caplog):
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
        for i, dag in enumerate(dags, start=1):
            o.dag = dag
            o.data = i
            assert o.data == i
            data = o.run()
            assert data == i or (5 + i)
            repo = uow.get_repo("event")
            logger.debug(repo.print())
            repo = uow.get_repo("dag")
            logger.debug(repo.print())
            assert isinstance(o.dag, DAG)
            o.reset()
            assert o.dag is None

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
