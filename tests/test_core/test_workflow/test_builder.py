#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_workflow/test_builder.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 07:21:43 pm                                               #
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
from mlops_lab.core.workflow.dag import DAG, Task
from mlops_lab.core.workflow.orchestrator import Orchestrator
from mlops_lab.core.workflow.operator.base import Operator
from mlops_lab.core.workflow.builder import Director, DAGBuilder

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"
config_filepath = "tests/data/datasource.yml"


@pytest.mark.dsb
class TestDataSourceBuilder:  # pragma: no cover
    def config(self) -> dict:
        return IOService.read(config_filepath)

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
        dba = container.dba.dag()
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
        builder = DAGBuilder()
        dir = Director()
        dir.builder = builder
        dag = dir.build_dag(self.config())
        assert isinstance(dag, DAG)
        assert len(dag) == 3
        for task in dag.tasks.values():
            assert isinstance(task, Task)
            assert isinstance(task.operator, Operator)
            assert task.operator.__class__.__name__ == "DataSourceLoader"
        # ---------------------------------------------------------------------------------------- #
        # Test Execution
        logger.debug("\n\n40*'='")
        uow = container.work.unit()
        pipe = Orchestrator(uow=uow)
        logger.debug(f"\n\n{100*'='}")
        logger.debug(dag)
        for task in dag.tasks.values():
            logger.debug(task)
        pipe.dag = dag
        pipe.run()
        uow.save()
        repo = uow.get_repo("datasource")
        events = uow.get_repo("event")
        logger.debug("\n40 * -")
        logger.debug(events.print())
        logger.debug("\n40 * -")
        logger.debug(repo.print())
        logger.debug("\n40*'='")
        # ---------------------------------------------------------------------------------------- #
        dir.builder.reset()
        assert dir.builder.dag is None
        builder = dir.builder
        assert isinstance(builder, DAGBuilder)

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
