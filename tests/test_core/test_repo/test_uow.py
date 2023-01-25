#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_repo/test_uow.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 10th 2023 06:03:41 pm                                               #
# Modified   : Tuesday January 24th 2023 08:13:50 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from mlops_lab.core.repo.uow import UnitOfWork
from mlops_lab.core.repo.entity import Repo
from mlops_lab.core.repo.dataset import DatasetRepo
from mlops_lab.core.repo.datasource import DataSourceRepo
from mlops_lab.core.repo.dag import DAGRepo

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.uow
class TestUOW:  # pragma: no cover
    # ============================================================================================ #
    def test_reset(self, container) -> UnitOfWork:
        dba = container.dba.file()
        dba.reset()
        dba = container.dba.object()
        dba.reset()
        uow = container.work.unit()
        assert isinstance(uow, UnitOfWork)
        return uow

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
        uow = self.test_reset(container)
        assert isinstance(uow.get_repo("file"), Repo)
        assert isinstance(uow.get_repo("profile"), Repo)
        assert isinstance(uow.get_repo("dataset"), DatasetRepo)
        assert isinstance(uow.get_repo("datasource"), DataSourceRepo)
        assert isinstance(uow.get_repo("dag"), DAGRepo)
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
    def test_file_w_rollback(self, container, files, caplog):
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
        uow = self.test_reset(container)
        with uow as unit:
            repo = unit.get_repo("file")
            for file in files:
                repo.add(file)

            unit.rollback()

        for i in range(1, 6):
            assert not repo.exists(i)

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
    def test_file_w_exit_context(self, container, files, caplog):
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
        uow = self.test_reset(container)
        with uow as unit:
            repo = unit.get_repo("file")
            for file in files:
                repo.add(file)

        for i in range(1, 6):
            assert repo.exists(i)

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
