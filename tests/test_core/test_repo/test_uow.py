#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_repo/test_uow.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 10th 2023 06:03:41 pm                                               #
# Modified   : Tuesday January 10th 2023 07:29:36 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.repo.uow import UnitOfWork
from recsys.core.entity.file import File
from recsys.core.entity.profile import Profile
from recsys.core.repo.entity import Repo
from recsys.core.repo.dataset import DatasetRepo
from recsys.core.repo.datasource import DataSourceRepo
from recsys.core.repo.job import JobRepo

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
        uow = container.repo.uow()
        assert isinstance(uow, UnitOfWork)
        uow.register(name="file", repo=Repo, entity=File)
        uow.register(name="profile", repo=Repo, entity=Profile)
        uow.register(name="dataset", repo=DatasetRepo)
        uow.register(name="datasource", repo=DataSourceRepo)
        uow.register(name="job", repo=JobRepo)
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
        assert isinstance(uow.get_repo("job"), JobRepo)
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
    def test_file_wo_save(self, container, files, caplog):
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
        repo = uow.get_repo('file')
        for file in files:
            repo.add(file)

        uow.rollback()

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
    def test_file_w_save(self, container, files, caplog):
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
        repo = uow.get_repo('file')
        for file in files:
            repo.add(file)

        uow.save()

        # No save, should not have committed to database.
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
