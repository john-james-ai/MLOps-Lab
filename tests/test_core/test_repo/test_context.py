#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_repo/test_context.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 4th 2023 11:14:20 pm                                              #
# Modified   : Friday January 20th 2023 10:16:41 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.dataset import Dataset
from recsys.core.entity.dataset import DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.workflow.process import Job, Task
from recsys.core.entity.file import File
from recsys.core.workflow.profile import Profile
from recsys.core.dal.dao import (
    DatasetDAO,
    DataFrameDAO,
    FileDAO,
    JobDAO,
    TaskDAO,
    DataSourceDAO,
    DataSourceURLDAO,
    ProfileDAO,
)
from recsys.core.dal.dto import DataFrameDTO
from recsys.core.repo.context import Context

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.context
class TestContext:  # pragma: no cover

    # ============================================================================================ #
    def reset_table(self, container):
        dba = container.dba.dataset()
        dba.reset()
        dba = container.dba.dataframe()
        dba.reset()

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
        self.reset_table(container)
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
    def test_get_dao(self, caplog):
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
        cntx = Context()
        dao = cntx.get_dao(Dataset)
        assert isinstance(dao, DatasetDAO)

        dao = cntx.get_dao(DataFrame)
        assert isinstance(dao, DataFrameDAO)

        dao = cntx.get_dao(DataSource)
        assert isinstance(dao, DataSourceDAO)

        dao = cntx.get_dao(DataSourceURL)
        assert isinstance(dao, DataSourceURLDAO)

        dao = cntx.get_dao(Profile)
        assert isinstance(dao, ProfileDAO)

        dao = cntx.get_dao(Job)
        assert isinstance(dao, JobDAO)

        dao = cntx.get_dao(Task)
        assert isinstance(dao, TaskDAO)

        dao = cntx.get_dao(File)
        assert isinstance(dao, FileDAO)

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
    def test_rollback(self, datasets, caplog):
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
        cntx = Context()
        ds_dao = cntx.get_dao(Dataset)
        df_dao = cntx.get_dao(DataFrame)

        cntx.begin()

        for dataset in datasets:
            for name, dataframe in dataset.dataframes.items():
                dataframe = df_dao.create(dataframe.as_dto())
                assert df_dao.exists(dataframe.id)
            dataset = ds_dao.create(dataset.as_dto())
            assert ds_dao.exists(dataset.id)

        for i, dataset in enumerate(datasets, start=1):
            assert ds_dao.exists(i)
            for name, dataframe in dataset.dataframes.items():
                df = df_dao.read_by_name(name, mode="test")
                assert isinstance(df, DataFrameDTO)

        cntx.rollback()

        assert len(df_dao) == 0
        assert len(ds_dao) == 0

        for i, dataset in enumerate(datasets, start=1):
            assert not ds_dao.exists(i)
            for name, dataframe in dataset.dataframes.items():
                df = df_dao.read_by_name(name, mode="test")
                assert not isinstance(df, DataFrameDTO)

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
    def test_transaction(self, datasets, caplog):
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
        cntx = Context()
        ds_dao = cntx.get_dao(Dataset)
        df_dao = cntx.get_dao(DataFrame)

        cntx.begin()
        assert cntx.in_transaction

        for dataset in datasets:
            for name, dataframe in dataset.dataframes.items():
                dataframe = df_dao.create(dataframe.as_dto())
                assert df_dao.exists(dataframe.id)
            dataset = ds_dao.create(dataset.as_dto())
            assert ds_dao.exists(dataset.id)

        cntx.save()
        cntx.close()

        assert not cntx.in_transaction

        cntx.begin()
        for dataset in datasets:
            for name, dataframe in dataset.dataframes.items():
                df = df_dao.read_by_name(name, mode="test")
                assert df_dao.exists(df.id)
            ds = ds_dao.read_by_name(name=dataset.name, mode=dataset.mode)
            assert ds_dao.exists(ds.id)
        cntx.close()
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
