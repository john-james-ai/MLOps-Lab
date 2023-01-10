#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Datasetname   : /tests/test_core/test_repo/test_dataset_repo.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 1st 2023 02:21:02 pm                                                 #
# Modified   : Tuesday January 10th 2023 01:22:05 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import pandas as pd
import logging

from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.repo.dataset import DatasetRepo
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}\n"


@pytest.mark.repo
@pytest.mark.dataset_repo
class TestDatasetRepo:  # pragma: no cover

    def reset_db(self, container) -> None:
        dba = container.dba.dataset()
        dba.reset()
        dba = container.dba.dataframe()
        dba.reset()
        dba = container.dba.object()
        dba.reset()

    # ============================================================================================ #
    def test_setup(self, container, caplog):
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
        self.reset_db(container)
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
    def test_xaction_insert_no_commit(self, container, context, datasets, caplog):
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
        repo = DatasetRepo(context)
        context.begin()
        for f1 in datasets.copy():
            f2 = repo.add(f1)
            assert repo.exists(f2.id)
        context.close()

        for i in range(1, 6):
            assert not repo.exists(i)

        self.reset_db(container)
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
    def test_xaction_insert_commit(self, container, context, datasets, caplog):
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
        repo = DatasetRepo(context)
        context.begin()
        ids = []
        for f3 in datasets.copy():
            f4 = repo.add(f3)
            assert repo.exists(f4.id)
            ids.append(f4.id)
        context.save()
        context.close()

        for i in ids:
            assert repo.exists(i)
        self.reset_db(container)
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
    def test_xaction_update(self, container, context, datasets, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        context.begin()
        for i, dataset in enumerate(datasets, start=1):
            f1 = repo.get(i)
            f1.task_id = 99 + i
            repo.update(f1)
            f2 = repo.get(i)
            assert f2.task_id == i + 99
        context.close()

        for i in range(1, 6):
            f3 = repo.get(i)
            assert not f3.task_id == i + 99

        context.begin()
        for i in range(1, 6):
            f4 = repo.get(i)
            f4.task_id = 99 + i
            repo.update(f4)
            f5 = repo.get(i)
            assert f5.task_id == i + 99
        context.save()
        context.close()

        for i in range(1, 6):
            f6 = repo.get(i)
            assert f6.task_id == i + 99

        self.reset_db(container)
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
    def test_xaction_delete(self, container, context, datasets, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        context.begin()
        for i in range(1, 6):
            repo.remove(i)
            assert not repo.exists(i)
        context.rollback()
        context.close()

        for i in range(1, 6):
            assert repo.exists(i)

        context.begin()
        for i in range(1, 6):
            repo.remove(i)
            assert not repo.exists(i)
        context.save()
        context.close()

        for i in range(1, 6):
            assert not repo.exists(i)

        self.reset_db(container)
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
    def test_add_get(self, context, container, datasets, caplog):
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
        repo = DatasetRepo(context)
        for i, dataset in enumerate(datasets, start=1):
            dataset = repo.add(dataset)
            assert dataset.id == i
            f2 = repo.get(i)
            assert dataset == f2
            for df in f2.dataframes.values():
                assert isinstance(df, DataFrame)
                assert isinstance(df.data, pd.DataFrame)
                assert df.nrows > 100
                assert df.ncols > 3
                assert df.parent == f2

        self.reset_db(container)
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
    def test_get_by_name(self, container, datasets, context, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        dataset = repo.get_by_name_mode(name="dataset_name_2")
        assert isinstance(dataset, Dataset)
        assert dataset.id == 2
        assert dataset.name == "dataset_name_2"
        assert dataset.mode == 'test'
        for df in dataset.dataframes.values():
            assert isinstance(df, DataFrame)
            assert isinstance(df.data, pd.DataFrame)
            assert df.nrows > 100
            assert df.ncols > 3
            assert df.parent == dataset

        self.reset_db(container)
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
    def test_update(self, container, context, datasets, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        for i in range(1, 6):
            dataset = repo.get(i)
            dataset.task_id = 1234
            repo.update(dataset)

        for i in range(1, 6):
            dataset = repo.get(i)
            assert dataset.task_id == 1234

        self.reset_db(container)
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
    def test_remove_exists(self, container, datasets, context, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        for i in range(1, 6):
            if i % 2 == 0:
                repo.remove(i)
        assert len(repo) < 5

        for i in range(1, 6):
            if i % 2 == 0:
                assert repo.get(i) == []
                assert repo.exists(i) is False
            else:
                assert repo.exists(i)

        self.reset_db(container)
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
    def test_print(self, context, datasets, caplog):
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
        repo = DatasetRepo(context)

        for dataset in datasets:
            repo.add(dataset)

        repo.print()
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
