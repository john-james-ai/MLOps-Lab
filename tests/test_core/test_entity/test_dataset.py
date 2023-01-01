#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_entity/test_dataset.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 27th 2022 05:33:26 pm                                              #
# Modified   : Sunday January 1st 2023 05:18:31 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pandas as pd
from datetime import datetime
import pytest
import logging

# Import modules to be tested
from recsys.core.entity.dataset import Dataset, DataFrame
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dataset
class TestDataset:  # pragma: no cover
    # ============================================================================================ #
    def test_instantiation(self, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
        )
        assert d.is_composite is True
        assert d.name == inspect.stack()[0][3]
        assert d.datasource == 'spotify'
        assert d.mode == 'test'
        assert d.stage == 'interim'
        assert d.dataframe_count == 0  # No children
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
    def test_instantiation_data(self, ratings, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
        )
        assert d.is_composite is True
        assert d.name == inspect.stack()[0][3]
        assert d.datasource == 'spotify'
        assert d.mode == 'test'
        assert d.stage == 'interim'
        assert d.dataframe_count == 1

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
    def test_instantiation_task(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id
        )
        assert d.dataframe_count == 1

        d.id = 5
        dto = d.as_dto()
        assert dto.id == 5
        assert dto.oid == "dataset_" + str(5)
        assert dto.name == inspect.stack()[0][3]
        assert dto.datasource == 'spotify'
        assert dto.mode == 'test'
        assert dto.stage == 'interim'
        assert dto.task_id == 1

        assert isinstance(dto.created, datetime)

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
    def test_validation(self, ratings, tasks, caplog):
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
        with pytest.raises(ValueError):
            _ = Dataset(
                name=inspect.stack()[0][3],
                datasource='sxpotify',
                stage='interim',
                description='Dataset for ' + inspect.stack()[0][3],
                data=ratings,
                task_id=tasks[0].id
            )

        with pytest.raises(ValueError):
            _ = Dataset(
                name=inspect.stack()[0][3],
                datasource='spotify',
                stage='interixm',
                description='Dataset for ' + inspect.stack()[0][3],
                data=ratings,
                task_id=tasks[0].id
            )

        with pytest.raises(ValueError):
            _ = Dataset(
                name=inspect.stack()[0][3],
                datasource='sxpotify',
                stage='interim',
                description='Dataset for ' + inspect.stack()[0][3],
                data=5,
                task_id=tasks[0].id
            )

        with pytest.raises(ValueError):
            _ = Dataset(
                name=inspect.stack()[0][3],
                datasource='sxpotify',
                stage='interim',
                description='Dataset for ' + inspect.stack()[0][3],
                data=ratings,
                task_id=5,
            )
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
    def test_get_dataframe(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        df = d.get_dataframe(name=inspect.stack()[0][3])
        assert isinstance(df, DataFrame)
        assert d.dataframe_count == 1

        with pytest.raises(FileNotFoundError):
            d.get_dataframe(name='33')

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
    def test_add_dataframe(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        df1 = DataFrame(
            name='new_dataframe',
            data=ratings,
            parent=d,
            description='some new dataframe'
        )
        assert isinstance(df1, DataFrame)

        d.add_dataframe(df1)
        assert d.dataframe_count == 2

        df2 = d.get_dataframe(name='new_dataframe')
        assert df1 == df2

        assert d.dataframe_names == [inspect.stack()[0][3], 'new_dataframe']

        df3 = DataFrame(
            name='new_dataframe',
            data=ratings,
            parent=d,
            description='some new dataframe'
        )
        d.add_dataframe(df3)
        assert df3.parent == d

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
    def test_remove_dataframe(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        d.remove_dataframe(name=inspect.stack()[0][3])

        assert d.dataframe_count == 0   # Zero DataFrames

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
    def test_equality(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        d2 = 5
        assert not d == d2

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
    def test_dataframe_data(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        df = DataFrame(
            name='new_dataframe',
            data=ratings,
            parent=d,
            description='some new dataframe'
        )
        assert df.is_composite is False
        assert len(df) == 0
        assert df.size > 0
        assert df.nrows > 100
        assert df.ncols > 3
        assert df.nulls == 0
        assert df.pct_nulls < 0.1
        logger.debug(df.info())
        assert isinstance(df.head(), pd.DataFrame)
        assert isinstance(df.tail(), pd.DataFrame)
        assert df.head().shape[0] == 5
        assert df.tail().shape[0] == 5

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
    def test_dto(self, ratings, tasks, caplog):
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
        d = Dataset(
            name=inspect.stack()[0][3],
            datasource='spotify',
            stage='interim',
            description='Dataset for ' + inspect.stack()[0][3],
            data=ratings,
            task_id=tasks[0].id,
        )
        d.id = 9

        df = d.get_dataframe()
        assert isinstance(df, DataFrame)

        dto = df.as_dto()
        assert dto.name == inspect.stack()[0][3]
        assert dto.stage == 'interim'
        assert dto.datasource == 'spotify'
        assert dto.dataset_id == 9
        assert dto.size > 0
        assert dto.nrows > 100
        assert dto.ncols > 2
        assert dto.nulls == 0
        assert dto.pct_nulls < 0.1
        assert isinstance(dto.created, datetime)

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
