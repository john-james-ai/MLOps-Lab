#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Dataname   : /tests/test_core/test_entity/test_datasource.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 10:37:56 am                                             #
# Modified   : Saturday December 10th 2022 10:14:07 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pandas as pd
import pytest
import logging

from recsys.core.entity.datasource import DataSource
from recsys.core.dal.dto import DataSourceDTO

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dse
class TestDataSourceEntity:  # pragma: no cover
    # ============================================================================================ #
    def test_instantiation_no_data(self, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            publisher="GroupLens",
            website="https://grouplens.org/datasets/movielens/"
        )
        assert ds.name == inspect.stack()[0][3]
        assert ds.description == f"Description of {inspect.stack()[0][3]}"
        assert ds.publisher == "GroupLens"
        assert ds.website == "https://grouplens.org/datasets/movielens/"
        assert isinstance(ds.created, datetime)

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
    def test_instantiation_validation(self, caplog):
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
        with pytest.raises(TypeError):  # No name
            _ = DataSource()

        with pytest.raises(TypeError):  # No publisher
            _ = DataSource(name="johosephat")

        with pytest.raises(TypeError):
            _ = DataSource(name="johosephat", publisher="spotify")  # website

        with pytest.raises(ValueError):
            _ = DataSource(name="johosephat", publisher="spotify", website="https://grouplens.org/datasets/movielens/")  # Invalid publisher

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
    def test_dto_instantiation(self, datasource_dtos, caplog):
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
        for i, dto in enumerate(datasource_dtos, start=1):
            ds = DataSource.from_dto(dto)

            assert ds.id == i
            assert ds.name == f"datasource_dto_{i}"
            assert ds.description == f"Description for DataSource DTO {i}"
            assert ds.datasource == "movielens25m"
            assert ds.workspace == "test"
            assert ds.stage == "staged"
            assert ds.filepath == "tests/file/" + f"datasource_dto_{i}" + ".pkl"
            assert ds.task_id == i + i
            assert isinstance(ds.created, datetime)
            assert isinstance(ds.modified, datetime)

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
    def test_dto_instantiation_validation(self, datasource_dtos, caplog):
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
        for i, dto in enumerate(datasource_dtos, start=1):
            if i == 1:
                dto.name = None  # Name
                with pytest.raises(TypeError):
                    DataSource.from_dto(dto)
            if i == 2:
                dto.datasource = None  # no datasource
                with pytest.raises(TypeError):
                    DataSource.from_dto(dto)
                dto.datasource = "dssds"  # Invalid datasource
                with pytest.raises(ValueError):
                    DataSource.from_dto(dto)
            if i == 3:
                dto.stage = None  # no stage
                with pytest.raises(TypeError):
                    DataSource.from_dto(dto)
                dto.stage = "dsaa"  # invalid stage
                with pytest.raises(ValueError):
                    DataSource.from_dto(dto)
            if i == 4:
                dto.task_id = None  # no task_id
                with pytest.raises(TypeError):
                    DataSource.from_dto(dto)
                dto.stage = "222"  # invalid task_id
                with pytest.raises(ValueError):
                    DataSource.from_dto(dto)

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
    def test_as_dict(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            data=ratings,
            stage="staged",
        )
        d = ds.as_dict()
        assert isinstance(d, dict)
        assert d["id"] is None
        assert d["name"] == inspect.stack()[0][3]
        assert d["description"] == f"Description of {inspect.stack()[0][3]}"
        assert d["datasource"] == "movielens25m"
        assert d["workspace"] == "test"
        assert d["stage"] == "staged"
        assert d["filepath"] is None
        assert d["task_id"] == 22
        assert isinstance(d["created"], datetime)
        assert d["modified"] is None

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
    def test_as_dto(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            data=ratings,
            stage="staged",
        )
        dto = ds.as_dto()
        assert isinstance(dto, DataSourceDTO)
        assert dto.id is None
        assert dto.name == inspect.stack()[0][3]
        assert dto.description == f"Description of {inspect.stack()[0][3]}"
        assert dto.datasource == "movielens25m"
        assert dto.workspace == "test"
        assert dto.stage == "staged"
        assert dto.filepath is None
        assert dto.task_id == 22
        assert isinstance(dto.created, datetime)
        assert dto.modified is None

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
    def test_access_methods(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            data=ratings,
            stage="staged",
        )
        logger.info(ds.info())
        assert isinstance(ds.head(), pd.DataFrame)
        assert isinstance(ds.tail(), pd.DataFrame)
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
    def test_access_methods_no_data(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )

        assert ds.info() is None
        assert ds.head() is None
        assert ds.tail() is None
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
    def test_set_once(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        ds.id = 5
        with pytest.raises(TypeError):
            ds.id = 6

        ds.data = ratings
        with pytest.raises(TypeError):
            ds.data = ratings

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
    def test_modified(self, ratings, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        modified = ds.modified
        ds.id = 9
        assert ds.modified != modified
        modified = ds.modified

        ds.data = ratings
        assert ds.modified != modified
        modified = ds.modified

        ds.filepath = "t/ests"
        assert ds.modified != modified
        modified = ds.modified

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
    def test_magic(self, caplog):
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
        ds = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        assert isinstance(ds.__str__(), str)
        assert isinstance(ds.__repr__(), str)

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

    def test_equality(self, ratings, caplog):
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
        ds1 = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
            data=ratings,
        )
        ds2 = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        assert not ds1 == ds2

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
    def test_data(self, ratings, caplog):
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
        ds1 = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
            data=ratings,
        )
        assert isinstance(ds1.data, pd.DataFrame)

        ds2 = DataSource(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        with pytest.raises(TypeError):
            ds2.data = {'some': 'dictionary'}
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
