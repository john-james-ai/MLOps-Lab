#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Dataname   : /tests/test_core/test_entity/test_dataset.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 7th 2022 10:37:56 am                                             #
# Modified   : Friday December 9th 2022 06:50:29 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pandas as pd
import pytest
import logging

from recsys.core.entity.dataset import Dataset
from recsys.core.dal.dto import DatasetDTO

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dse
class TestDatasetEntity:  # pragma: no cover
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
        )
        assert ds.name == inspect.stack()[0][3]
        assert ds.description == f"Description of {inspect.stack()[0][3]}"
        assert ds.datasource == "movielens25m"
        assert ds.task_id == 22
        assert ds.cost is None
        assert ds.nrows is None
        assert ds.ncols is None
        assert ds.null_counts is None
        assert ds.memory_size_mb is None
        assert isinstance(ds.created, datetime)

        ds.cost = 23902
        assert ds.cost == 23902

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
    def test_instantiation_with_data(self, ratings, caplog):
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            data=ratings,
            task_id=22,
            stage="staged",
        )
        assert ds.name == inspect.stack()[0][3]
        assert ds.description == f"Description of {inspect.stack()[0][3]}"
        assert ds.datasource == "movielens25m"
        assert ds.task_id == 22
        assert ds.cost is None
        assert ds.nrows is not None
        assert ds.ncols is not None
        assert ds.null_counts is not None
        assert ds.memory_size_mb is not None
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
            _ = Dataset()

        with pytest.raises(TypeError):  # No datasource
            _ = Dataset(name="johosephat")

        with pytest.raises(TypeError):
            _ = Dataset(name="johosephat", datasource="spotify")  # No task_id

        with pytest.raises(TypeError):
            _ = Dataset(name="johosephat", datasource="spotify", task_id=232)  # No stage

        with pytest.raises(ValueError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id=232, stage="rafas"
            )  # Invalid stage

        with pytest.raises(TypeError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id=232, stage="raw", version="55"
            )  # Invalid version

        with pytest.raises(TypeError):
            _ = Dataset(
                name="johosephat", datasource="spotify", task_id="232", stage="raw"
            )  # Invalid task_id

        with pytest.raises(ValueError):
            _ = Dataset(
                name="johosephat", datasource="dfas", task_id=232, version=2, stage="raw"
            )  # Invalid datasource

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
    def test_dto_instantiation(self, dataset_dtos, caplog):
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
        for i, dto in enumerate(dataset_dtos, start=1):
            ds = Dataset.from_dto(dto)
            assert ds.id == i
            assert ds.id == i
            assert ds.name == f"dataset_dto_{i}"
            assert ds.description == f"Description for Dataset DTO {i}"
            assert ds.datasource == "movielens25m"
            assert ds.workspace == "test"
            assert ds.stage == "staged"
            assert ds.version == i + 1
            assert ds.cost == 1000 * i
            assert ds.nrows == 100 * i
            assert ds.ncols == i
            assert ds.null_counts == i + i
            assert ds.memory_size_mb == 100 * i
            assert ds.filepath == "tests/file/" + f"dataset_dto_{i}" + ".pkl"
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
    def test_dto_instantiation_validation(self, dataset_dtos, caplog):
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
        for i, dto in enumerate(dataset_dtos, start=1):
            if i == 1:
                dto.name = None  # Name
                with pytest.raises(TypeError):
                    Dataset.from_dto(dto)
            if i == 2:
                dto.datasource = None  # no datasource
                with pytest.raises(TypeError):
                    Dataset.from_dto(dto)
                dto.datasource = "dssds"  # Invalid datasource
                with pytest.raises(ValueError):
                    Dataset.from_dto(dto)
            if i == 3:
                dto.stage = None  # no stage
                with pytest.raises(TypeError):
                    Dataset.from_dto(dto)
                dto.stage = "dsaa"  # invalid stage
                with pytest.raises(ValueError):
                    Dataset.from_dto(dto)
            if i == 4:
                dto.task_id = None  # no task_id
                with pytest.raises(TypeError):
                    Dataset.from_dto(dto)
                dto.stage = "222"  # invalid task_id
                with pytest.raises(ValueError):
                    Dataset.from_dto(dto)

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
        ds = Dataset(
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
        assert d["version"] == 1
        assert d["cost"] is None
        assert d["nrows"] is not None
        assert d["ncols"] is not None
        assert d["null_counts"] is not None
        assert d["memory_size_mb"] is not None
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
        ds = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            data=ratings,
            stage="staged",
        )
        dto = ds.as_dto()
        assert isinstance(dto, DatasetDTO)
        assert dto.id is None
        assert dto.name == inspect.stack()[0][3]
        assert dto.description == f"Description of {inspect.stack()[0][3]}"
        assert dto.datasource == "movielens25m"
        assert dto.workspace == "test"
        assert dto.stage == "staged"
        assert dto.version == 1
        assert dto.cost is None
        assert dto.nrows is not None
        assert dto.ncols is not None
        assert dto.null_counts is not None
        assert dto.memory_size_mb is not None
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
        ds = Dataset(
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
        ds = Dataset(
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
        ds = Dataset(
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
        ds = Dataset(
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

        ds.cost = 222
        assert ds.modified != modified
        modified = ds.modified

        ds.version = 3
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
        ds = Dataset(
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
            data=ratings,
        )
        ds2 = Dataset(
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
        ds1 = Dataset(
            name=inspect.stack()[0][3],
            description=f"Description of {inspect.stack()[0][3]}",
            datasource="movielens25m",
            task_id=22,
            stage="staged",
            data=ratings,
        )
        assert isinstance(ds1.data, pd.DataFrame)

        ds2 = Dataset(
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
