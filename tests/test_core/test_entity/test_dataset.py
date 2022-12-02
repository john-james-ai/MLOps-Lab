#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_dataset.py                                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 11:54:55 pm                                              #
# Modified   : Friday December 2nd 2022 02:19:23 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import inspect
import pandas as pd
from datetime import datetime
import pytest
import logging
from logging import config

from recsys.core.entity.dataset import Dataset
from recsys.core.dal.dataset import DatasetDTO

from recsys.config.log import test_log_config

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.dataset
class TestDataset:
    # ============================================================================================ #
    def test_instantiation(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        # No Name
        with pytest.raises(ValueError):
            ds = Dataset(
                source="movielens25m",
                description="Some description",
                data=5,
                stage="interim",
                version=1,
                task_id=15,
                step_id=29,
            )
            ds.validate()
        # wrong source
        with pytest.raises(ValueError):
            ds = Dataset(
                source="movieledsds",
                name="joe",
                description="Some description",
                data=5,
                stage="interim",
                version=1,
                task_id=15,
                step_id=29,
            )
            ds.validate()
        # wrong stage
        with pytest.raises(ValueError):
            ds = Dataset(
                source="movielens25m",
                name="joe",
                description="Some description",
                data=5,
                stage="intexm",
                version=1,
                task_id=15,
                step_id=29,
            )
            ds.validate()
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_properties(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        # Test io
        ds = Dataset(
            source="movielens25m",
            name="joe",
            description="Some description",
            data=ratings,
            stage="interim",
            version=1,
            task_id=15,
            step_id=29,
        )

        assert ds.id is None
        ds.id = 5
        assert ds.id == 5
        assert ds.source == "movielens25m"
        assert ds.env == "test"
        assert ds.name == "joe"
        assert ds.description == "Some description"
        assert ds.stage == "interim"
        assert ds.task_id == 15
        assert ds.step_id == 29
        assert isinstance(ds.created, datetime)
        assert ds.version == 1
        ds.version == 10
        assert ds.version == 10
        assert ds.cost is None
        ds.cost = 940
        assert ds.cost == 940
        assert ds.nrows > 1000
        assert ds.cols > 2
        assert ds.null_counts == 0
        assert ds.memory_size_mb > 0
        assert ds.filepath is None
        ds.filepath == "data/filepath"
        assert ds.filepath == "data/filepath"
        assert ds.is_archived is False
        ds.is_archived = True
        assert ds.is_archived is True
        assert ds.archived is None
        ds.archived = datetime.now()
        assert isinstance(ds.archived, datetime)
        assert isinstance(ds.data, pd.DataFrame)
        ds.data = ratings
        assert isinstance(ds.data, pd.DataFrame)
        ds.archive()
        assert ds.is_archived is True
        ds.restore()
        assert ds.is_archived is False

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_export(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        # Test Export
        ds = Dataset(
            source="movielens25m",
            name="joe",
            description="Some description",
            data=ratings,
            stage="interim",
            version=1,
            task_id=15,
            step_id=29,
        )
        d = ds.as_dict()
        assert isinstance(d, dict)
        assert ds["id"] == 5
        assert ds["source"] == "movielens25m"
        assert ds["env"] == "test"
        assert ds["name"] == "joe"
        assert ds["description"] == "Some description"
        assert ds["stage"] == "interim"
        assert ds["task_id"] == 15
        assert ds["step_id"] == 29
        assert isinstance(ds["created,"], datetime)
        assert ds["version"] == 1
        assert ds["cost"] is None
        assert ds["cost"] == 940
        assert ds["cost"] == 940
        assert ds["nrows"] == 1000
        assert ds["cols"] == 2
        assert ds["null_counts"] == 0
        assert ds["memory_size_mb"] == 0
        assert ds["filepath"] is None
        assert ds["is_archived"] is False
        assert ds["archived"] is None
        assert isinstance(ds["archived"], datetime)
        assert ds["data"] == ratings

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_access_methods(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        ds = Dataset(
            source="movielens25m",
            name="joe",
            description="Some description",
            data=ratings,
            stage="interim",
            version=1,
            task_id=15,
            step_id=29,
        )

        logger.debug(f"\n\nInfo is:\n {ds.info()}")
        logger.debug(f"\n\nInfo is:\n {ds.head()}")
        logger.debug(f"\n\nInfo is:\n {ds.tail()}")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )
        # ============================================================================================ #

    def test_as_dto(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        ds = Dataset(
            source="movielens25m",
            name="joe",
            description="Some description",
            data=ratings,
            stage="interim",
            version=1,
            task_id=15,
            step_id=29,
        )

        dto = ds.as_dto()
        assert isinstance(dto, DatasetDTO)
        assert dto.id == 5
        assert dto.source == "movielens25m"
        assert dto.env == "test"
        assert dto.name == "joe"
        assert dto.description == "Some description"
        assert dto.stage == "interim"
        assert dto.task_id == 15
        assert dto.step_id == 29
        assert isinstance(dto.created, datetime)
        assert dto.version == 1
        assert dto.cost is None
        assert dto.nrows == 1000
        assert dto.cols == 2
        assert dto.null_counts == 0
        assert dto.memory_size_mb == 0
        assert dto.filepath is None
        assert dto.is_archived is False
        assert dto.archived is None
        assert isinstance(dto.archived, datetime)
        assert dto.data == ratings

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_from_dto(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        dto = DatasetDTO(
            id=5,
            source="movielens25m",
            env="test",
            name="joe",
            description="Somedescription",
            stage="interim",
            task_id=15,
            step_id=29,
            created=datetime.now(),
            version=1,
            cost=232,
            nrows=1000,
            cols=2,
            null_counts=0,
            memory_size_mb=0,
            filepath="data/filepath",
            is_archived=False,
            archived=datetime,
        )
        assert dto.id == 5
        assert dto.source == "movielens25m"
        assert dto.env == "test"
        assert dto.name == "joe"
        assert dto.description == "Somedescription"
        assert dto.stage == "interim"
        assert dto.task_id == 15
        assert dto.step_id == 29
        assert dto.version == 1
        assert dto.cost == 232
        assert dto.nrows == 1000
        assert dto.cols == 2
        assert dto.null_counts == 0
        assert dto.memory_size_mb == 0
        assert dto.filepath == "data/filepath"
        assert dto.is_archived is False

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%H:%M:%S"),
                end.strftime("%m/%d/%Y"),
            )
        )
