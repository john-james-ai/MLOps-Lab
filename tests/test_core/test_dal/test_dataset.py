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
# Created    : Monday November 14th 2022 05:45:09 pm                                               #
# Modified   : Saturday November 26th 2022 08:21:24 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pandas as pd
import pytest
import copy
from datetime import datetime
import logging
from logging import config

from recsys.config.log import test_log_config
from recsys.core.dal.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
NAME = "rating"
COST = 23022


@pytest.mark.dataset
class TestDataset:
    def test_validation(self, ratings, caplog):
        start = datetime.now()
        logger.info(
            "\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%H:%M:%S"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ------------------------------------------------------------------------------------------------ #
        with pytest.raises(ValueError):
            _ = Dataset(name=NAME, data=ratings, stage="328")

        with pytest.raises(ValueError):
            _ = Dataset(data=ratings, stage="input")

        # ------------------------------------------------------------------------------------------------ #
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
        DATASET = Dataset(name=NAME, data=ratings, cost=COST)

        assert DATASET.name == NAME
        assert DATASET.stage == "interim"
        assert DATASET.nrows > 1000
        assert DATASET.ncols > 2
        assert DATASET.nrows > 1000
        assert DATASET.null_counts == 0
        assert isinstance(DATASET.created, datetime)
        assert isinstance(DATASET.creator, str)
        assert DATASET.cost == COST
        assert isinstance(DATASET.__str__(), str)
        assert isinstance(DATASET.__repr__(), str)

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
    def test_reconstitution(self, ratings, caplog):
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
        DATASET = Dataset(name=NAME, data=ratings, cost=COST)
        metadata = DATASET.as_dict()
        new_dataset = copy.deepcopy(Dataset(**metadata))
        new_dataset.data = DATASET.data
        assert DATASET == new_dataset
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
    def test_repo_methods(self, ratings, caplog):
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
        pass
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
    def test_methods(self, ratings, caplog):
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
        DATASET = Dataset(name=NAME, data=ratings, cost=COST)
        logger.info(DATASET.info())
        assert isinstance(DATASET.head(), pd.DataFrame)
        assert isinstance(DATASET.tail(), pd.DataFrame)
        assert isinstance(DATASET.sample(n=10), pd.DataFrame)
        assert len(DATASET.columns) == 3
        assert isinstance(DATASET.cluster_sample(n=10, by="userId"), pd.DataFrame)
        assert DATASET.sample(n=10).shape[0] == 10

        with pytest.raises(TypeError):
            DATASET.data = ratings

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
    def test_equality(self, ratings, dataset, dataset_sans_data, caplog):
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
        # Equal
        ds2 = copy.deepcopy(dataset)
        assert ds2 == dataset
        # Not equal as due to lack of data
        assert dataset != dataset_sans_data
        # Not equal as data is different type
        dataset_sans_data.data = 5
        assert dataset != dataset_sans_data
        # Not equal as data same type but different values
        ratings2 = ratings.head()
        params = dataset.as_dict()
        assert isinstance(params, dict)
        ds2 = Dataset(**params)
        ds2.data = ratings2
        assert ds2 != dataset
        # Different metadata
        params["stage"] = "final"
        ds2 = Dataset(**params)
        ds2.data = ratings
        assert ds2 != dataset

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
    def test_sans_data(self, dataset_sans_data, caplog):
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
        assert dataset_sans_data.columns is None
        assert dataset_sans_data.nrows is None
        assert dataset_sans_data.ncols is None
        assert dataset_sans_data.memory_size_mb is None
        assert dataset_sans_data.null_counts is None

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
    def test_reset_data(self, dataset, dataset_sans_data, caplog):
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
        with pytest.raises(TypeError):
            dataset.data = 5

        dataset.filepath = "some/new/path"
        assert dataset.filepath == "some/new/path"
        dataset.version = 12
        assert dataset.version == 12

        # Reset cost data
        dataset_sans_data.cost = 55
        with pytest.raises(TypeError):
            dataset_sans_data.cost = 5

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
