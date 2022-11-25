#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_process.py                                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 19th 2022 08:09:34 am                                             #
# Modified   : Thursday November 24th 2022 04:46:41 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import inspect
import importlib
from dotenv import load_dotenv
from datetime import datetime
import pytest
import logging


from recsys.config.base import PIPELINES
from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core.workflow.operators import CreateDataset, TrainTestSplit, RatingsAdjuster, Phi

# ------------------------------------------------------------------------------------------------ #
CONTEXT = IOService()


class Pipeline:
    load_dotenv()
    env = os.getenv("ENV")
    module = PIPELINES["collabfilter"]["preprocess"].get(env)
    config = importlib.import_module(module)


# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m/%d/%Y %H:%M",
    force=True,
)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.cf
class TestCreateDataset:

    # ============================================================================================ #
    def test_setup(self, repo, caplog):
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
        repo.reset()
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
    def test_create_dataset(self, caplog):
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
        cds = CreateDataset(
            step_params=Pipeline.config.CreateDatasetParams().step_params,
            input_params=Pipeline.config.CreateDatasetParams().input_params,
            output_params=Pipeline.config.CreateDatasetParams().output_params,
        )
        dataset = cds.run(context=CONTEXT)
        assert isinstance(dataset, Dataset)
        assert dataset.name == "rating"
        assert dataset.env == "test"
        assert dataset.stage == "interim"
        assert dataset.version == 1
        filepath = os.path.join(
            "tests/data/movielens20m/repo", dataset.env, dataset.stage, "rating_test_interim_v1.pkl"
        )
        assert os.path.exists(filepath)
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


@pytest.mark.cf
class TestTrainTestSplit:
    # ================================================================================================ #
    def test_setup(self, caplog):
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
    def test_train_test_split_clustered(self, caplog):
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
        tts = TrainTestSplit(
            step_params=Pipeline.config.TrainTestParams().step_params,
            input_params=Pipeline.config.TrainTestParams().input_params,
            output_params=Pipeline.config.TrainTestParams().output_params,
        )
        train_test = tts.run(context=CONTEXT)

        assert isinstance(train_test, dict)
        assert train_test["train"].name == "train"
        assert train_test["train"].env == "test"
        assert train_test["train"].stage == "interim"
        assert train_test["train"].version == 1
        assert train_test["train"].data is not None

        assert train_test["test"].name == "test"
        assert train_test["test"].env == "test"
        assert train_test["test"].stage == "interim"
        assert train_test["test"].version == 1
        assert train_test["test"].data is not None

        train = os.path.join(
            "tests/data/movielens20m/repo", "test", "interim", "train_test_interim_v1.pkl"
        )
        test = os.path.join(
            "tests/data/movielens20m/repo", "test", "interim", "test_test_interim_v1.pkl"
        )
        assert os.path.exists(train)
        assert os.path.exists(test)
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


@pytest.mark.cf
class TestCentering:
    # ================================================================================================ #
    def test_setup(self, caplog):
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
    def test_centering(self, caplog):
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
        ra = RatingsAdjuster(
            step_params=Pipeline.config.RatingsAdjusterParams().step_params,
            input_params=Pipeline.config.RatingsAdjusterParams().input_params,
            output_params=Pipeline.config.RatingsAdjusterParams().output_params,
        )
        dataset = ra.run(context=CONTEXT)
        assert dataset.name == "adjusted_ratings"
        assert dataset.env == "test"
        assert dataset.stage == "interim"
        assert dataset.version == 1
        logger.debug(dataset.data.head())
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


@pytest.mark.cf
class TestUserAverageRatings:
    # ================================================================================================ #
    def test_setup(self, caplog):
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
    def test_user(self, caplog):
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
        ra = RatingsAdjuster(
            step_params=Pipeline.config.UserParams().step_params,
            input_params=Pipeline.config.UserParams().input_params,
            output_params=Pipeline.config.UserParams().output_params,
        )
        dataset = ra.run(context=CONTEXT)
        assert dataset.name == "user"
        assert dataset.env == "test"
        assert dataset.stage == "interim"
        assert dataset.version == 1
        logger.debug(dataset.data.head())
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


@pytest.mark.cf
class TestPhi:
    # ================================================================================================ #
    def test_setup(self, caplog):
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
    def test_phi(self, caplog):
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
        ra = Phi(
            step_params=Pipeline.config.PhiParams().step_params,
            input_params=Pipeline.config.PhiParams().input_params,
            output_params=Pipeline.config.PhiParams().output_params,
        )
        dataset = ra.run(context=CONTEXT)
        assert dataset.name == "user"
        assert dataset.env == "test"
        assert dataset.stage == "interim"
        assert dataset.version == 1
        logger.debug(dataset.data.head())
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
