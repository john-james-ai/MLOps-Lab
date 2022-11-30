#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_dataprep.py                                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 27th 2022 09:15:09 am                                               #
# Modified   : Wednesday November 30th 2022 12:28:34 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pandas as pd
from datetime import datetime
import pytest
import logging

from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService

# from recsys.recommenders.collabfilter.workflow.pipeline import CFDataPipelineBuilder, CFDataPipeline
from recsys.recommenders.collabfilter.workflow.operators import (
    CreateDataset,
    TrainTestSplit,
    TrainDataCentralizer,
    TestDataCentralizer,
    TrainUser,
    TestUser,
)
from recsys.recommenders.collabfilter.workflow.pipeline import CFDataPipelineBuilder
from recsys.core.workflow.pipeline import PipelineDirector

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def check_dataset(dataset: Dataset) -> None:
    assert isinstance(dataset.data, pd.DataFrame)
    assert dataset.cost > 0
    assert dataset.nrows > 100
    assert dataset.ncols >= 2
    # assert dataset.memory_size_mb > 1
    assert dataset.version == 1
    assert dataset.archived == 0
    assert isinstance(dataset.created, datetime)
    logger.debug(f"\n\nDataset {dataset.id} created by {dataset.creator}: \n{print(dataset)}")


@pytest.mark.cfdp
class TestCFDataPrep:
    # ================================================================================================ #
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
        repo.reset(silent=True)
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
    def test_create_dataset(self, context, repo, caplog):
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
        step = CreateDataset()
        step.run(context=context)
        assert step.output_params.name == "rating"
        dataset = repo.get_dataset(name="rating", stage="staged")
        check_dataset(dataset)

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
    def test_train_test_split(self, context, repo, caplog):
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
        step = TrainTestSplit()
        step.run(context=context)
        train = repo.get_dataset(name="train", stage="split")
        test = repo.get_dataset(name="test", stage="split")
        check_dataset(train)
        check_dataset(test)
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
    def test_centralizer_train(self, context, repo, caplog):
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
        step = TrainDataCentralizer()
        step.run(context=context)
        train_centered = repo.get_dataset(name="train_ratings_centered", stage="interim")
        check_dataset(train_centered)
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
    def test_centralizer_test(self, context, repo, caplog):
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
        step = TestDataCentralizer()
        step.run(context=context)
        test_centered = repo.get_dataset(name="test_ratings_centered", stage="interim")
        check_dataset(test_centered)
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
    def test_train_user_ave_rating(self, context, repo, caplog):
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
        step = TrainUser()
        step.run(context=context)
        train_user_ave_ratings = repo.get_dataset(name="train_user_ave_ratings", stage="interim")
        check_dataset(train_user_ave_ratings)
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
    def test_test_user_ave_rating(self, context, repo, caplog):
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
        step = TestUser()
        step.run(context=context)
        test_user_ave_ratings = repo.get_dataset(name="test_user_ave_ratings", stage="interim")
        check_dataset(test_user_ave_ratings)
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


@pytest.mark.cfpipe
class TestCFPipeline:
    # ================================================================================================ #
    def test_pipeline(self, cf_config, repo, caplog):
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
        repo.reset(silent=True)
        builder = CFDataPipelineBuilder()
        director = PipelineDirector(config=cf_config, builder=builder, io=IOService)
        director.build_pipeline()
        pipeline = builder.pipeline
        pipeline.run()
        registry = repo.print_registry()
        assert registry.shape[0] == 7
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
