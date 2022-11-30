#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_decorator.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 29th 2022 08:25:32 pm                                              #
# Modified   : Tuesday November 29th 2022 08:43:17 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
from logging import config

from recsys.config.workflow import StepPO, DatasetInputPO, DatasetOutputPO, FilesetInputPO
from recsys.core.dal.dataset import Dataset
from recsys.core.workflow.operators import DatasetOperator
from recsys.core.workflow.pipeline import Context
from recsys.config.log import test_log_config

# ------------------------------------------------------------------------------------------------ #
config.dictConfig(test_log_config)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
FILEPATH = "data/working/test/input/rating.pkl"
# ------------------------------------------------------------------------------------------------ #


class NullOperator(DatasetOperator):
    """Computes and stores average rating for each user.

    Args:
        step_params (StepPO): Name and description of the step
        input_params (DatasetInputPO): The test user Dataset params
        output_params (DatasetOutputPO) Test user average ratings

    Returns: Output Dataset Object

    Returns Dataset
    """

    def __init__(
        self,
        step_params: StepPO = StepPO(force=False),
        input_params: DatasetInputPO = FilesetInputPO(filepath=FILEPATH),
        output_params: DatasetOutputPO = DatasetOutputPO(
            name="null", description="nulloperator", stage="input"
        ),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        return 5


@pytest.mark.repo
class TestRepository:
    # ============================================================================================ #
    def test_invalid_return_type(self, caplog):
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
        nullops = NullOperator()
        with pytest.raises(TypeError):
            nullops.run()
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
