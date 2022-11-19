#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test_cluster_sample.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 12th 2022 08:14:50 pm                                             #
# Modified   : Monday November 14th 2022 11:30:28 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
import pytest
import logging
import logging.config
import pandas as pd

from recsys.core.utils.data import clustered_sample

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.sample
class TestSample:
    def test_errors(self, data, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        with pytest.raises(KeyError):
            clustered_sample(data, by="x")
        with pytest.raises(ValueError):
            clustered_sample(data, by="userId", frac=0.8, n=2)
        with pytest.raises(ValueError):
            clustered_sample(data, by="userId", frac=2)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def test_sample(self, data, caplog):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sample = clustered_sample(data, by="userId", frac=0.01)

        assert isinstance(sample, pd.DataFrame)
        assert data.shape[0] > sample.shape[0]
        logger.info(sample.head())
        logger.info(
            "There are {} unique clusters in the sample.".format(sample["userId"].nunique())
        )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
