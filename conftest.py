#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /conftest.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 11th 2022 06:38:26 am                                               #
# Modified   : Wednesday November 23rd 2022 07:48:31 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest

from recsys.core.services.repo import Container
from recsys.core.dal.dataset import Dataset
from recsys.core.dal.database import Database
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
RATINGS_FILEPATH = "tests/data/ratings.pkl"
# ------------------------------------------------------------------------------------------------ #
dp = [
    {"name": "ds1", "stage": "raw", "env": "dev", "description": "Desc1", "cost": 1234},
    {"name": "ds2", "stage": "interim", "env": "test", "description": "Desc2", "cost": 2345},
    {"name": "ds3", "stage": "raw", "env": "prod", "description": "Desc3", "cost": 3456},
    {"name": "ds4", "stage": "cooked", "env": "dev", "description": "Desc4", "cost": 4567},
]
# ------------------------------------------------------------------------------------------------ #
#                                        DATAFRAME                                                 #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def ratings():
    return IOService().read(filepath=RATINGS_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
#                           DATASETS PRE REPO (w/o directory and filepath)                         #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def dataset(ratings):
    dataset = Dataset(**dp[0], data=ratings)

    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="function")
def datasets(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for params in dp:
        ds = Dataset(**params, data=ratings)
        datasets.append(ds)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def repo():
    container = Container()
    return container.repo()


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def database():
    return Database(database="data")
