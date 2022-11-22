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
# Modified   : Tuesday November 22nd 2022 04:30:06 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest

from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core.services.repo import DatasetRepo
from recsys.core import REPO_FILE_FORMAT

# ------------------------------------------------------------------------------------------------ #
RATINGS_FILEPATH = "tests/data/ratings.pkl"
# ------------------------------------------------------------------------------------------------ #
dp = [
    {
        "name": "ds1",
        "stage": "raw",
        "env": "dev",
        "description": "Desc1",
    },
    {
        "name": "ds2",
        "stage": "interim",
        "env": "test",
        "description": "Desc2",
    },
    {
        "name": "ds3",
        "stage": "raw",
        "env": "prod",
        "description": "Desc3",
    },
    {
        "name": "ds4",
        "stage": "cooked",
        "env": "dev",
        "description": "Desc4",
    },
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
    dataset = Dataset(
        name="ratings",
        stage="interim",
        env="dev",
        data=ratings,
        description="Desc5",
    )

    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def datasets(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for params in dp:
        ds = Dataset(**params)
        ds.data = ratings
        datasets.append(ds)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def repo():
    return DatasetRepo(io=IOService, file_format=REPO_FILE_FORMAT, version_control=True)
