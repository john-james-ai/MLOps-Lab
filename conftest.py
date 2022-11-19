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
# Modified   : Saturday November 19th 2022 04:22:26 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
from recsys.core.services.io import IOService
from recsys.core.dal.dataset import DatasetParams, get_id

# ------------------------------------------------------------------------------------------------ #
RATINGS_FILEPATH = "tests/data/ratings.pkl"
# ------------------------------------------------------------------------------------------------ #
dp = [
    {
        "name": "ds1",
        "stage": "raw",
        "env": "dev",
        "version": 1,
        "id": get_id(name="ds1", stage="raw", version=1, env="dev"),
        "description": "Desc1",
    },
    {
        "name": "ds1",
        "stage": "raw",
        "env": "dev",
        "version": 1,
        "id": get_id(name="ds1", stage="raw", version=1, env="dev"),
        "description": "Desc1",
    },
    {
        "name": "ds2",
        "stage": "interim",
        "env": "test",
        "version": 2,
        "id": get_id(name="ds2", stage="interim", version=2, env="test"),
        "description": "Desc2",
    },
    {
        "name": "ds3",
        "stage": "raw",
        "env": "prod",
        "version": 3,
        "id": get_id(name="ds3", stage="raw", version=3, env="prod"),
        "description": "Desc3",
    },
    {
        "name": "ds4",
        "stage": "cooked",
        "env": "dev",
        "version": 4,
        "id": get_id(name="ds4", stage="cooked", version=4, env="dev"),
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
def ratings_dataset():
    dp = DatasetParams(
        name="ratings",
        stage="interim",
        env="dev",
        version=5,
        id=get_id("ds1", "interim", 5, "dev"),
        description="Desc5",
    )

    dataset = dp.to_dataset()
    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def datasets(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for dataset in dp:
        ds = DatasetParams(**dataset)
        ds = ds.to_dataset()
        datasets.append(ds)
    return datasets
