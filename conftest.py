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
# Modified   : Friday November 18th 2022 08:37:17 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import pytest
from recsys.core.services.io import IOService
from recsys.core.dal.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
RATINGS_FILEPATH = "tests/data/ratings.pkl"
# ------------------------------------------------------------------------------------------------ #
dataset_names = ["users", "weights", "phi"]


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
    data = IOService().read(filepath=RATINGS_FILEPATH)
    name = os.path.splitext(os.path.basename(RATINGS_FILEPATH))[0]
    dataset = Dataset(name=name, data=data, env="test")
    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def datasets(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for name in dataset_names:
        datasets.append(Dataset(name=name, data=ratings, env="test"))
    return datasets
