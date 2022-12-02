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
# Modified   : Friday December 2nd 2022 03:02:37 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field

from recsys.config import Config

# from recsys.containers import container
from recsys.core.entity.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core.workflow.pipeline import Context


# ------------------------------------------------------------------------------------------------ #
RATINGS_FILEPATH = "data/working/dev/input/rating.pkl"
ETL_CONFIG_FILEPATH = "recsys/config/etl.yml"
CF_CONFIG_FILEPATH = "recsys/config/cf.yml"
# ------------------------------------------------------------------------------------------------ #


@dataclass
class CONFIG_TESTER(Config):
    name: str = "joe"
    age: int = 42
    kid_ages: list[int] = field(default_factory=list)
    dob: datetime = datetime.now()
    wife: defaultdict[dict] = field(default_factory=lambda: defaultdict(dict))

    def __post_init__(self) -> None:
        self.kid_ages = [2, 3, 5]
        self.wife = {"name": "Ann", "age": 39}


# ------------------------------------------------------------------------------------------------ #
#                                     CONFIG FIXTURE                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def cfg():
    return CONFIG_TESTER()


# ------------------------------------------------------------------------------------------------ #
#                                     RATINGS DATA                                                 #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def ratings():
    return IOService().read(filepath=RATINGS_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
#                                    PIPELINE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def etl_config():
    return IOService().read(filepath=ETL_CONFIG_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
#                                       DATASETS                                                   #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="function")
def datasets(ratings):
    stages = ["raw", "staged", "interim", "final", "ext"]
    datasets = []
    for i in range(5):
        j = i % 5
        datasets.append(
            Dataset(source="movielens25m"),
            name=f"dataset_{i}",
            description=f"Description {i}",
            data=ratings,
            stage=stages[j],
            version=i,
            task_id=i,
            step_id=i + 5,
        )
    return datasets


# ------------------------------------------------------------------------------------------------ #
#                                DATABASE, REGISTRY, AND REPO                                      #
# ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def database():
#     return container.db()


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def registry(database):
    return Dataset(database=database)


# # ------------------------------------------------------------------------------------------------ #
# @pytest.fixture(scope="module")
# def repo():
#     repo = container.repo()
#     return repo


# ------------------------------------------------------------------------------------------------ #
#                                    CONFIG FIXTURES                                               #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def cf_config():
    return IOService().read(filepath=CF_CONFIG_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def context(cf_config):
    return Context(name=cf_config["name"], description=cf_config["description"], io=IOService)
