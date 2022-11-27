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
# Modified   : Sunday November 27th 2022 04:08:29 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field

from recsys.config.base import Config
from recsys.core.services.container import container
from recsys.core.dal.dataset import Dataset
from recsys.core.dal.registry import DatasetRegistry
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


DATASET_PARAMS_I = [
    {"name": "ds1", "stage": "staged", "description": "Desc1", "cost": 1234},
    {"name": "ds2", "stage": "interim", "description": "Desc2", "cost": 2345},
    {"name": "ds3", "stage": "staged", "description": "Desc3", "cost": 3456},
    {"name": "ds4", "stage": "final", "description": "Desc4", "cost": 4567},
    {"name": "ds5", "stage": "interim", "description": "Desc5", "cost": None},
]

DATASET_PARAMS_II = [
    {"name": "ds6", "stage": "staged", "description": "Desc6", "cost": 1234},
    {"name": "ds7", "stage": "interim", "description": "Desc7", "cost": 2345},
    {"name": "ds8", "stage": "staged", "description": "Desc8", "cost": 3456},
    {"name": "ds9", "stage": "final", "description": "Desc9", "cost": 4567},
    {"name": "ds10", "stage": "interim", "description": "Desc10", "cost": None},
]
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
#                           DATASETS PRE REPO (w/o directory and filepath)                         #
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def dataset(ratings):
    dataset = Dataset(**DATASET_PARAMS_I[0], data=ratings)
    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="function")
def dataset_sans_data(ratings):
    dataset = Dataset(**DATASET_PARAMS_I[4])
    return dataset


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="function")
def datasets(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for params in DATASET_PARAMS_I[0:4]:
        ds = Dataset(**params, data=ratings)
        datasets.append(ds)
    return datasets


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="function")
def datasets_ii(ratings):
    # Phi dataset was removed b.c. it was accidentally deleted.
    datasets = []
    for params in DATASET_PARAMS_II[0:4]:
        ds = Dataset(**params, data=ratings)
        datasets.append(ds)
    return datasets


# ------------------------------------------------------------------------------------------------ #
#                                DATABASE, REGISTRY, AND REPO                                      #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def database():
    return container.db()


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def registry(database):
    return DatasetRegistry(database=database)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def repo():
    repo = container.repo()
    return repo


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
