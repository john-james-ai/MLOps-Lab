#!/usr/bin/workspace python3
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
# Created    : Saturday December 3rd 2022 09:37:10 am                                              #
# Modified   : Friday December 9th 2022 06:50:29 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
import sqlite3
from datetime import datetime
import pandas as pd

import recsys
from recsys.containers import Recsys
from recsys.core.services.io import IOService
from recsys.core.dal.dao import DatasetDTO, FilesetDTO
from recsys.core.data.database import SQLiteConnection, SQLiteDatabase
from recsys.core.entity.datasource import DataSource

# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
RATINGS_FILEPATH = "tests/data/movielens25m/ratings.pkl"
DATA_SOURCE_FILEPATH = "data/sources.csv"
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def location():
    return TEST_LOCATION


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def ratings():
    return IOService.read(RATINGS_FILEPATH)


# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def connection():
    return SQLiteConnection(connector=sqlite3.connect, location=TEST_LOCATION)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def database(connection):
    return SQLiteDatabase(connection=connection)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataset_dtos():
    dtos = []
    for i in range(1, 6):
        dto = DatasetDTO(
            id=i,
            name=f"dataset_dto_{i}",
            description=f"Description for Dataset DTO {i}",
            datasource="movielens25m",
            workspace="test",
            stage="staged",
            version=i + 1,
            cost=1000 * i,
            nrows=100 * i,
            ncols=i,
            null_counts=i + i,
            memory_size_mb=100 * i,
            filepath="tests/file/" + f"dataset_dto_{i}" + ".pkl",
            task_id=i + i,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def fileset_dtos():
    dtos = []
    for i in range(1, 6):
        dto = FilesetDTO(
            id=None,
            name=f"fileset_dto_{i}",
            description=f"Fileset Description DTO {i}",
            datasource="movielens25m",
            filesize=501,
            filepath="tests/file/" + f"dataset_dto_{i}" + ".pkl",
            task_id=i + i,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataset_dicts():
    """List of dictionaries that can be used to instantiate Dataset."""
    lod = []
    for i in range(1, 6):
        d = {
            "name": f"dataset_{i}",
            "description": f"Description for Dataset {i}",
            "datasource": "movielens25m",
            "workspace": "test",
            "stage": "staged",
            "task_id": i + i,
        }
        lod.append(d)

    return lod


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def fileset_dicts():
    """List of dictionaries that can be used to instantiate Fileset."""
    lod = []
    for i in range(1, 6):
        d = {
            "name": f"fileset_{i}",
            "description": f"Description for fileset_{i}",
            "datasource": "movielens25m",
            "filepath": "tests/file/" + f"fileset_dto_{i}" + ".pkl",
            "task_id": i + i,
        }
        lod.append(d)

    return lod


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasources():
    """Provide a list of DataSource objects."""
    datasources = []
    df = pd.read_csv(DATA_SOURCE_FILEPATH)
    for idx in df.index:
        ds = DataSource(name=df['name'][idx], publisher=df['publisher'][idx], description=df['description'][idx], website=df['website'][idx], url=df['url'][idx])
        datasources.append(ds)

    return datasources


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope='module', autouse=True)
def container():
    container = Recsys()
    container.init_resources()
    container.wire(modules=[recsys.__name__])
    return container
