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
# Created    : Saturday December 3rd 2022 09:37:10 am                                              #
# Modified   : Sunday December 4th 2022 06:18:49 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
import sqlite3
from datetime import datetime

from recsys.core.dal.dao import DatasetDTO
from recsys.core.data.database import SQLiteConnection, SQLiteDatabase

# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def location():
    return TEST_LOCATION


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
            id=None,
            name=f"dataset_dto_{i}",
            description=f"Description for Dataset DTO {i}",
            source="movielens25m",
            env="test",
            stage="staged",
            version=1,
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
