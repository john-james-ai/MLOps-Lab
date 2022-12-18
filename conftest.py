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
# Modified   : Sunday December 18th 2022 01:00:06 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
import numpy as np
import sqlite3
from datetime import datetime, timedelta

import tests.containers
from tests.containers import Recsys
from recsys.core.services.io import IOService
from recsys.core.entity.dataset import Dataset
from recsys.core.dal.dto import DatasetDTO, ProfileDTO, TaskDTO, JobDTO, OperationDTO
from recsys.core.data.database import Database
from recsys.core.data.connection import SQLiteConnection

# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
RATINGS_FILEPATH = "tests/data/movielens25m/raw/ratings.pkl"
DATA_SOURCE_FILEPATH = "tests/data/datasources.xlsx"


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
    return Database(connection=connection)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataset_dtos(ratings):
    size = ratings.memory_usage(deep=True).sum().astype(np.int64)
    nrows = ratings.shape[0]
    ncols = ratings.shape[1]
    nulls = ratings.isnull().sum().sum()
    pct_nulls = (ratings.isnull().sum().sum() / (ratings.shape[0] * ratings.shape[1])) * 100
    dtos = []
    for i in range(1, 6):
        dto = DatasetDTO(
            id=i,
            name=f"dataset_dto_{i}",
            description=f"Description for Dataset DTO {i}",
            datasource="movielens25m",
            workspace="test",
            stage="interim",
            filename=f"test_file_{i}.pkl",
            uri=f"tests/data/dataset_dto_{i}.pkl",
            size=size,
            nrows=nrows,
            ncols=ncols,
            nulls=nulls,
            pct_nulls=pct_nulls,
            task_id=i + i,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasets(dataset_dtos, ratings):
    datasets = []
    for i, dto in enumerate(dataset_dtos):
        dataset = Dataset.from_dto(dto)
        if i % 1 == 0:
            dataset.data = ratings
        datasets.append(dataset)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def profile_dtos():
    dtos = []
    for i in range(1, 6):
        dto = ProfileDTO(
            id=None,
            name=f"profile_dto_{i}",
            description=f"Description for Profile {i}",
            started=datetime.now(),
            ended=datetime.now(),
            duration=i + 1000,
            user_cpu_time=i + 2000,
            percent_cpu_used=i + 3000,
            total_physical_memory=i + 4000,
            physical_memory_available=i + 5000,
            physical_memory_used=i + 6000,
            percent_physical_memory_used=i + 7000,
            active_memory_used=i + 8000,
            disk_usage=i + 9000,
            percent_disk_usage=i + 10000,
            read_count=i + 11000,
            write_count=i + 12000,
            read_bytes=i + 13000,
            write_bytes=i + 14000,
            read_time=i + 15000,
            write_time=i + 16000,
            bytes_sent=i + 17000,
            bytes_recv=i + 18000,
            task_id=i * 20,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def task_dtos():
    dtos = []
    for i in range(1, 6):
        dto = TaskDTO(
            id=i,
            name=f"task_dto_{i}",
            description=f"Task Description DTO {i}",
            workspace="test",
            operator="some_operator",
            started=datetime.now() - timedelta(hours=i),
            ended=datetime.now(),
            duration=(datetime.now() - (datetime.now() - timedelta(hours=i))).total_seconds(),
            job_id=i * 10,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def job_dtos():
    dtos = []
    for i in range(1, 6):
        dto = JobDTO(
            id=i,
            name=f"job_dto_{i}",
            description=f"Description for Job # {i}",
            workspace="test",
            n_tasks=i * 10,
            n_tasks_completed=i * 8,
            pct_tasks_completed=(i * 8) / (i * 10),
            started=datetime.now() - timedelta(hours=i),
            ended=datetime.now(),
            duration=(datetime.now() - (datetime.now() - timedelta(hours=i))).total_seconds(),
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def operation_dtos():
    dtos = []
    for i in range(1, 6):
        dto = OperationDTO(
            id=i,
            name=f"operation_dto_{i}",
            description=f"Description for Operation # {i}",
            workspace="test",
            stage="interim",
            uri=f"tests/operations/operation_dto_{i}.pkl",
            task_id=i + 22,
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
@pytest.fixture(scope='module', autouse=True)
def container():
    container = Recsys()
    container.init_resources()
    container.wire(modules=[tests.containers])
    return container
