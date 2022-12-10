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
# Modified   : Friday December 9th 2022 09:22:34 pm                                                #
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
from recsys.core.dal.dao import DatasetDTO, FilesetDTO, DataSourceDTO, ProfileDTO, TaskDTO, TaskResourceDTO, JobDTO
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
def datasource_dtos():
    dtos = []
    for i in range(1, 6):
        dto = DataSourceDTO(
            id=None,
            name=f"datasource_dto_{i}",
            publisher=f"Publisher {i}",
            description=f"Datasource Description DTO {i}",
            website="www.somewebsite.com",
            url="www.someurl.com",
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def profile_dtos():
    dtos = []
    for i in range(1, 6):
        dto = ProfileDTO(
            id=None,
            name=f"profile_{i}",
            description=f"Description for Profile {i}",
            start=datetime.now(),
            end=datetime.now(),
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
            id=None,
            name=f"task_dto_{i}",
            description=f"Task Description DTO {i}",
            workspace="test",
            operator="some_operator",
            module="some.module",
            job_id=i * 10,
            profile_id=i + 10,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def task_resource_dtos():
    dtos = []
    for i in range(1, 6):
        dto = TaskResourceDTO(
            id=None,
            name=f"task_resource_{i}",
            description=f"Description for Task Resource # {i}",
            task_id=i + 10,
            resource_kind="Dataset",
            resource_id=i * 10,
            resource_context="input",
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
            id=None,
            name=f"job_{i}",
            description=f"Description for Job # {i}",
            pipeline=f"pipeline_{i}",
            workspace="test",
            profile_id=i + 10,
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
