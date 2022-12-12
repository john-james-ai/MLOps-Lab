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
# Modified   : Monday December 12th 2022 12:32:01 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
import numpy as np
import sqlite3
from copy import deepcopy
from datetime import datetime

import recsys
from tests.containers import Recsys
from recsys.core.entity.fileset import Fileset
from recsys.core.entity.datasource import DataSource
from recsys.core.services.io import IOService
from recsys.core.dal.dao import DatasetDTO, FilesetDTO, DataSourceDTO, ProfileDTO, TaskDTO, TaskResourceDTO, JobDTO
from recsys.core.data.database import SQLiteConnection, SQLiteDatabase
# from recsys.core.entity.datasource import DataSource

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
    return SQLiteDatabase(connection=connection)


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
            stage="staged",
            uri=f"tests/file/dataset_dto_{i}.pkl",
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
def datasource_dto():

    fileset_dtos = []

    for i in range(1, 6):
        dto = FilesetDTO(
            id=i,
            name=f"fileset_dto_{i}",
            description=f"Description of fileset_dto_{i}",
            datasource="movielens25m",
            workspace="remote",
            stage="staged",
            uri="tests/data/movielens25m/raw/ratings.pkl",
            task_id=26,
            created=datetime.now(),
            modified=datetime.now()
        )
        fileset_dtos.append(dto)

    dto = DataSourceDTO(id=None, name="movielens25m", publisher="GroupLens", description="MovieLens25M Dataset",
                        website="wwww.grouplens.com/movielens/", created=datetime.now(), modified=None,
                        filesets=fileset_dtos)

    return dto


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def filesets():
    dtos = []
    for i in range(1, 6):
        dto = FilesetDTO(
            id=None,
            name=f"fileset_dto_{i}",
            description=f"Fileset Description DTO {i}",
            datasource="movielens25m",
            workspace="test",
            stage="interim",
            uri="tests/file/" + f"fileset_dto_{i}" + ".pkl",
            task_id=i + i,
            created=datetime.now(),
            modified=datetime.now(),
        )
        dtos.append(dto)
    return dtos


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasource_dtos(datasource_dto):
    dtos = []
    for i in range(1, 6):
        dto = deepcopy(datasource_dto)
        dto.id = i
        dtos.append(dto)
    return dtos


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
            name=f"task_resource_dto_{i}",
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
            name=f"job_dto_{i}",
            description=f"Description for Job # {i}",
            workspace="test",
            started=datetime.now(),
            ended=datetime.now(),
            duration=123,
            tasks_completed=5,
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
            "uri": "tests/file/" + f"fileset_dto_{i}" + ".pkl",
            "task_id": i + i,
        }
        lod.append(d)

    return lod


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasources():
    """Provide a list of DataSource objects."""
    ds_list = []
    datasources = IOService.read(DATA_SOURCE_FILEPATH, sheet_name="datasource")
    filesets = IOService.read(DATA_SOURCE_FILEPATH, sheet_name="fileset")
    for idx in datasources.index:
        ds = DataSource(name=datasources['name'][idx], publisher=datasources['publisher'][idx], description=datasources['description'][idx], website=datasources['website'][idx])
        fs = filesets.loc[filesets["datasource"] == ds.name]
        for idx2 in fs.index:
            fileset = Fileset(name=fs["name"][idx2], description=fs["description"][idx2], datasource=fs["datasource"][idx2], stage=fs["stage"][idx2], workspace=fs["workspace"][idx2], uri=fs["uri"][idx2], task_id=fs["task_id"][idx2])
            ds.add_fileset(fileset)
        ds_list.append(ds)

    return ds_list


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope='module', autouse=True)
def container():
    container = Recsys()
    container.init_resources()
    container.wire(modules=[recsys.__name__])
    return container
