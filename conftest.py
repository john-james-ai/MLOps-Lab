#!/usr/bin/mode python3
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
# Modified   : Tuesday December 27th 2022 11:35:02 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
import sqlite3
from datetime import datetime, timedelta

import recsys
from recsys.containers import Recsys
from recsys.core.services.io import IOService
from recsys.core.entity.dataset import DataFrame, Dataset, DatasetSpec
from recsys.core.entity.profile import Profile
from recsys.core.workflow.job import Job
from recsys.core.workflow.task import Task
from recsys.core.workflow.operator import NullOperator
from recsys.core.database.relational import RDB
from recsys.core.database.connection import SQLiteConnection
from recsys.core.repo.base import Context
from recsys.core.repo.uow import UnitOfWork
# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
RATINGS_FILEPATH = "tests/data/movielens25m/raw/ratings.pkl"
DATA_SOURCE_FILEPATH = "tests/data/datasources.xlsx"
JOB_CONFIG_FILEPATH = "recsys/data/movielens25m/config.yml"


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
    return RDB(connection=connection)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataset(ratings):
    dataset = Dataset(
        name=f"dataset_name_{1}",
        description=f"Dataset Description {1}",
        datasource="spotify",
        stage="extract",
        task_id=55,
    )
    for i in range(1, 6):
        dataframe = DataFrame(
            name=f"dataframe_name_{i}",
            description=f"Description for DataFrame {i}",
            data=ratings,
            parent=dataset,
        )
        dataset.add_dataframe(dataframe=dataframe)

    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def profiles():
    profiles = []
    for i in range(1, 6):
        profile = Profile(
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
        profiles.append(profile)
    return profiles


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def tasks(dataset):
    tasks = []
    for i in range(1, 6):
        input_spec = DatasetSpec(
            name=f"input_dataset_{i}",
            datasource='spotify',
            stage="extract"
        )
        output_spec = DatasetSpec(
            name=f"output_dataset_{i}",
            datasource='spotify',
            stage="interim"
        )
        task = Task(
            name=f"task_dto_{i}",
            description=f"Task Description DTO {i}",
            stage="extract",
            operator=NullOperator(),
            input_spec=input_spec,
            output_spec=output_spec,
            job_id=i * 10,
        )
        task.id = i
        task.run()
        tasks.append(task)
    return tasks


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def jobs():
    jobs = []
    for i in range(1, 6):
        job = Job(
            name=f"job_name_{i}",
            description=f"Description for Job # {i}",
            pipeline={},
            started=datetime.now() - timedelta(hours=i),
            ended=datetime.now(),
            duration=(datetime.now() - (datetime.now() - timedelta(hours=i))).total_seconds(),
            state="READY",
            created=datetime.now(),
            modified=datetime.now(),
        )
        jobs.append(job)
    return jobs


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dataframe_dicts():
    """List of dictionaries that can be used to instantiate DataFrame."""
    lod = []
    for i in range(1, 6):
        d = {
            "name": f"dataframe_{i}",
            "description": f"Description for DataFrame {i}",
            "datasource": "movielens25m",
            "mode": "test",
            "stage": "interim",
            "task_id": i + i,
        }
        lod.append(d)

    return lod


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def job_config():
    """List of dictionaries that can be used to instantiate DataFrame."""
    return IOService.read(JOB_CONFIG_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope='module', autouse=True)
def container():
    container = Recsys()
    container.init_resources()
    container.wire(modules=[recsys.containers, recsys.core.repo.base])
    return container


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope='module', autouse=True)
def uow():
    context = Context()
    uow = UnitOfWork(context=context)
    return uow
