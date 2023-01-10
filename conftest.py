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
# Modified   : Monday January 9th 2023 05:28:22 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pytest
from datetime import datetime

import recsys
from recsys.containers import Recsys
from recsys.core.services.io import IOService
from recsys.core.entity.file import File
from recsys.core.entity.dataset import DataFrame, Dataset
from recsys.core.entity.datasource import DataSource
from recsys.core.entity.profile import Profile
from recsys.core.entity.job import Job
from recsys.core.repo.context import Context


# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
RATINGS_FILEPATH = "tests/data/movielens25m/raw/ratings.pkl"
DATA_SOURCE_FILEPATH = "tests/data/datasources.xlsx"
JOB_CONFIG_FILEPATH = "recsys/data/movielens25m/config.yml"

# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["*er.py"]
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
def datasource(ratings):
    datasource = DataSource(
        name=f"datasource_id_{1}",
        description=f"Datasource Description {1}",
        website="www.spotify.com",
    )
    for i in range(1, 6):
        url = datasource.create_url(url=f"www.spotify.resource_{i}.com", name=f"datasource_url_name_{i}", description=f"Description for DataFrame {i}")
        datasource.add_url(url)

    return datasource


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def file():

    file = File(
        name="test_file",
        description="Test File Description",
        datasource="movielens25m",
        stage="extract",
        uri="tests/data/movielens25m/raw/ratings.pkl",
        mode="test",
        task_id=55,
    )
    return file


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def files():
    files = []
    for i in range(1, 6):
        file = File(
            name=f"file_{i}",
            description=f"Test File Description {i}",
            datasource_id=i % 3,
            stage="extract",
            uri="tests/data/movielens25m/raw/ratings.pkl",
            task_id=i + 22,
            mode='test',
        )
        files.append(file)
    return files


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
            mode='test',
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
            parent_id=i * 20,
            created=datetime.now(),
            modified=datetime.now(),
        )
        profiles.append(profile)
    return profiles


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def datasets(ratings):
    datasets = []
    for i in range(1, 6):
        dataset = Dataset(
            name=f"dataset_name_{i}",
            datasource_id=i + 5,
            description=f"Description for Dataset # {i}",
            stage="extract",
            mode="test",
        )
        for j in range(1, 6):
            j = j + (i - 1)
            dataframe = DataFrame(
                name=f"dataframe_{j}_dataset_{i}",
                data=ratings,
                parent=dataset,
                description=f"Description for dataframe {j} of dataset {i}",
                mode="test",
                stage="extract",
            )
            dataset.add_dataframe(dataframe)
        datasets.append(dataset)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def jobs():
    jobs = []
    for i in range(1, 6):
        job = Job(
            name=f"job_name_{i}",
            description=f"Description for Job # {i}",
            mode="test"
        )
        for j in range(1, 6):
            task = job.create_task(name=f"task_{j}_job_{i}",
                                   description=f"Description for task {j} of job {i}"
                                   )
            job.add_task(task)
        jobs.append(job)
    return jobs


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def datasources():
    datasources = []
    sources = ['spotify', 'movielens25m', 'tenrec']
    for i in range(1, 6):
        datasource = DataSource(
            name=f"{sources[i % 3]}_{i}",
            description=f"Description for DataSource # {i}",
            website="www.some_website.com"
        )
        for j in range(1, 6):
            datasource_url = datasource.create_url(
                name=f"datasource_url_{j}",
                url=f"www.url_{j}.com",
                description=f"Description for datasource_url {j} of datasource {i}"
            )
            datasource.add_url(datasource_url)
        datasources.append(datasource)
    return datasources


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
    container.wire(modules=[recsys.containers,
                            "recsys.setup.reset_db",
                            "recsys.core.repo.context"])
    return container


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def context():
    return Context()
