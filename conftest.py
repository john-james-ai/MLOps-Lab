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
# Created    : Wednesday January 11th 2023 06:32:03 pm                                             #
# Modified   : Sunday January 22nd 2023 04:10:12 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import pytest
import pandas as pd
from datetime import datetime

from recsys.container import Recsys
from recsys.core.service.io import IOService
from recsys.core.entity.file import File
from recsys.core.entity.dataset import DataFrame, Dataset
from recsys.core.entity.datasource import DataSource
from recsys.core.workflow.profile import Profile
from recsys.core.workflow.dag import DAG, Task
from tests.data.operator import MockOperator, BadOperator
from recsys.core.workflow.callback import DAGCallback, TaskCallback

# ------------------------------------------------------------------------------------------------ #
TEST_LOCATION = "tests/test.sqlite3"
RATINGS_FILEPATH = "tests/data/movielens25m/raw/ratings.pkl"
DATA_SOURCE_FILEPATH = "tests/data/datasources.xlsx"
JOB_CONFIG_FILEPATH = "recsys/data/movielens25m/config.yml"
DAG_IMPORT = "tests/data/import/dag.csv"
DATAFRAME_IMPORT = "tests/data/import/dataframe.csv"
DATASET_IMPORT = "tests/data/import/dataset.csv"
DATASOURCE_IMPORT = "tests/data/import/datasource.csv"
DATASOURCE_URL_IMPORT = "tests/data/import/datasource_url.csv"
EVENT_IMPORT = "tests/data/import/event.csv"
TASK_IMPORT = "tests/data/import/task.csv"
IMPORT_FILES = {
    "dag": DAG_IMPORT,
    "task": TASK_IMPORT,
    "event": EVENT_IMPORT,
    "dataset": DATASET_IMPORT,
    "dataframe": DATAFRAME_IMPORT,
    "datasource": DATASOURCE_IMPORT,
    "datasource_url": DATASOURCE_URL_IMPORT,
}
# ------------------------------------------------------------------------------------------------ #
# collect_ignore_glob = ["*test_builder.py"]
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def location():
    return TEST_LOCATION


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def test_data():
    data = {}
    for name, filepath in IMPORT_FILES.items():
        df = pd.read_csv(filepath, index_col=[0])
        data[name] = df.to_dict(orient="index")
    return data


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def ratings():
    return IOService.read(RATINGS_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasource():
    datasource = DataSource(
        name=f"datasource_name_{1}",
        description=f"Datasource Description {1}",
        website="www.spotify.com",
    )
    for i in range(1, 6):
        url = datasource.create_url(
            url=f"www.spotify.resource_{i}.com",
            name=f"datasource_url_name_{i}",
            description=f"Description for DataFrame {i}",
        )
        datasource.add_url(url)

    return datasource


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def datasource_urls():
    urls = []
    datasource = DataSource(
        name=f"datasource_name_{1}",
        description=f"Datasource Description {1}",
        website="www.spotify.com",
    )
    for i in range(1, 6):
        url = datasource.create_url(
            url=f"www.spotify.resource_{i}.com",
            name=f"datasource_url_name_{i}",
            description=f"Description for DataFrame {i}",
        )
        urls.append(url)

    return urls


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def file():

    file = File(
        name="test_file",
        description="Test File Description",
        datasource="movielens25m",
        stage="extract",
        uri="tests/data/movielens25m/raw/ratings.pkl",
        task_oid=55,
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
            datasource_oid=i % 3,
            stage="extract",
            uri="tests/data/movielens25m/raw/ratings.pkl",
            task_oid=i + 22,
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
        task_oid=55,
    )
    for i in range(1, 6):
        dataframe = DataFrame(
            name=f"dataframe_name_{i}",
            description=f"Description for DataFrame {i}",
            data=ratings,
            dataset=dataset,
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
            parent_oid=i * 20,
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
            datasource_oid=i + 5,
            description=f"Description for Dataset # {i}",
            stage="extract",
        )
        for j in range(1, 6):
            j = j + (i - 1)
            dataframe = DataFrame(
                name=f"dataframe_{j}_dataset_{i}",
                data=ratings,
                dataset=dataset,
                description=f"Description for dataframe {j} of dataset {i}",
                stage="extract",
            )
            dataset.add_dataframe(dataframe)
        datasets.append(dataset)
    return datasets


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class")
def dataframes(ratings):
    dataframes = []
    dataset = Dataset(
        name="dataset_for_dataframe_testing",
        description="Dataset for DataFrame Testing",
        stage="extract",
        datasource_oid="datasource_tenrec",
        task_oid="task_test_dataframe",
    )
    for i in range(1, 6):
        dataframe = DataFrame(
            name=f"dataframe_{i}",
            data=ratings,
            dataset=dataset,
            description=f"Description for dataframe {i}",
            stage="extract",
        )
        dataframes.append(dataframe)
    return dataframes


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dag(dags):
    return dags[0]


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def tasks(dags):
    tasks = []
    for i in range(1, 6):
        task = Task(
            name=f"task_{i}", description=f"Description for task {i}.", operator=MockOperator()
        )
        task.id = i
        task.dag = dags[i - 1]
        tasks.append(task)

    return tasks


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def datasources():
    datasources = []
    sources = ["spotify", "movielens25m", "tenrec"]
    for i in range(1, 6):
        datasource = DataSource(
            name=f"{sources[i % 3]}_{i}",
            description=f"Description for DataSource # {i}",
            website="www.some_website.com",
        )
        for j in range(1, 6):
            datasource_url = datasource.create_url(
                name=f"datasource_url_{j}_of_datasource_{i}",
                url=f"www.url_{j}.com",
                description=f"Description for datasource_url {j} of datasource {i}",
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
            "stage": "interim",
            "task_oid": i + i,
        }
        lod.append(d)

    return lod


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def dag_config():
    """List of dictionaries that can be used to instantiate DataFrame."""
    return IOService.read(JOB_CONFIG_FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class", autouse=True)
def container():
    container = Recsys()
    container.init_resources()
    container.wire(
        modules=["recsys.container"],
        packages=["recsys.core.workflow"],
    )

    return container


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class", autouse=True)
def clean_container(container):
    container.dba.dataset().reset()
    container.dba.dataframe().reset()
    container.dba.datasource().reset()
    container.dba.datasource_url().reset()
    container.dba.file().reset()
    container.dba.profile().reset()
    container.dba.dag().reset()
    container.dba.task().reset()
    container.dba.event().reset()
    return container


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class", autouse=True)
def loaded_container(clean_container):
    clean_container.dal().dag().load(DAG_IMPORT)
    clean_container.dal().task().load(TASK_IMPORT)
    clean_container.dal().datasource().load(DATASOURCE_IMPORT)
    clean_container.dal().datasource_url().load(DATASOURCE_URL_IMPORT)
    clean_container.dal().dataset().load(DATASET_IMPORT)
    clean_container.dal().dataframe().load(DATAFRAME_IMPORT)
    clean_container.dal().event().load(EVENT_IMPORT)
    return clean_container


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def context(loaded_container):
    return loaded_container.repo.context()


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def mockdag():
    dag = DAG(name="test_mockdag", description="Test Mock DAG")
    for i in range(1, 6):
        task = Task(
            name=f"test_mockdag_{i}", description=f"Test MockDAG Task {i}", operator=MockOperator()
        )
        dag.add_task(task)
    return dag


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module")
def baddag():
    dag = DAG(name="test_baddag", description="Test Bad DAG")
    for i in range(1, 6):
        task = Task(
            name=f"test_baddag_{i}", description=f"Test BadDAG Task {i}", operator=BadOperator()
        )
        dag.add_task(task)
    return dag


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def dags():
    dags = []
    for i in range(1, 6):
        dag = DAG(
            name=f"dag_name_{i}", description=f"Description for DAG # {i}", callback=DAGCallback()
        )
        for j in range(1, 6):
            task = Task(
                name=f"task_{j}_baddag_{i}",
                description=f"Description for task {j} of dag {i}",
                callback=TaskCallback(),
            )
            if j == 5:
                task.operator = BadOperator()
            else:
                task.operator = MockOperator()
            dag.add_task(task)
        dags.append(dag)
    return dags
