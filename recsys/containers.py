#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/containers.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 11:21:14 am                                              #
# Modified   : Saturday December 17th 2022 03:40:06 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.dal.repo import Repo, Context
from recsys.core.entity.dataset import Dataset
from recsys.core.entity.job import Job
from recsys.core.entity.task import Task
from recsys.core.entity.operation import Operation
from recsys.core.entity.profile import Profile
from recsys.core.dal.dao import DatasetDAO, JobDAO, TaskDAO, ProfileDAO, OperationDAO
from recsys.core.dal.ddo import TableService
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.operation import OperationDDL, OperationDML
from recsys.core.data.connection import SQLiteConnection
from recsys.core.data.database import Database

# ------------------------------------------------------------------------------------------------ #


class CoreContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )

    io = providers.Singleton(IOService)


class DataLayerContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    connection = providers.Factory(
        SQLiteConnection,
        connector=sqlite3.connect,
        location=config.database.sqlite.location,
    )

    database = providers.Factory(Database, connection=connection)


class TableContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    dataset = providers.Factory(TableService, database=database, ddl=DatasetDDL)

    operation = providers.Factory(TableService, database=database, ddl=OperationDDL)

    job = providers.Factory(TableService, database=database, ddl=JobDDL)

    task = providers.Factory(TableService, database=database, ddl=TaskDDL)

    profile = providers.Factory(TableService, database=database, ddl=ProfileDDL)


class DAOContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    dataset = providers.Factory(DatasetDAO, database=database, dml=DatasetDML)

    operation = providers.Factory(OperationDAO, database=database, dml=OperationDML)

    job = providers.Factory(JobDAO, database=database, dml=JobDML)

    task = providers.Factory(TaskDAO, database=database, dml=TaskDML)

    profile = providers.Factory(ProfileDAO, database=database, dml=ProfileDML)


class RepoContainer(containers.DeclarativeContainer):

    entity = providers.Dependency()
    dao = providers.Dependency()

    repo = providers.Singleton(
        Repo,
        dataset=entity,
        dao=dao,
    )


class ContextContainer(containers.DeclarativeContainer):

    dataset_repo = providers.Dependency(instance_of=Repo)

    operation_repo = providers.Dependency(instance_of=Repo)

    job_repo = providers.Dependency(instance_of=Repo)

    task_repo = providers.Dependency(instance_of=Repo)

    profile_repo = providers.Dependency(instance_of=Repo)

    context = providers.Singleton(
        Context,
        dataset=dataset_repo,
        operation=operation_repo,
        job=job_repo,
        task=task_repo,
        profile=profile_repo,
    )


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    core = providers.Container(CoreContainer, config=config.core)

    data = providers.Container(DataLayerContainer, config=config.data)

    table = providers.Container(TableContainer, database=data.database)

    dao = providers.Container(DAOContainer, database=data.database)

    dataset_repo = providers.Container(RepoContainer, entity=Dataset, dao=dao.dataset)

    operation_repo = providers.Container(RepoContainer, entity=Operation, dao=dao.operation)

    job_repo = providers.Container(RepoContainer, entity=Job, dao=dao.job)

    task_repo = providers.Container(RepoContainer, entity=Task, dao=dao.task)

    profile_repo = providers.Container(RepoContainer, entity=Profile, dao=dao.profile)

    context = providers.Container(ContextContainer,
                                  dataset_repo=dataset_repo(),
                                  operation_repo=operation_repo(),
                                  job_repo=job_repo(),
                                  task_repo=task_repo(),
                                  profile_repo=profile_repo())
