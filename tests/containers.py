#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/containers.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 11:21:14 am                                              #
# Modified   : Tuesday December 13th 2022 03:16:27 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.dal.dao import DatasetDAO, FilesetDAO, JobDAO, DataSourceDAO, TaskDAO, TaskResourceDAO, ProfileDAO
from recsys.core.dal.ddo import TableService
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.fileset import FilesetDDL, FilesetDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.task_resource import TaskResourceDDL, TaskResourceDML
from recsys.core.database.sqlite import SQLiteDatabase, SQLiteConnection

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

    database = providers.Factory(SQLiteDatabase, connection=connection)


class TableContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    dataset = providers.Factory(TableService, database=database, ddl=DatasetDDL)

    fileset = providers.Factory(TableService, database=database, ddl=FilesetDDL)

    job = providers.Factory(TableService, database=database, ddl=JobDDL)

    datasource = providers.Factory(TableService, database=database, ddl=DataSourceDDL)

    task = providers.Factory(TableService, database=database, ddl=TaskDDL)

    task_resource = providers.Factory(TableService, database=database, ddl=TaskResourceDDL)

    profile = providers.Factory(TableService, database=database, ddl=ProfileDDL)


class DAOContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    dataset = providers.Factory(DatasetDAO, database=database, dml=DatasetDML)

    fileset = providers.Factory(FilesetDAO, database=database, dml=FilesetDML)

    job = providers.Factory(JobDAO, database=database, dml=JobDML)

    datasource = providers.Factory(DataSourceDAO, database=database, dml=DataSourceDML)

    task = providers.Factory(TaskDAO, database=database, dml=TaskDML)

    task_resource = providers.Factory(TaskResourceDAO, database=database, dml=TaskResourceDML)

    profile = providers.Factory(ProfileDAO, database=database, dml=ProfileDML)


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["tests/config.yml"])

    core = providers.Container(CoreContainer, config=config.core)

    data = providers.Container(DataLayerContainer, config=config.data)

    table = providers.Container(TableContainer, database=data.database)

    dao = providers.Container(DAOContainer, database=data.database)
