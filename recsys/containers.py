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
# Modified   : Thursday December 8th 2022 06:12:13 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.dal.dao import DatasetDAO, FilesetDAO, JobDAO, DataSourceDAO, TaskDAO
from recsys.core.dal.ddo import TableService
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.fileset import FilesetDDL, FilesetDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.data.database import SQLiteDatabase, SQLiteConnection

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


class DataAccessLayerContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    # Tables
    dataset_table = providers.Factory(TableService, database=database, ddl=DatasetDDL)

    fileset_table = providers.Factory(TableService, database=database, ddl=FilesetDDL)

    job_table = providers.Factory(TableService, database=database, ddl=JobDDL)

    datasource_table = providers.Factory(TableService, database=database, ddl=DataSourceDDL)

    task_table = providers.Factory(TableService, database=database, ddl=TaskDDL)

    # DAO
    dataset_dao = providers.Factory(DatasetDAO, database=database, dml=DatasetDML)

    fileset_dao = providers.Factory(FilesetDAO, database=database, dml=FilesetDML)

    job_dao = providers.Factory(JobDAO, database=database, dml=JobDML)

    datasource_dao = providers.Factory(DataSourceDAO, database=database, dml=DataSourceDML)

    task_dao = providers.Factory(TaskDAO, database=database, dml=TaskDML)


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    core = providers.Container(CoreContainer, config=config.core)

    data = providers.Container(DataLayerContainer, config=config.data)

    dal = providers.Container(DataAccessLayerContainer, database=data.database)
