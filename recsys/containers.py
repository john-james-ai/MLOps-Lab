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
# Modified   : Wednesday January 4th 2023 01:26:54 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import logging.config  # pragma: no cover
import pymysql
import dotenv

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.dal.dba import DBA
from recsys.core.dal.dao import FileDAO, DatasetDAO, DataFrameDAO, DataSourceDAO, DataSourceURLDAO
from recsys.core.dal.dao import JobDAO, TaskDAO, ProfileDAO
from recsys.core.dal.sql.database import DatabaseDDL
from recsys.core.dal.sql.file import FileDDL, FileDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.database.relational import Database, MySQLConnection
from recsys.core.database.object import ObjectDBConnection, ObjectDB


# ------------------------------------------------------------------------------------------------ #
class CoreContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )

    io = providers.Singleton(IOService)


# ------------------------------------------------------------------------------------------------ #
class ConnectionContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    recsys_connection = providers.Factory(
        MySQLConnection,
        pymysql.connect,
    )

    object_db_connection = providers.Factory(
        ObjectDBConnection,
        location=config.database.shelve.location,
    )


# ------------------------------------------------------------------------------------------------ #
class DatabaseContainer(containers.DeclarativeContainer):

    recsys_connection = providers.Dependency()
    object_db_connection = providers.Dependency()

    recsys = providers.Factory(
        Database,
        connection=recsys_connection
    )

    object_db = providers.Factory(
        ObjectDB,
        connection=object_db_connection
    )


# ------------------------------------------------------------------------------------------------ #
class DAOContainer(containers.DeclarativeContainer):

    file = providers.Factory(FileDAO, dml=FileDML)

    datasource = providers.Factory(DataSourceDAO, dml=DataSourceDML)

    datasource_url = providers.Factory(DataSourceURLDAO, dml=DataSourceURLDML)

    dataframe = providers.Factory(DataFrameDAO, dml=DataFrameDML)

    dataset = providers.Factory(DatasetDAO, dml=DatasetDML)

    job = providers.Factory(JobDAO, dml=JobDML)

    task = providers.Factory(TaskDAO, dml=TaskDML)

    profile = providers.Factory(ProfileDAO, dml=ProfileDML)


# ------------------------------------------------------------------------------------------------ #
class DBAContainer(containers.DeclarativeContainer):

    recsys = providers.Dependency()

    database = providers.Factory(DBA, ddl=DatabaseDDL)

    file = providers.Factory(DBA, ddl=FileDDL)

    datasource = providers.Factory(DBA, ddl=DataSourceDDL)

    datasource_url = providers.Factory(DBA, ddl=DataSourceURLDDL)

    dataframe = providers.Factory(DBA, ddl=DataFrameDDL)

    dataset = providers.Factory(DBA, ddl=DatasetDDL)

    job = providers.Factory(DBA, ddl=JobDDL)

    task = providers.Factory(DBA, ddl=TaskDDL)

    profile = providers.Factory(DBA, ddl=ProfileDDL)


# ------------------------------------------------------------------------------------------------ #
class Recsys(containers.DeclarativeContainer):

    dotenv.load_dotenv()
    mode = os.getenv("MODE")
    logging_config_filepath = os.path.join('config', mode, "logging.yml")
    database_config_filepath = os.path.join('config', mode, "database.yml")

    config = providers.Configuration(yaml_files=[logging_config_filepath, database_config_filepath])

    core = providers.Container(CoreContainer, config=config)

    connection = providers.Container(ConnectionContainer, config=config)

    database = providers.Container(DatabaseContainer,
                                   recsys_connection=connection.recsys_connection,
                                   )

    dao = providers.Container(DAOContainer)

    dba = providers.Container(DBAContainer)
