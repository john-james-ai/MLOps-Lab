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
# Modified   : Saturday January 7th 2023 01:02:12 pm                                               #
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
from recsys.core.dal.dba import DBA, ODBA
from recsys.core.dal.oao import OAO
from recsys.core.dal.dao import FileDAO, DatasetDAO, DataFrameDAO, DataSourceDAO, DataSourceURLDAO
from recsys.core.dal.dao import JobDAO, TaskDAO, ProfileDAO
from recsys.core.dal.sql.file import FileDDL, FileDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.database import DatabaseDDL
from recsys.core.dal.sql.odb import ObjectODL, ObjectOML
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

    mysql_connection = providers.Factory(
        MySQLConnection
    )

    object_db_connection = providers.Factory(
        ObjectDBConnection,
        location=config.database.shelve.location,
    )


# ------------------------------------------------------------------------------------------------ #
class DatabaseContainer(containers.DeclarativeContainer):

    recsys_connection = providers.Dependency()
    object_db_connection = providers.Dependency()

    recsys = providers.Singleton(
        Database,
        connection=recsys_connection
    )

    object_db = providers.Singleton(
        ObjectDB,
        connection=object_db_connection
    )


# ------------------------------------------------------------------------------------------------ #
class DALContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()
    odb = providers.Dependency()

    file = providers.Factory(FileDAO, dml=FileDML, database=rdb)

    datasource = providers.Factory(DataSourceDAO, dml=DataSourceDML, database=rdb)

    datasource_url = providers.Factory(DataSourceURLDAO, dml=DataSourceURLDML, database=rdb)

    dataframe = providers.Factory(DataFrameDAO, dml=DataFrameDML, database=rdb)

    dataset = providers.Factory(DatasetDAO, dml=DatasetDML, database=rdb)

    job = providers.Factory(JobDAO, dml=JobDML, database=rdb)

    task = providers.Factory(TaskDAO, dml=TaskDML, database=rdb)

    profile = providers.Factory(ProfileDAO, dml=ProfileDML, database=rdb)

    object = providers.Factory(OAO, dml=ObjectOML, database=odb)


# ------------------------------------------------------------------------------------------------ #
class DBAContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()
    odb = providers.Dependency()

    database = providers.Factory(DBA, database=rdb, ddl=DatabaseDDL)

    file = providers.Factory(DBA, database=rdb, ddl=FileDDL)

    datasource = providers.Factory(DBA, database=rdb, ddl=DataSourceDDL)

    datasource_url = providers.Factory(DBA, database=rdb, ddl=DataSourceURLDDL)

    dataframe = providers.Factory(DBA, database=rdb, ddl=DataFrameDDL)

    dataset = providers.Factory(DBA, database=rdb, ddl=DatasetDDL)

    job = providers.Factory(DBA, database=rdb, ddl=JobDDL)

    task = providers.Factory(DBA, database=rdb, ddl=TaskDDL)

    profile = providers.Factory(DBA, database=rdb, ddl=ProfileDDL)

    object = providers.Factory(ODBA, database=odb, ddl=ObjectODL)


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

    dal = providers.Container(DALContainer,
                              rdb=database.recsys,
                              odb=database.object_db)

    dba = providers.Container(DBAContainer,
                              rdb=database.recsys,
                              odb=database.object_db)
