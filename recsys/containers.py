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
# Modified   : Tuesday January 3rd 2023 06:03:29 am                                                #
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
from recsys.core.dal.sql.database import DatabaseDDL
from recsys.core.dal.sql.file import FileDDL
from recsys.core.dal.sql.datasource import DataSourceDDL
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL
from recsys.core.dal.sql.dataset import DatasetDDL
from recsys.core.dal.sql.dataframe import DataFrameDDL
from recsys.core.dal.sql.job import JobDDL
from recsys.core.dal.sql.task import TaskDDL
from recsys.core.dal.sql.profile import ProfileDDL
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
        host=config.database.host,
        user=config.database.user,
        password=config.database.password,
        database=config.database.dbname
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
class DBAContainer(containers.DeclarativeContainer):

    recsys = providers.Dependency()
    object_db = providers.Dependency()

    database = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=DatabaseDDL)

    file = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=FileDDL)

    datasource = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=DataSourceDDL)

    datasource_url = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=DataSourceURLDDL)

    dataframe = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=DataFrameDDL)

    dataset = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=DatasetDDL)

    job = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=JobDDL)

    task = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=TaskDDL)

    profile = providers.Factory(DBA, database=recsys, object_db=object_db, ddl=ProfileDDL)


# ------------------------------------------------------------------------------------------------ #
class Recsys(containers.DeclarativeContainer):

    dotenv.load_dotenv()
    mode = os.getenv("MODE")
    logging_config_filepath = os.path.join('config', mode, "logging.yml")
    database_config_filepath = os.path.join('config', mode, "database.yml")

    os.environ["HOST"] = os.getenv("MYSQL_HOST")
    os.environ["USER"] = os.getenv("MYSQL_USER")
    os.environ["PASSWORD"] = os.getenv("MYSQL_PASSWORD")
    os.environ["DATABASE"] = os.getenv("MYSQL_DATABASE")

    config = providers.Configuration(yaml_files=[logging_config_filepath, database_config_filepath])
    config.database.host.from_env("HOST")
    config.database.user.from_env("USER")
    config.database.password.from_env("PASSWORD")
    config.database.dbname.from_env("DATABASE")

    core = providers.Container(CoreContainer, config=config)

    connection = providers.Container(ConnectionContainer, config=config)

    database = providers.Container(DatabaseContainer,
                                   recsys_connection=connection.recsys_connection,
                                   object_db_connection=connection.object_db_connection
                                   )

    dba = providers.Container(DBAContainer,
                              recsys=database.recsys,
                              object_db=database.object_db,
                              )
