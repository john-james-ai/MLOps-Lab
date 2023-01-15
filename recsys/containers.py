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
# Modified   : Saturday January 14th 2023 08:16:43 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import pymysql
import dotenv

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.dal.sql.rdb import DatabaseDDL as RecsysDatabaseDDL
from recsys.core.dal.sql.edb import DatabaseDDL as EventsDatabaseDDL
from recsys.core.services.io import IOService
from recsys.core.dal.dba import DBA, ODBA
from recsys.core.dal.oao import OAO
from recsys.core.dal.dao import FileDAO, DatasetDAO, DataFrameDAO, DataSourceDAO, DataSourceURLDAO
from recsys.core.dal.dao import JobDAO, TaskDAO, ProfileDAO, EventDAO
from recsys.core.dal.sql.file import FileDDL, FileDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.event import EventDDL, EventDML
from recsys.core.dal.sql.odb import ObjectODL, ObjectOML
from recsys.core.database.relational import Database, MySQLConnection, DatabaseConnection
from recsys.core.database.object import ObjectDBConnection, ObjectDB
from recsys.core.repo.context import Context
from recsys.core.repo.uow import UnitOfWork
from recsys.core.repo.entity import Repo
from recsys.core.repo.dataset import DatasetRepo
from recsys.core.repo.datasource import DataSourceRepo
from recsys.core.repo.job import JobRepo
from recsys.core.factory.dataset import DataFrameFactory, DatasetFactory
from recsys.core.factory.datasource import DataSourceFactory, DataSourceURLFactory
from recsys.core.factory.event import EventFactory
from recsys.core.factory.job import JobFactory, TaskFactory


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

    recsys_database = providers.Dependency()
    events_database = providers.Dependency()

    dbms_connection = providers.Factory(
        MySQLConnection, connector=pymysql.connect, autocommit=False, autoclose=False
    )

    rdb_connection = providers.Factory(
        DatabaseConnection,
        connector=pymysql.connect,
        database=recsys_database,
        autocommit=False,
        autoclose=False,
    )

    edb_connection = providers.Factory(
        DatabaseConnection,
        connector=pymysql.connect,
        database=events_database,
        autocommit=True,
        autoclose=False,
    )

    odb_connection = providers.Factory(
        ObjectDBConnection,
    )


# ------------------------------------------------------------------------------------------------ #
class DatabaseContainer(containers.DeclarativeContainer):

    dbms_connection = providers.Dependency()
    rdb_connection = providers.Dependency()
    edb_connection = providers.Dependency()
    odb_connection = providers.Dependency()

    dbms = providers.Singleton(Database, connection=dbms_connection)

    rdb = providers.Singleton(Database, connection=rdb_connection)

    edb = providers.Singleton(Database, connection=edb_connection)

    odb = providers.Singleton(ObjectDB, connection=odb_connection)


# ------------------------------------------------------------------------------------------------ #
class DALContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()
    edb = providers.Dependency()
    odb = providers.Dependency()

    file = providers.Factory(FileDAO, dml=FileDML, database=rdb)

    datasource = providers.Factory(DataSourceDAO, dml=DataSourceDML, database=rdb)

    datasource_url = providers.Factory(DataSourceURLDAO, dml=DataSourceURLDML, database=rdb)

    dataframe = providers.Factory(DataFrameDAO, dml=DataFrameDML, database=rdb)

    dataset = providers.Factory(DatasetDAO, dml=DatasetDML, database=rdb)

    job = providers.Factory(JobDAO, dml=JobDML, database=rdb)

    task = providers.Factory(TaskDAO, dml=TaskDML, database=rdb)

    profile = providers.Factory(ProfileDAO, dml=ProfileDML, database=rdb)

    event = providers.Factory(EventDAO, dml=EventDML, database=edb)

    object = providers.Factory(OAO, oml=ObjectOML, database=odb)


# ------------------------------------------------------------------------------------------------ #
class DBAContainer(containers.DeclarativeContainer):

    dbms = providers.Dependency()
    rdb = providers.Dependency()
    edb = providers.Dependency()
    odb = providers.Dependency()

    recsys_database = providers.Factory(DBA, database=dbms, ddl=RecsysDatabaseDDL)

    events_database = providers.Factory(DBA, database=dbms, ddl=EventsDatabaseDDL)

    file = providers.Factory(DBA, database=rdb, ddl=FileDDL)

    datasource = providers.Factory(DBA, database=rdb, ddl=DataSourceDDL)

    datasource_url = providers.Factory(DBA, database=rdb, ddl=DataSourceURLDDL)

    dataframe = providers.Factory(DBA, database=rdb, ddl=DataFrameDDL)

    dataset = providers.Factory(DBA, database=rdb, ddl=DatasetDDL)

    job = providers.Factory(DBA, database=rdb, ddl=JobDDL)

    task = providers.Factory(DBA, database=rdb, ddl=TaskDDL)

    profile = providers.Factory(DBA, database=rdb, ddl=ProfileDDL)

    event = providers.Factory(DBA, database=edb, ddl=EventDDL)

    object = providers.Factory(ODBA, database=odb, ddl=ObjectODL)


# ------------------------------------------------------------------------------------------------ #
class RepoContainer(containers.DeclarativeContainer):

    dal = providers.Dependency()

    context = providers.Factory(Context, dal=dal)

    file = providers.Factory(Repo, context=context, entity="file")

    profile = providers.Factory(Repo, context=context, entity="profile")

    event = providers.Factory(Repo, context=context, entity="event")

    datasource = providers.Factory(DataSourceRepo, context=context)

    dataset = providers.Factory(DatasetRepo, context=context)

    job = providers.Factory(JobRepo, context=context)


# ------------------------------------------------------------------------------------------------ #
class WorkContainer(containers.DeclarativeContainer):

    repos = providers.Dependency()

    unit = providers.Factory(UnitOfWork, repos=repos)


# ------------------------------------------------------------------------------------------------ #
class FactoryContainer(containers.DeclarativeContainer):

    dataset = providers.Factory(DatasetFactory)
    dataframe = providers.Factory(DataFrameFactory)
    datasource = providers.Factory(DataSourceFactory)
    datasource_url = providers.Factory(DataSourceURLFactory)
    job = providers.Factory(JobFactory)
    task = providers.Factory(TaskFactory)
    event = providers.Factory(EventFactory)


# ------------------------------------------------------------------------------------------------ #
class Recsys(containers.DeclarativeContainer):

    dotenv.load_dotenv()

    config = providers.Configuration(yaml_files=["config.yml"])
    config_datasources = providers.Configuration(yaml_files=["recsys.data.etl.datasources.yml"])

    core = providers.Container(CoreContainer, config=config)

    connection = providers.Container(
        ConnectionContainer,
        recsys_database=config.databases.recsys,
        events_database=config.databases.events,
    )

    database = providers.Container(
        DatabaseContainer,
        dbms_connection=connection.dbms_connection,
        rdb_connection=connection.rdb_connection,
        edb_connection=connection.edb_connection,
        odb_connection=connection.odb_connection,
    )

    dal = providers.Container(DALContainer, rdb=database.rdb, edb=database.edb, odb=database.odb)

    dba = providers.Container(
        DBAContainer, dbms=database.dbms, rdb=database.rdb, edb=database.edb, odb=database.odb
    )

    repo = providers.Container(RepoContainer, dal=dal)

    work = providers.Container(WorkContainer, repos=repo)

    factory = providers.Container(FactoryContainer)
