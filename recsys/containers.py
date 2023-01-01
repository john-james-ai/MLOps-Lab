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
# Modified   : Sunday January 1st 2023 01:54:13 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.repo.context import Context
from recsys.core.repo.entity import Repo
from recsys.core.repo.job import JobRepo
from recsys.core.entity.file import File
from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.entity.profile import Profile
from recsys.core.dal.dao import DataFrameDAO, DatasetDAO, JobDAO, TaskDAO, ProfileDAO, FileDAO, DataSourceDAO, DataSourceURLDAO
from recsys.core.dal.dba import DBA
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from recsys.core.dal.sql.file import FileDDL, FileDML
from recsys.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.job import JobDDL, JobDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.database.relational import RDB
from recsys.core.database.object import ODB
from recsys.core.database.connection import SQLiteConnection, ODBConnection


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

    rdb_connection = providers.Factory(
        SQLiteConnection,
        connector=sqlite3.connect,
        location=config.database.sqlite.location,
    )

    odb_connection = providers.Factory(
        ODBConnection,
        dbfile=config.database.shelve.location,
    )

    rdb = providers.Factory(RDB, connection=rdb_connection)
    odb = providers.Factory(ODB, connection=odb_connection)


class TableContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()
    odb = providers.Dependency()

    file = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=FileDDL)

    datasource = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=DataSourceDDL)

    datasource_url = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=DataSourceURLDDL)

    dataframe = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=DataFrameDDL)

    dataset = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=DatasetDDL)

    job = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=JobDDL)

    task = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=TaskDDL)

    profile = providers.Factory(DBA, rdb=rdb, odb=odb, ddl=ProfileDDL)


class DAOContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()

    odb = providers.Dependency()

    file = providers.Factory(FileDAO, rdb=rdb, odb=odb, dml=FileDML)

    datasource = providers.Factory(DataSourceDAO, rdb=rdb, odb=odb, dml=DataSourceDML)

    datasource_url = providers.Factory(DataSourceURLDAO, rdb=rdb, odb=odb, dml=DataSourceURLDML)

    dataframe = providers.Factory(DataFrameDAO, rdb=rdb, odb=odb, dml=DataFrameDML)

    dataset = providers.Factory(DatasetDAO, rdb=rdb, odb=odb, dml=DatasetDML)

    job = providers.Factory(JobDAO, rdb=rdb, odb=odb, dml=JobDML)

    task = providers.Factory(TaskDAO, rdb=rdb, odb=odb, dml=TaskDML)

    profile = providers.Factory(ProfileDAO, rdb=rdb, odb=odb, dml=ProfileDML)


class ContextContainer(containers.DeclarativeContainer):

    dao = providers.Dependency()

    context = providers.Factory(Context, dao=dao)


class RepoContainer(containers.DeclarativeContainer):

    context = providers.Dependency()

    dataset = providers.Factory(Repo, context=context, entity=Dataset)

    dataframe = providers.Factory(Repo, context=context, entity=DataFrame)

    file = providers.Factory(Repo, context=context, entity=File)

    datasource = providers.Factory(Repo, context=context, entity=DataSource)

    datasource_url = providers.Factory(Repo, context=context, entity=DataSourceURL)

    job = providers.Factory(JobRepo, context=context)

    profile = providers.Factory(Repo, context=context, entity=Profile)


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    core = providers.Container(CoreContainer, config=config.test.core)  # substitute test for the mode (prod, dev) of interest

    data = providers.Container(DataLayerContainer, config=config.test.data)

    table = providers.Container(TableContainer, rdb=data.rdb, odb=data.odb)

    dao = providers.Container(DAOContainer, rdb=data.rdb, odb=data.odb)

    context = providers.Container(ContextContainer, dao=dao)

    repos = providers.Container(RepoContainer, context=context.context)
