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
# Modified   : Wednesday December 28th 2022 02:41:00 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.services.io import IOService
from recsys.core.dal.dao import DataFrameDAO, DatasetDAO, JobDAO, TaskDAO, ProfileDAO, FileDAO
from recsys.core.dal.ddo import TableService
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

    file = providers.Factory(TableService, database=rdb, ddl=FileDDL)

    dataframe = providers.Factory(TableService, database=rdb, ddl=DataFrameDDL)

    dataset = providers.Factory(TableService, database=rdb, ddl=DatasetDDL)

    job = providers.Factory(TableService, database=rdb, ddl=JobDDL)

    task = providers.Factory(TableService, database=rdb, ddl=TaskDDL)

    profile = providers.Factory(TableService, database=rdb, ddl=ProfileDDL)


class DAOContainer(containers.DeclarativeContainer):

    rdb = providers.Dependency()

    odb = providers.Dependency()

    file = providers.Factory(FileDAO, rdb=rdb, odb=odb, dml=FileDML)

    dataframe = providers.Factory(DataFrameDAO, rdb=rdb, odb=odb, dml=DataFrameDML)

    dataset = providers.Factory(DatasetDAO, rdb=rdb, odb=odb, dml=DatasetDML)

    job = providers.Factory(JobDAO, rdb=rdb, odb=odb, dml=JobDML)

    task = providers.Factory(TaskDAO, rdb=rdb, odb=odb, dml=TaskDML)

    profile = providers.Factory(ProfileDAO, rdb=rdb, odb=odb, dml=ProfileDML)


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    core = providers.Container(CoreContainer, config=config.test.core)  # substitute test for the mode (prod, dev) of interest

    data = providers.Container(DataLayerContainer, config=config.test.data)

    table = providers.Container(TableContainer, rdb=data.rdb)

    dao = providers.Container(DAOContainer, rdb=data.rdb, odb=data.odb)
