#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/container.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:22:48 am                                              #
# Modified   : Saturday January 21st 2023 04:54:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Data Access Layer Dependency Injection Container"""
from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.dal.sql.rdb import DatabaseDDL as RecsysDatabaseDDL
from recsys.core.dal.sql.edb import DatabaseDDL as EventsDatabaseDDL
from recsys.core.dal.sql.file import FileDDL, FileDML
from recsys.core.dal.sql.dataset import DatasetDDL, DatasetDML
from recsys.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from recsys.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from recsys.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from recsys.core.dal.sql.dag import DAGDDL, DAGDML
from recsys.core.dal.sql.task import TaskDDL, TaskDML
from recsys.core.dal.sql.event import EventDDL, EventDML
from recsys.core.dal.sql.profile import ProfileDDL, ProfileDML
from recsys.core.dal.sql.odb import ObjectODL, ObjectOML
from recsys.core.dal.dao import FileDAO, DatasetDAO, DataFrameDAO, DataSourceDAO, DataSourceURLDAO
from recsys.core.dal.dao import DAGDAO, TaskDAO, EventDAO, ProfileDAO
from recsys.core.dal.dba import DBA, ODBA
from recsys.core.dal.oao import OAO


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

    dag = providers.Factory(DBA, database=edb, ddl=DAGDDL)

    task = providers.Factory(DBA, database=edb, ddl=TaskDDL)

    profile = providers.Factory(DBA, database=edb, ddl=ProfileDDL)

    event = providers.Factory(DBA, database=edb, ddl=EventDDL)

    object = providers.Factory(ODBA, database=odb, ddl=ObjectODL)


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

    dag = providers.Factory(DAGDAO, dml=DAGDML, database=edb)

    task = providers.Factory(TaskDAO, dml=TaskDML, database=edb)

    profile = providers.Factory(ProfileDAO, dml=ProfileDML, database=edb)

    event = providers.Factory(EventDAO, dml=EventDML, database=edb)

    object = providers.Factory(OAO, oml=ObjectOML, database=odb)
