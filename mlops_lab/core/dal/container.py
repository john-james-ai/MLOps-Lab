#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/container.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:22:48 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Data Access Layer Dependency Injection Container"""
from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.dal.sql.rdb import DatabaseDDL as mlops_labDatabaseDDL
from mlops_lab.core.dal.sql.edb import DatabaseDDL as EventsDatabaseDDL
from mlops_lab.core.dal.sql.file import FileDDL, FileDML
from mlops_lab.core.dal.sql.dataset import DatasetDDL, DatasetDML
from mlops_lab.core.dal.sql.dataframe import DataFrameDDL, DataFrameDML
from mlops_lab.core.dal.sql.datasource import DataSourceDDL, DataSourceDML
from mlops_lab.core.dal.sql.datasource_url import DataSourceURLDDL, DataSourceURLDML
from mlops_lab.core.dal.sql.dag import DAGDDL, DAGDML
from mlops_lab.core.dal.sql.task import TaskDDL, TaskDML
from mlops_lab.core.dal.sql.event import EventDDL, EventDML
from mlops_lab.core.dal.sql.profile import ProfileDDL, ProfileDML
from mlops_lab.core.dal.sql.odb import ObjectODL, ObjectOML
from mlops_lab.core.dal.dao import (
    FileDAO,
    DatasetDAO,
    DataFrameDAO,
    DataSourceDAO,
    DataSourceURLDAO,
)
from mlops_lab.core.dal.dao import DAGDAO, TaskDAO, EventDAO, ProfileDAO
from mlops_lab.core.dal.dba import DBA, ODBA
from mlops_lab.core.dal.oao import OAO


# ------------------------------------------------------------------------------------------------ #
class DBAContainer(containers.DeclarativeContainer):

    dbms = providers.Dependency()
    rdb = providers.Dependency()
    edb = providers.Dependency()
    odb = providers.Dependency()

    mlops_lab_database = providers.Factory(DBA, database=dbms, ddl=mlops_labDatabaseDDL)

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
