#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/database/container.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:17:50 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Database Container Module"""
import pymysql

from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.database.relational import Database, MySQLConnection, DatabaseConnection
from mlops_lab.core.database.object import ObjectDBConnection, ObjectDB


# ------------------------------------------------------------------------------------------------ #
class ConnectionContainer(containers.DeclarativeContainer):

    mlops_lab_database = providers.Configuration()
    events_database = providers.Configuration()

    dbms_connection = providers.Factory(
        MySQLConnection, connector=pymysql.connect, autocommit=False, autoclose=False
    )

    rdb_connection = providers.Factory(
        DatabaseConnection,
        connector=pymysql.connect,
        database=mlops_lab_database,
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
