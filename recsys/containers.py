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
# Modified   : Sunday December 4th 2022 05:20:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config  # pragma: no cover
import sqlite3

from dependency_injector import containers, providers  # pragma: no cover


from recsys.core.dal.dao import DatasetDAO
from recsys.core.dal.ddo import TableService
from recsys.core.dal.sql import DatasetDDL
from recsys.core.data.database import SQLiteDatabase, SQLiteConnection

# ------------------------------------------------------------------------------------------------ #


class CoreContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )


class DataLayerContainer(containers.DeclarativeContainer):

    config = providers.Configuration()

    connection = providers.Singleton(
        SQLiteConnection,
        connector=sqlite3.connect,
        location=config.database.sqlite.location,
    )

    database = providers.Singleton(SQLiteDatabase, connection=connection)


class DataAccessLayerContainer(containers.DeclarativeContainer):

    database = providers.Dependency()

    dataset_table = providers.Factory(TableService, database=database, ddl=DatasetDDL)

    dataset_dao = providers.Singleton(DatasetDAO, database=database)


class Recsys(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["config.yml"])

    core = providers.Container(CoreContainer, config=config.core)

    data = providers.Container(DataLayerContainer, config=config.data)

    dal = providers.Container(DataAccessLayerContainer, database=data.database)
