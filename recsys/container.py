#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/container.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:20:39 am                                              #
# Modified   : Saturday January 21st 2023 05:50:07 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Application Dependency Injector Container"""
import dotenv

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.database.container import ConnectionContainer, DatabaseContainer
from recsys.core.service.container import ServiceContainer
from recsys.core.dal.container import DBAContainer, DALContainer
from recsys.core.repo.container import ContextContainer, EntityRepoContainer, EventRepoContainer
from recsys.core.workflow.container import WorkContainer
from recsys.core.factory.container import ObjectFactoryContainer


# ------------------------------------------------------------------------------------------------ #
class Recsys(containers.DeclarativeContainer):

    dotenv.load_dotenv()

    config = providers.Configuration(yaml_files=["config.yml"])
    config_datasources = providers.Configuration(yaml_files=["recsys.data.etl.datasources.yml"])

    core = providers.Container(ServiceContainer, config=config)

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

    dba = providers.Container(
        DBAContainer, dbms=database.dbms, rdb=database.rdb, edb=database.edb, odb=database.odb
    )

    dal = providers.Container(DALContainer, rdb=database.rdb, edb=database.edb, odb=database.odb)

    context = providers.Container(ContextContainer, dal=dal)

    entities = providers.Container(EntityRepoContainer, context=context)

    events = providers.Container(EventRepoContainer, context=context)

    work = providers.Container(WorkContainer, entities=entities)

    factory = providers.Container(ObjectFactoryContainer)
