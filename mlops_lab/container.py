#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/container.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:20:39 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Application Dependency Injector Container"""
import dotenv

from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.database.container import ConnectionContainer, DatabaseContainer
from mlops_lab.core.service.container import ServiceContainer
from mlops_lab.core.dal.container import DBAContainer, DALContainer
from mlops_lab.core.repo.container import ContextContainer, EntityRepoContainer, EventRepoContainer
from mlops_lab.core.workflow.container import WorkContainer
from mlops_lab.core.factory.container import ObjectFactoryContainer


# ------------------------------------------------------------------------------------------------ #
class mlops_lab(containers.DeclarativeContainer):

    dotenv.load_dotenv()

    config = providers.Configuration(yaml_files=["config.yml"])
    config_datasources = providers.Configuration(yaml_files=["mlops_lab.data.etl.datasources.yml"])

    core = providers.Container(ServiceContainer, config=config)

    connection = providers.Container(
        ConnectionContainer,
        mlops_lab_database=config.databases.mlops_lab,
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
