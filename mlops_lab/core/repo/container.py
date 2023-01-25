#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/repo/container.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:34:43 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:45 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Repository Dependency Injection Container"""

from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.repo.entity import Repo
from mlops_lab.core.repo.dataset import DatasetRepo
from mlops_lab.core.repo.datasource import DataSourceRepo
from mlops_lab.core.repo.dag import DAGRepo
from mlops_lab.core.repo.context import Context
from mlops_lab.core.repo.uow import UnitOfWork


# ------------------------------------------------------------------------------------------------ #
class ContextContainer(containers.DeclarativeContainer):

    dal = providers.Dependency()

    context = providers.Factory(Context, dal=dal)


# ------------------------------------------------------------------------------------------------ #
class EntityRepoContainer(containers.DeclarativeContainer):

    context = providers.Dependency()

    file = providers.Factory(Repo, context=context, entity="file")

    datasource = providers.Factory(DataSourceRepo, context=context)

    dataset = providers.Factory(DatasetRepo, context=context)


# ------------------------------------------------------------------------------------------------ #
class EventRepoContainer(containers.DeclarativeContainer):

    context = providers.Dependency()

    profile = providers.Factory(Repo, context=context, entity="profile")

    event = providers.Factory(Repo, context=context, entity="event")

    dag = providers.Factory(DAGRepo, context=context)


# ------------------------------------------------------------------------------------------------ #
class WorkContainer(containers.DeclarativeContainer):

    entities = providers.Dependency()

    unit = providers.Factory(UnitOfWork, entities=entities)
