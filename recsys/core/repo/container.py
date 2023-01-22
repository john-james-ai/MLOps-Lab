#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/container.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:34:43 am                                              #
# Modified   : Saturday January 21st 2023 05:02:41 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Repository Dependency Injection Container"""

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.repo.entity import Repo
from recsys.core.repo.dataset import DatasetRepo
from recsys.core.repo.datasource import DataSourceRepo
from recsys.core.repo.dag import DAGRepo
from recsys.core.repo.context import Context
from recsys.core.repo.uow import UnitOfWork


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
