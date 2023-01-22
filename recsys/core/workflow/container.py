#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/container.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:42:45 am                                              #
# Modified   : Saturday January 21st 2023 09:14:34 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Workflow Dependency Injection Container Module"""

from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.repo.uow import UnitOfWork
from recsys.core.workflow.callback import DAGCallback, TaskCallback


# ------------------------------------------------------------------------------------------------ #
class WorkContainer(containers.DeclarativeContainer):

    entities = providers.Dependency()

    unit = providers.Factory(UnitOfWork, entities=entities)


# ------------------------------------------------------------------------------------------------ #
class CallbackContainer(containers.DeclarativeContainer):

    events = providers.Dependency()

    dag = providers.Factory(DAGCallback, events=events)

    task = providers.Factory(TaskCallback, events=events)
