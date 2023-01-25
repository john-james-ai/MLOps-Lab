#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/container.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:42:45 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Workflow Dependency Injection Container Module"""

from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.repo.uow import UnitOfWork
from mlops_lab.core.workflow.callback import DAGCallback, TaskCallback


# ------------------------------------------------------------------------------------------------ #
class WorkContainer(containers.DeclarativeContainer):

    entities = providers.Dependency()

    unit = providers.Factory(UnitOfWork, entities=entities)


# ------------------------------------------------------------------------------------------------ #
class CallbackContainer(containers.DeclarativeContainer):

    events = providers.Dependency()

    dag = providers.Factory(DAGCallback, events=events)

    task = providers.Factory(TaskCallback, events=events)
