#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/factory/container.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:40:12 am                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Object Factory Dependency Injection Container"""
from dependency_injector import containers, providers  # pragma: no cover

from mlops_lab.core.factory.dataset import DatasetFactory, DataFrameFactory
from mlops_lab.core.factory.datasource import DataSourceFactory, DataSourceURLFactory
from mlops_lab.core.factory.dag import DAGFactory, TaskFactory
from mlops_lab.core.factory.event import EventFactory


# ------------------------------------------------------------------------------------------------ #
class ObjectFactoryContainer(containers.DeclarativeContainer):

    dataset = providers.Factory(DatasetFactory)
    dataframe = providers.Factory(DataFrameFactory)
    datasource = providers.Factory(DataSourceFactory)
    datasource_url = providers.Factory(DataSourceURLFactory)
    dag = providers.Factory(DAGFactory)
    task = providers.Factory(TaskFactory)
    event = providers.Factory(EventFactory)
