#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/factory/container.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 21st 2023 05:40:12 am                                              #
# Modified   : Saturday January 21st 2023 04:58:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Object Factory Dependency Injection Container"""
from dependency_injector import containers, providers  # pragma: no cover

from recsys.core.factory.dataset import DatasetFactory, DataFrameFactory
from recsys.core.factory.datasource import DataSourceFactory, DataSourceURLFactory
from recsys.core.factory.dag import DAGFactory, TaskFactory
from recsys.core.factory.event import EventFactory


# ------------------------------------------------------------------------------------------------ #
class ObjectFactoryContainer(containers.DeclarativeContainer):

    dataset = providers.Factory(DatasetFactory)
    dataframe = providers.Factory(DataFrameFactory)
    datasource = providers.Factory(DataSourceFactory)
    datasource_url = providers.Factory(DataSourceURLFactory)
    dag = providers.Factory(DAGFactory)
    task = providers.Factory(TaskFactory)
    event = providers.Factory(EventFactory)
