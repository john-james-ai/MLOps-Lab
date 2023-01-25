#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/factory/datasource.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 02:54:39 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""DataSourceFactory"""
from mlops_lab.core.factory.base import Factory
from mlops_lab.core.entity.datasource import DataSource, DataSourceURL


# ------------------------------------------------------------------------------------------------ #
#                                      DATASOURCE FACTORY                                          #
# ------------------------------------------------------------------------------------------------ #
class DataSourceFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> DataSource:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> DataSource:
        datasource = DataSource(**config)
        return datasource


# ------------------------------------------------------------------------------------------------ #
#                                 DATASOURCE URL FACTORY                                           #
# ------------------------------------------------------------------------------------------------ #
class DataSourceURLFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> DataSourceURL:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> DataSourceURL:
        for url in config.get("urls"):
            datasource_url = DataSourceURL(**url)
        return datasource_url
