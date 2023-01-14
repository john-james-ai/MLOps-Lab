#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/builder/datasource.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 02:54:39 pm                                              #
# Modified   : Saturday January 14th 2023 02:55:53 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""DataSourceBuilder"""
from .base import Builder
from recsys.core.entity.datasource import DataSource, DataSourceURL


# ------------------------------------------------------------------------------------------------ #
#                                      DATASOURCE BUILDER                                          #
# ------------------------------------------------------------------------------------------------ #
class DataSourceBuilder(Builder):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config) -> DataSource:
        if not self._instance:
            self._instance = self._build_datasource(config)
        return self._instance

    def _build_datasource(self, config) -> DataSource:
        config = config.get("datasource")
        datasource = DataSource(
            name=config.get("name"),
            description=config.get("description"),
            website=config.get("website"),
        )
        for url in config.get("urls"):
            datasource_url = DataSourceURL(
                name=url.get("name"),
                description=url.get("description"),
                url=url.get("url"),
                datasource=datasource,
            )
            datasource.add_url(datasource_url)
        return datasource
