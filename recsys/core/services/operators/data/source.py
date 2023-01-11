#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/operators/data/source.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:02:44 am                                               #
# Modified   : Wednesday January 11th 2023 03:05:01 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Download Operator Module"""
from types import SimpleNamespace

from ..base import Operator
from recsys.core.entity.datasource import DataSource, DataSourceURL
from atelier.operator.download import Downloader
from recsys.core.repo.uow import UnitOfWork


# ================================================================================================ #
#                             DATASOURCE DOWNLOAD SERVICE                                          #
# ================================================================================================ #
class DataSourceDownloader(Operator):
    """Downloads and if compressed, extracts data from a DataSource object.

    Args:
        datasource (DataSource): A data source object.
        destination (str): The folder into which the data is to be extracted.

    """

    def __init__(self, datasource: DataSource, destination: str) -> None:
        super().__init__()
        self._datasource = datasource
        self._destination = destination

    def execute(self, uow: UnitOfWork, * args, **kwargs) -> None:
        """Downloads the DataSource and persists them to the database."""
        for datasource_file in self._datasource.urls.values():
            Downloader(url=datasource_file.url, destination=self._destination).execute()

        repo = uow.get_repo('datasource')
        datasource = repo.get(self._datasource.oid)
        datasource.state = 'DOWNLOADED'


# ================================================================================================ #
#                             DATASOURCE BUILDER SERVICE                                           #
# ================================================================================================ #
class DataSourceBuilder(Operator):
    """Creates a DataSource object from configuration.

    Args:
        config (dict): Configuration for the DataSource object.
    """

    def __init__(self, config: dict) -> None:
        super().__init__()
        self._config = SimpleNamespace(**config)

    def execute(self, uow: UnitOfWork, * args, **kwargs) -> None:
        """Builds a DataSource object and commits it to the database

        Args:
            uow (UnitOfWork): Unit of Work class containing common database for all repos.
        """
        datasource = self._build_datasource()
        for url in self._config.urls():
            datasource_url = self._build_datasource_url(datasource, url)
            datasource.add_url(datasource_url)
        datasource.state = 'CREATED'
        repo = uow.get_repo(DataSource)
        repo.add(datasource)
        return datasource

    def _build_datasource(self) -> DataSource:
        return DataSource(name=self._config.name, website=self._config.website, description=self._config.description)

    def _build_datasource_url(self, datasource: DataSource, url: dict) -> DataSourceURL:
        url = SimpleNamespace(**url)
        return DataSourceURL(name=url.name, url=url.url, datasource=datasource, description=url.description)
