#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/datasource.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Tuesday January 10th 2023 01:57:47 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataSource Repository"""

from recsys.core.entity.base import Entity
from recsys.core.entity.datasource import DataSource, DataSourceURL
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class DataSourceRepo(RepoABC):
    """DataSource aggregate repository. """

    def __init__(self, context: Context) -> None:
        super().__init__()
        self._context = context
        self._datasource_dao = self._context.get_dao(DataSource)
        self._datasource_url_dao = self._context.get_dao(DataSourceURL)
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self._datasource_dao.read_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""

        dto = self._datasource_dao.create(entity.as_dto())
        entity.id = dto.id

        for name, datasource_url in entity.urls.items():
            datasource_url.parent = entity
            dto = self._datasource_url_dao.create(dto=datasource_url.as_dto())
            datasource_url.id = dto.id
            entity.update_url(datasource_url)

        self._oao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        result = []
        dto = self._datasource_dao.read(id)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        result = []
        mode = mode or self._get_mode()
        dto = self._datasource_dao.read_by_name_mode(name, mode)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        for datasource_url in entity.urls.values():
            self._datasource_url_dao.update(datasource_url.as_dto())

        self._datasource_dao.update(dto=entity.as_dto())   # Update DataSource metadata
        self._oao.update(entity)  # Persist datasource in object storage

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._datasource_dao.read(id)
        datasource = self._oao.read(dto.oid)
        for datasource_url in datasource.urls.values():
            self._datasource_url_dao.delete(datasource_url.id)

        self._datasource_dao.delete(id)   # Delete DataSource metadata
        self._oao.delete(datasource.oid)  # Delete datasource from object storage

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._datasource_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataSourceURL."""
        dtos = self._datasource_dao.read_all()
        for dto in dtos.values():
            datasource = self._oao.read(dto.oid)
            print("\n\n")
            print(datasource)
            print(120 * "=")
            print(50 * " ", "DataSourceURLs")
            print(120 * "_")
            dfs = datasource.get_urls()
            print(dfs)
