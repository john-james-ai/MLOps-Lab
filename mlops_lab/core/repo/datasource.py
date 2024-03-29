#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/repo/datasource.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Tuesday January 24th 2023 08:13:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataSource Repository"""

from mlops_lab.core.entity.base import Entity
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class DataSourceRepo(RepoABC):
    """DataSource aggregate repository."""

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__()
        self._context = context
        self._datasource_dao = self._context.get_dao("datasource")
        self._datasource_url_dao = self._context.get_dao("datasourceurl")
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self._datasource_dao.read_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""

        dto = self._datasource_dao.create(entity.as_dto())
        entity.id = dto.id

        for datasource_url in entity.urls.values():
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

    def get_by_name(self, name: str) -> Entity:
        result = []
        dto = self._datasource_dao.read_by_name(name)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_all(self) -> dict:
        entities = {}
        dtos = self._datasource_dao.read_all()
        for dto in dtos.values():
            entity = self._oao.read(oid=dto.oid)
            if hasattr(entity, "id"):
                entities[entity.id] = entity
        return entities

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        for datasource_url in entity.urls.values():
            self._datasource_url_dao.update(datasource_url.as_dto())

        self._datasource_dao.update(dto=entity.as_dto())  # Update DataSource metadata
        self._oao.update(entity)  # Persist datasource in object storage

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._datasource_dao.read(id)
        datasource = self._oao.read(dto.oid)
        for datasource_url in datasource.urls.values():
            self._datasource_url_dao.delete(datasource_url.id)

        self._datasource_dao.delete(id)  # Delete DataSource metadata
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
