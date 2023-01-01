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
# Modified   : Sunday January 1st 2023 03:08:29 pm                                                 #
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

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        for name, ds_url in entity.urls.items():
            datasource_url = self._datasource_url_dao.create(entity=ds_url, persist=False)
            self._datasource_url_dao.save()
            entity.update_url(datasource_url)

        entity = self._datasource_dao.create(entity=entity, persist=True)
        self._datasource_dao.save()
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._datasource_dao.read(id)

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        mode = mode or self._get_mode()
        return self._datasource_dao.read_by_name_mode(name, mode)

    def get_all(self) -> dict:
        return self._datasource_dao.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._datasource_dao.update(entity=entity, persist=True)
        self._datasource_dao.save()
        for _, datasource_url in entity.urls.items():
            self._datasource_url_dao.update(datasource_url, persist=False)
        self._datasource_url_dao.save()

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        entity = self.get(id)
        for _, datasource_url in entity.urls.items():
            self._datasource_url_dao.delete(datasource_url.id, persist=False)
        self._datasource_url_dao.save()
        self._datasource_dao.delete(id)
        self._datasource_dao.save()

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._datasource_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        entities = self.get_all()
        for name, entity in entities.items():
            print("\n\n")
            print(entity)
            print(120 * "=")
            print(50 * " ", "DataSource URLs")
            print(120 * "_")
            datasource_urls = self._datasource_url_dao.read_by_parent_id(parent=entity)
            print(datasource_urls)
