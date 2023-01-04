#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/entity.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Wednesday January 4th 2023 01:23:12 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Entity Repository. Serves as generic repository supporting basic CRUD functionality."""
import pandas as pd

from recsys.core.entity.base import Entity
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class Repo(RepoABC):
    """Repository base class"""

    def __init__(self, context: Context, entity: type(Entity)) -> None:
        super().__init__()
        self._context = context
        self._entity = entity
        self._dao = self._context.get_dao(self._entity)

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        entity = self._dao.create(entity=entity, persist=True)
        self._dao.save()
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._dao.read(id)

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        mode = mode or self._get_mode()
        return self._dao.read_by_name_mode(name, mode)

    def get_all(self) -> dict:
        return self._dao.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._dao.update(entity=entity, persist=True)
        self._dao.save()

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        self._dao.delete(id)
        self._dao.save()

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        df = pd.DataFrame()
        entities = self.get_all()
        for entity in entities.values():
            data = entity.as_dto().as_dict()
            entity_df = pd.DataFrame(data=data, index=[entity.id])
            df = pd.concat([df, entity_df], axis=0)
        print(120 * "=")
        print(40 * " ", f"\t\t{self._entity.__name__} Repository")
        print(120 * "_")
        print(df)
