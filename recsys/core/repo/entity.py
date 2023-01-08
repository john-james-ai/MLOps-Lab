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
# Modified   : Sunday January 8th 2023 10:29:55 am                                                 #
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
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = self._dao.create(dto=entity.as_dto())
        entity.id = dto.id
        self._oao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        dto = self._dao.read(id)
        return self._oao.read(dto.oid)

    def get_all(self) -> dict:
        entities = {}
        dtos = self._dao.read_all()
        for dto in dtos.values():
            entity = self._oao.read(oid=dto.oid)
            entities[entity.id] = entity
        return entities

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        mode = mode or self._get_mode()
        dto = self._dao.read_by_name_mode(name, mode)
        return self._oao.read(dto.oid)

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._dao.update(dto=entity.as_dto())
        self._aoa.update(entity)

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._dao.read(id)
        self._dao.delete(id)
        self._aoa.delete(dto.oid)

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
