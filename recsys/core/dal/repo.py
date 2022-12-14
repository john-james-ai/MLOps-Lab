#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/repo.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 13th 2022 04:39:34 am                                              #
# Modified   : Tuesday December 13th 2022 02:43:41 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pandas as pd

from recsys.core.dal.base import DAO
from recsys.core.entity.base import Entity
from .base import Service
# ------------------------------------------------------------------------------------------------ #


class Repo(Service):
    """Entity Repository

        Args:
            entity (str): The entity for which the repository has been instantiated.
        dao (DAO): Database Access Object for the Entity
    """
    def __init__(self, entity: type(Entity), dao: DAO) -> None:
        self._entity = entity
        self._dao = dao

    def __len__(self) -> int:
        return len(self._dao())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = entity.as_dto()
        dto = self._dao.create(dto)
        return self._entity.from_dto(dto)

    def get(self, id: int) -> Entity:
        "Returns an entity with the designated id"
        dto = self._dao.read(id)
        return self._entity.from_dto(dto)

    def get_by_name(self, name: str) -> Entity:
        "Returns an entity with the designated id"
        dto = self._dao.read_by_name(name)
        return self._entity.from_dto(dto)

    def update(self, entity: Entity) -> None:
        """Updates an entity in the databases."""
        dto = entity.as_dto()
        self._dao.update(dto=dto)

    def remove(self, id: int) -> None:
        """Removes an entity with id from repository."""
        self._dao.delete(id)

    def exists(self, id: int) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        entities = self._dao.read_all()
        df = pd.DataFrame.from_dict(data=entities, orient='index')
        print(df)
