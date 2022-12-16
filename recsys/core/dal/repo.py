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
# Created    : Friday December 16th 2022 12:28:07 am                                               #
# Modified   : Friday December 16th 2022 06:33:25 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pandas as pd

from recsys.core.dal.dao import DAO
from recsys.core.entity.base import Entity
from recsys.core.services.base import Service


# ------------------------------------------------------------------------------------------------ #
#                                       REPOSITORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class Repo(Service):
    """Repository base class"""

    def __init__(self, entity: type(Entity), dao: DAO) -> None:
        super().__init__()
        self._entity = entity
        self._dao = dao

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = entity.as_dto()
        dto = self._dao.create(dto)
        # Convert the dto with id, back to a Dataset and return.
        return self._entity.from_dto(dto)

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        dto = self._dao.get(id)
        return self._entity.from_dto(dto)

    def get_by_name(self, name: str) -> Entity:
        dto = self._dao.get_by_name(name)
        return self._entity.from_dto(dto)

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        dto = entity.as_dto()
        self._dao.update(dto)

    def remove(self, id: str) -> None:
        """Removes an entity with id from repository."""
        self._dao.delete(id)

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        self._dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        results = self._dao.read_all()
        df = pd.DataFrame.from_dict(results, orient="index")
        print(df)
