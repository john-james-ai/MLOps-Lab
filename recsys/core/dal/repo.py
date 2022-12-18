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
# Modified   : Sunday December 18th 2022 01:23:37 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dataclasses import dataclass
import pandas as pd
import shelve

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
        self._db_name = os.path.join("data", self._entity.__name__ + ".db")

    def __len__(self) -> int:
        return len(self._dao.read_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        dto = entity.as_dto()
        dto = self._dao.create(dto)
        # Convert the dto with id, back to a Dataset, retrieve object from persistence.
        entity = self._entity.from_dto(dto)
        self._save_object(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._load_object(id)

    def get_by_name(self, name: str) -> Entity:
        dto = self._dao.read_by_name(name)
        entity = self._entity.from_dto(dto)
        return self._load_object(entity.id)

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        dto = entity.as_dto()
        self._dao.update(dto)
        self._save_object(entity)

    def remove(self, id: str) -> None:
        """Removes an entity with id from repository."""
        self._dao.delete(id)
        self._delete_object(id)

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def print(self, id: int = None) -> None:
        """Prints the repository contents as a DataFrame."""
        if id is not None:
            results = self._dao.read(id)
            print(results)
        else:
            results = self._dao.read_all()
            df = pd.DataFrame.from_dict(results, orient="index")
            print(df)

    def _save_object(self, entity: Entity) -> None:
        """Persists objects"""
        odb = shelve.open(self._db_name)
        odb[str(entity.id)] = entity
        odb.close()

    def _load_object(self, id) -> Entity:
        """Loads an object from persistence."""
        odb = shelve.open(self._db_name)
        try:
            result = odb[str(id)]
        except KeyError:
            msg = f"Object with id - {id} is not found."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        finally:
            odb.close()
        return result

    def _delete_object(self, id) -> None:
        """Deletes an object from storage."""
        odb = shelve.open(self._db_name)
        try:
            del odb[str(id)]
        except KeyError:
            msg = f"Object with id - {id} is not found."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        finally:
            odb.close()


# ------------------------------------------------------------------------------------------------ #
#                                        CONTEXT                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context:
    dataset: Repo
    task: Repo
    job: Repo
    profile: Repo
