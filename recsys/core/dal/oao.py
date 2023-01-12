#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/oao.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 6th 2023 11:49:31 pm                                                 #
# Modified   : Wednesday January 11th 2023 07:01:42 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Object Access Object Module."""
import logging

from recsys.core.dal.sql.odb import OML
from recsys.core.database.object import ObjectDB
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                     OBJECT ACCESS OBJECT                                         #
# ------------------------------------------------------------------------------------------------ #
class OAO:
    """Provides access to an underlying Object Database.

    Args:
        database(ObjectDB): An object database
        oml (OML): An instance of the Object Manipulation Language class.
    """

    def __init__(self, oml: OML, database: ObjectDB) -> None:
        self._oml = oml
        self._database = database
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __len__(self) -> int:
        """Returns the number of objects in the underlying object store."""
        raise NotImplementedError()

    def begin(self) -> None:
        """Starts a transaction on the underlying database."""
        self._database.begin()

    def save(self) -> None:
        """Commits changes to the database."""
        self._database.save()

    def close(self) -> None:
        """Commits changes to the database."""
        self._database.close()

    def rollback(self) -> None:
        """Rollback changes to the database."""
        self._database.rollback()

    def create(self, entity: Entity) -> Entity:
        """Adds an entity to the objct database.

        Args:
            entity (Entity): An entity.

        Returns: the Entity
        """
        self._database.insert(entity)
        return entity

    def read(self, oid: int) -> Entity:
        """Obtains an entity Entity with the designated id.

        Args:
            id (int): The id for the entity.

        Returns a Entity
        """
        return self._database.select(oid)

    def read_by_name(self, name: str) -> Entity:
        """Obtains an entity Entity with the designated name.
        Args:
            name (str): The name assigned to the entity.

        Returns a Data Transfer Object (Entity)
        """
        result = []
        ocl = self._oml.select_by_name(name)
        result = self._database.select(ocl.oid)
        return result

    def update(self, entity: Entity) -> None:
        """Updates an existing entity.

        Args:
            entity (Entity): Entity

        """
        self._database.update(entity)

    def exists(self, oid: str) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        return self._database.exists(oid)

    def delete(self, oid: str) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.

        """
        self._database.delete(oid)
