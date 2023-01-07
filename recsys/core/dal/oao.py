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
# Modified   : Saturday January 7th 2023 12:45:03 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Object Access Object Module."""
import logging
from collections import OrderedDict
from typing import Dict, List

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
        ocl = self._oml.insert(entity)
        self._database.insert(ocl)
        msg = f"{self.__class__.__name__} inserted {entity.__class__.__name__}.{entity.oid} - {entity.name} into the database."
        self._logger.debug(msg)
        return entity

    def read(self, oid: int) -> Entity:
        """Obtains an entity Entity with the designated id.

        Args:
            id (int): The id for the entity.

        Returns a Entity
        """
        result = None
        ocl = self._oml.select(oid)
        result = self._database.select(ocl)
        return result

    def read_by_name_mode(self, name: str, mode: str) -> Entity:
        """Obtains an entity Entity with the designated name and mode.
        Args:
            name (str): The name assigned to the entity.
            mode (str): Mode, i.e. 'dev', 'prod', or 'test'

        Returns a Data Transfer Object (Entity)
        """
        result = None
        ocl = self._oml.select_by_name_mode(name, mode)
        result = self._database.select(ocl)
        return result

    def read_all(self) -> Dict[str, Entity]:
        """Returns a dictionary of all entity data transfere objects of the in the database."""
        result = {}
        ocl = self._oml.select_all()
        entities = self._database.select_all(ocl)
        if entities is not None:
            result = self._rows_to_dict(entities)
        return result

    def update(self, entity: Entity) -> int:
        """Updates an existing entity.

        Args:
            entity (Entity): Entity

        Returns number of rows effected.
        """
        rows_affected = 0
        if self.exists(entity.oid):
            ocl = self._oml.update(entity)
            rows_affected = self._database.update(ocl)
            msg = f"{self.__class__.__name__} updated {entity.__class__.__name__}.{entity.oid} - {entity.name} in the database."
            self._logger.debug(msg)
        else:
            msg = f"{self.__class__.__name__} was unable to update {self._entity.__name__}.{entity.oid}. Not found in the database. Try insert instead."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        return rows_affected

    def exists(self, oid: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """
        ocl = self._oml.exists(oid)
        result = self._database.exists(ocl)
        return result

    def delete(self, oid: int, persist=True) -> None:
        """Deletes a Entity from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.

        """
        if self.exists(oid):
            entity = self.read(oid)
            ocl = self._oml.delete(oid)
            self._database.delete(ocl)
            if entity is not None:
                msg = f"{self.__class__.__name__} deleted {entity.__class__.__name__}.{entity.oid} - {entity.name} from the database."
                self._logger.debug(msg)
            else:
                msg = f"{self.__class__.__name__} was unable to delete entity oid: {oid}. Not found in the database."
                self._logger.debug(msg)
                raise FileNotFoundError(msg)
        else:
            msg = f"{self.__class__.__name__} was unable to delete entity oid: {oid}. Not found in the database."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def _rows_to_dict(self, entities: List) -> Dict:
        """Converts the results from a list to a dictionary of Entities."""
        results_dict = OrderedDict()
        for entity in entities:
            results_dict[entity.oid] = entity
        return results_dict
