#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/object.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 24th 2022 07:01:02 am                                             #
# Modified   : Saturday December 24th 2022 04:52:41 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Object persistence module"""
from .connection import Connection
from recsys.core.services.base import Service
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                     OBJECT DATABASE                                              #
# ------------------------------------------------------------------------------------------------ #
class ODB(Service):
    """Manages object persistence."""
    def __init__(self, connection: Connection) -> None:
        super().__init__()
        self._connection = connection

    def __enter__(self):
        if not self._connection.is_connected():
            self._connection.connect()
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception):
            self._connection.rollback()
        else:
            self._connection.commit()
        self._connection.close()

    def __del__(self):  # pragma: no cover
        if self._connection.is_connected():
            self._connection.close()

    def __len__(self) -> int:
        if not self._connection.is_connected():
            self._connection.connect()
        cursor = self._connection.cursor()
        return len(cursor.keys())

    # -------------------------------------------------------------------------------------------- #
    def reset(self) -> None:
        self._connection.reset()

    # -------------------------------------------------------------------------------------------- #
    def create(self, entity: Entity) -> None:
        """Persists a new entity into the object database.

        Args:
            entity (Entity): The entity to persist
        """
        if not self.exists(entity.oid):
            cache = self._connection.cache
            cache[entity.oid] = entity
            self._connection.cache = cache
        else:
            msg = f"Object {entity.oid} already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)

    # -------------------------------------------------------------------------------------------- #
    def read(self, oid: str) -> Entity:
        """Reads an Entity from the Database

        Args:
            oid (str): Object Id comprised of <classname>_<id> in lower case.
        """
        try:
            cursor = self._connection.cursor()
            return cursor[oid]
        except KeyError:
            msg = f"Object with object_id = {oid} does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def update(self, entity: Entity) -> None:
        """Updates an existing entity.

        Args:
            entity (Entity): The entity to update
        """
        if self.exists(entity.oid):
            cache = self._connection.cache
            cache[entity.oid] = entity
            self._connection.cache = cache
        else:
            msg = f"Object with object_id = {entity.oid} does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, oid: str) -> None:
        """Deletes an object from the database.

        Args:
            oid (str): Object Id = <classname>_<id> lowercase.
        """
        if self.exists(oid):
            cache = self._connection.cache
            cache[oid] = None
            self._connection.cache = cache
        else:
            msg = f"Object with object_id = {oid} does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def exists(self, oid: str) -> bool:
        cursor = self._connection.cursor()
        return oid in cursor.keys()

    # -------------------------------------------------------------------------------------------- #
    def save(self) -> None:
        self._connection.commit()
