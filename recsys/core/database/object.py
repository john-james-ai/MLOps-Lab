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
# Modified   : Monday January 9th 2023 06:01:38 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Object persistence module"""
import os
from abc import abstractmethod
import shelve
from typing import Union
from glob import glob

from .base import Connection, AbstractDatabase, Service
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                          CURSOR                                                  #
# ------------------------------------------------------------------------------------------------ #
class Cursor(Service):
    """Abstract base class for object database cursors.

    Args:
        location (str): The path of the shelve database file.
    """
    def __init__(self, location) -> None:
        super().__init__()
        self._location = location
        self._cursor = None
        self.open()

    def open(self) -> None:
        os.makedirs(os.path.dirname(self._location), exist_ok=True)
        self._cursor = shelve.open(self._location)
        msg = f"Object storage opened at {self._location}"
        self._logger.debug(msg)

    def close(self) -> None:
        self._cursor.close()
        msg = f"Object storage at {self._location} is closed."
        self._logger.debug(msg)

    def drop(self) -> None:
        """Delete the cursor, i.e. the shelve database."""
        pattern = self._location + ".*"
        self._remove(pattern)
        self._is_open = False
        msg = f"Pattern: {pattern}."
        self._logger.debug(msg)
        msg = f"Dropped object store at {self._location}."
        self._logger.info(msg)

    def select(self, oid: str) -> Union[Entity, None]:
        """Select an existing entity by oid from object storage"""
        self.open()
        try:
            result = self._cursor[oid]
        except KeyError:
            result = []
        self.close()
        return result

    def insert(self, entity: Entity) -> None:
        """Inserts an entity into the underlying object data store."""
        self.open()
        if entity.oid not in self._cursor.keys():
            self._cursor[entity.oid] = entity
            msg = f"Inserted entity oid: {entity.oid}."
            self._logger.info(msg)
        else:
            msg = f"Unable to insert entity oid: {entity.oid}. Entity already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)
        self.close()

    def update(self, entity: Entity) -> None:
        """Update an existing entity in object storage or cache."""
        self.open()
        if entity.oid in self._cursor.keys():
            self._cursor[entity.oid] = entity
            msg = f"Updated entity oid: {entity.oid}."
            self._logger.info(msg)
        else:
            msg = f"Unable to update entity oid: {entity.oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        self.close()

    @abstractmethod
    def delete(self, oid: str) -> None:
        """Deletes a key/value pair from object storage"""

    def exists(self, oid: str) -> None:
        """Checks existence of an object in the storage"""
        self.open()
        exists = oid in self._cursor.keys()
        answer = "exists" if exists else "does not exist."
        msg = f"Checked existence of {oid}. Entity {answer}."
        self._logger.debug(msg)
        self.close()
        return exists

    def _remove(self, pattern) -> None:
        """Removes files that match the glob pattern."""
        file_list = glob(pattern, recursive=True)
        for filepath in file_list:
            try:
                os.remove(filepath)
                msg = f"Removed {filepath}."
                self._logger.debug(msg)
            except OSError:  # pragma: no cover
                msg = f"Encountered an error while deleting file at {filepath}"
                self._logger.error(msg)
                raise OSError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                          CACHE CURSOR                                            #
# ------------------------------------------------------------------------------------------------ #
class CacheCursor(Cursor):
    """Class for object cache.

    Args:
        location (str): The path to the database file.

    """

    def __init__(self, location) -> None:
        super().__init__(location=location)
        self._location = self._set_cache_location()

    @property
    def cache(self) -> dict:
        self.open()
        cache = {}
        for oid, entity in self._cursor.items():
            cache[oid] = entity
        return cache

    def reset(self) -> None:
        self.open()
        for oid in self._cursor.keys():
            del self._cursor[oid]
        self.close()
        msg = "Cache is reset."
        self._logger.debug(msg)

    def delete(self, oid: str) -> None:
        """Deletes a key/value pair from object storage"""
        self.open()

        self._cursor[oid] = None
        msg = f"Marked entity oid = {oid} for deletion."
        self._logger.info(msg)

        self.close()

    def exists(self, oid: str) -> None:
        """Checks existence of an object in the storage"""
        self.open()
        if oid in self._cursor.keys():
            exists = not self._cursor[oid] == []
        else:
            exists = False
        answer = "exists" if exists else "does not exist."
        msg = f"Checked existence of {oid}. Entity {answer}."
        self._logger.debug(msg)
        self.close()
        return exists

    def _set_cache_location(self) -> str:
        return os.path.join(os.path.dirname(self._location), "cache", os.path.basename(self._location))


# ------------------------------------------------------------------------------------------------ #
#                                         STORAGE CURSOR                                           #
# ------------------------------------------------------------------------------------------------ #
class StorageCursor(Cursor):
    """Class for object storage cursors.

    Args:
        location (str): The path to the database file.

    """

    def __init__(self, location) -> None:
        super().__init__(location=location)

    def save(self, cache_cursor: CacheCursor) -> None:
        """Commits cache to object storage."""
        self.open()
        cache = cache_cursor.cache
        for oid, entity in cache.items():
            if entity is not None:
                self._cursor[oid] = entity
                msg = f"Saved entity {entity.oid} to object storage."
                self._logger.debug(msg)
            else:
                del self._cursor[oid]
        cache_cursor.reset()
        cache_cursor.close()
        self.close()

    def delete(self, oid) -> None:
        """Deletes a key/value pair from object storage"""
        self.open()
        try:
            del self._cursor[oid]
            msg = f"Deleted object with oid = {oid} from object storage."
            self._logger.info(msg)

        except KeyError:
            msg = f"Unable to delete entity oid: {oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        self.close()


# ------------------------------------------------------------------------------------------------ #
#                             OBJECT DATABASE (PSEUDO) CONNECTION                                  #
# ------------------------------------------------------------------------------------------------ #
class ObjectDBConnection(Connection):
    """Creates an object datastore at the designated directory location.

    Args:
        location (str): The path and filename for the data store. The base of the path is
            the database name by convention.
    """

    __cache_filename = "cache.odb"

    def __init__(self, location: str, autocommit: bool = True) -> None:
        super().__init__()
        self._location = location
        self._autocommit = autocommit
        self._cache = None
        self._storage = None
        self._build_cursors()

    @property
    def location(self) -> str:
        return self._location

    @property
    def storage(self) -> Cursor:
        return self._storage

    @property
    def cache(self) -> Cursor:
        return self._cache

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    def begin(self) -> None:
        """Starts a transaction."""
        if not self._is_open:
            self.open()

    def open(self) -> None:
        """Opens a database connection."""
        self._storage.open()
        self._cache.open()
        self._logger.debug("connection is open.")

    def close(self) -> None:
        """Closes the current connection."""
        self._cache.reset()
        self._cache.close()
        self._storage.close()
        self._logger.debug("is closed.")

    def commit(self) -> None:
        """Commits data to the underlying database."""
        self._storage.save(self._cache)
        self._cache.reset()

    def rollback(self) -> None:  # pragma: no cover
        """Rolls back changes made to the database since last commit."""
        self._cache.reset()

    def drop(self) -> None:
        self._cache.drop()
        self._storage.drop()

    def _build_cursors(self) -> None:
        self._cache = CacheCursor(self._location)
        self._storage = StorageCursor(self._location)


# ------------------------------------------------------------------------------------------------ #
#                                     OBJECT DATABASE                                              #
# ------------------------------------------------------------------------------------------------ #
class ObjectDB(AbstractDatabase):
    """Manages object persistence."""

    def __init__(self, connection: type(Connection)) -> None:
        super().__init__()
        self._connection = connection
        self._autocommit = connection.autocommit
        self._in_transaction = False
        self._is_open = False

    @property
    def is_open(self) -> bool:
        return self._is_open

    def connect(self) -> None:
        """Connects to the database."""
        self._connection.open()
        self._is_open = True

    def begin(self) -> None:
        """Starts a transaction on the underlying database connection."""
        self._connection.begin()
        self._in_transaction = True
        msg = "Transaction has started"
        self._logger.info(msg)

    def close(self) -> None:
        """Closes the underlying database connection."""
        self._connection.close()
        self._is_open = False
        self._in_transaction = False

    def save(self) -> None:
        """Saves changes to the database."""
        self._connection.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Rolls back the database to state as of last save or commit."""
        self._connection.rollback()
        self._in_transaction = False

    def select(self, oid: str) -> Entity:
        if self._in_transaction:
            result = self._connection.cache.select(oid)
            if result == []:
                result = self._connection.storage.select(oid)
        else:
            result = self._connection.storage.select(oid)
        return result

    def insert(self, entity: Entity) -> int:
        """Inserts an object into object storage."""
        if self._in_transaction:
            if not self._connection.storage.exists(entity.oid) and not self._connection.cache.exists(entity.oid):
                self._connection.cache.insert(entity)
            else:
                msg = f"Unable to insert entity oid = {entity.oid}. Entity already exists."
                self._logger.error(msg)
                raise FileExistsError(msg)
        else:
            self._connection.storage.insert(entity)

    def update(self, entity) -> None:
        """Performs an update on existing data in the database."""
        if self._in_transaction:
            if not self._connection.cache.exists(entity.oid):
                self._cache(entity.oid)
            self._connection.cache.update(entity)
        else:
            self._connection.storage.update(entity)

    def delete(self, oid: str) -> None:
        """Deletes existing data."""
        if self._in_transaction:
            if not self._connection.cache.exists(oid):
                self._cache(oid)
            self._connection.cache.delete(oid)
        else:
            self._connection.storage.delete(oid)

    def drop(self) -> None:
        """Drop database."""
        self._connection.drop()

    def exists(self, oid: str) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""
        if self._in_transaction:
            return self._connection.cache.exists(oid)
        else:
            return self._connection.storage.exists(oid)

    def database_exists(self) -> bool:
        pattern = self._connection.location + ".*"
        return len(glob(pattern)) > 0

    def _cache(self, oid: str) -> None:
        """Copies an entity from storage to cache"""
        entity = self._connection.storage.select(oid)
        self._connection.cache.insert(entity)
        msg = f"Copied entity {oid} from storage to cache."
        self._logger.debug(msg)
