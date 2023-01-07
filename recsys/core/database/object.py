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
# Modified   : Saturday January 7th 2023 11:29:33 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Object persistence module"""
import os
import shelve
from typing import Union
from glob import glob

from .base import Connection, AbstractDatabase, Cursor
from recsys.core.entity.base import Entity
from recsys.core.dal.sql.base import OCL, OQL, OML


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

    def execute(self, ocl: OCL) -> Union[Entity, None]:

        commands = {"insert": self._insert, "select": self._select,
                    "select_by_name_mode": self._select_by_name_mode,
                    "update": self._update, "delete": self._delete,
                    "exists": self._exists}

        return commands[ocl.cmd](ocl)

    def _select(self, ocl: OQL) -> Union[Entity, None]:
        """Select an existing entity by oid from object storage"""
        try:
            result = self._cursor[ocl.oid]
        except KeyError:
            result = None
        return result

    def _select_by_name_mode(self, ocl: OQL) -> Union[Entity, None]:
        """Select an existing entity by name and mode from object storage."""
        try:
            result = self._cursor[ocl.oid]
        except KeyError:
            result = None
        return result

    def _insert(self, ocl: OML) -> None:
        """Inserts an entity into the underlying object data store."""
        if ocl.oid not in self._cursor.keys():
            self._cursor[ocl.entity.oid] = ocl.entity
            msg = f"{self.__class__.__name__} inserted entity oid: {ocl.oid}."
            self._logger.info(msg)
        else:
            msg = f"{self.__class__.__name__} unable to insert entity oid: {ocl.oid}. Entity already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)

    def _update(self, ocl: OML) -> None:
        """Update an existing entity in object storage or cache."""
        if ocl.oid in self._cursor.keys():
            self._cursor[ocl.oid] = ocl.entity
            msg = f"{self.__class__.__name__} updated entity oid: {ocl.oid}."
            self._logger.info(msg)
        else:
            msg = f"{self.__class__.__name__} unable to update entity oid: {ocl.oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def _delete(self, ocl: OML) -> None:
        """Deletes a key/value pair from object storage"""
        try:
            del self._cursor[ocl.oid]
            msg = f"Deleted object with oid = {ocl.oid} from object storage."
            self._logger.info(msg)

        except KeyError:
            msg = f"{self.__class__.__name__} unable to delete entity oid: {ocl.oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    @classmethod
    def _exists(self, ocl: OML) -> None:
        """Checks existence of an object in the storage"""
        return ocl.oid in self._cursor.keys()

    @classmethod
    def save(self, cache: dict) -> None:
        """Commits cache to object storage."""
        for oid, entity in cache.items():
            if entity is not None:
                self._cursor[oid] = entity
            else:
                del self._cursor[oid]


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

    @property
    def cache(self) -> dict:
        cache = {}
        for oid, entity in self._cursor.items():
            cache[oid] = entity
        return cache

    def execute(self, ocl: OCL) -> Union[Entity, None]:

        commands = {"insert": self._insert,
                    "update": self._update,
                    "delete": self._delete}
        return commands[ocl.cmd](ocl)

    def _insert(self, ocl: OML) -> None:
        """Inserts an entity into the underlying object cache."""
        if ocl.oid not in self._cursor.keys():
            self._cursor[ocl.entity.oid] = ocl.entity
            msg = f"{self.__class__.__name__} inserted entity oid: {ocl.oid}."
            self._logger.debug(msg)
        else:
            msg = f"{self.__class__.__name__} unable to insert entity oid: {ocl.oid}. Entity already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)

    def _update(self, ocl: OML) -> None:
        """Update an existing entity in object cache."""
        if ocl.oid in self._cursor.keys():
            self._cursor[ocl.oid] = ocl.entity
            msg = f"{self.__class__.__name__} updated entity oid: {ocl.oid}."
            self._logger.debug(msg)
        else:
            msg = f"{self.__class__.__name__} unable to update entity oid: {ocl.oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def _delete(self, ocl: OML) -> None:
        """Marks a key/value pair for deletion from object storage"""
        try:
            self._cursor[ocl.oid] = None
            msg = f"Marked object with oid = {ocl.oid} for deletion from object cache."
        except KeyError:
            msg = f"{self.__class__.__name__} unable to delete entity oid: {ocl.oid}. Entity does not exist."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    def reset(self) -> None:
        for oid in self._cursor.keys():
            del self._cursor[oid]
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

    def __init__(self, location: str, autocommit: bool = True, autoclose: bool = False) -> None:
        super().__init__()
        self._location = location
        self._cache_location = os.path.join(os.path.dirname(location), self.__cache_filename)
        self._autocommit = autocommit
        self._autoclose = autoclose
        self._cache_cursor = None
        self._storage_cursor = None
        self._current_cursor = None
        self._in_transaction = False

    @property
    def is_connected(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""
        return self._is_connected

    @property
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""
        return self._in_transaction

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @property
    def autoclose(self) -> bool:
        return self._autoclose

    @property
    def location(self) -> str:
        return self._location

    @property
    def cache_filename(self) -> str:
        return self.__cache_filename

    @property
    def cursor(self) -> shelve:
        """Returns a cursor from the connection."""
        return self._current_cursor

    def begin(self) -> None:
        """Starts a transaction."""
        self._in_transaction = True
        self._current_cursor = self._cache_cursor
        self._logger.debug(f"{self.__class__.__name__} transaction has started.")

    def open(self, autocommit: bool = False) -> None:
        """Opens a database connection."""
        self._cache_cursor = CacheCursor(self._cache_location)
        self._storage_cursor = StorageCursor(self._location)
        if self._autocommit and not self._in_transaction:
            self._current_cursor = self._storage_cursor
        else:
            self._current_cursor = self._cache_cursor
        self._is_connected = True
        self._logger.debug(f"{self.__class__.__name__} connection is open.")

    def execute(self, ocl: OCL) -> Union[Entity, None]:
        return self._current_cursor.execute(ocl)

    def close(self) -> None:
        """Closes the current connection."""
        self._cache_cursor.close()
        self._storage_cursor.close()
        self._current_connection = None
        self._is_connected = False
        self._in_transaction = False
        self._logger.debug(f"{self.__class__.__name__} is closed.")

    def commit(self) -> None:
        """Commits data to the underlying database."""
        self._storage_cursor.save(self._cache_cursor.cache)
        self._cache_cursor.reset()
        self._in_transaction = False

    def rollback(self) -> None:  # pragma: no cover
        """Rolls back changes made to the database since last commit."""
        self._cache_cursor.reset()
        self._in_transaction = False


# ------------------------------------------------------------------------------------------------ #
#                                     OBJECT DATABASE                                              #
# ------------------------------------------------------------------------------------------------ #
class ObjectDB(AbstractDatabase):
    """Manages object persistence."""

    def __init__(self, connection: type(Connection)) -> None:
        super().__init__()
        self._connection = connection
        self._autocommit = connection.autocommit
        self._autoclose = connection.autoclose
        self._in_transaction = False
        self._is_connected = False

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    def connect(self) -> None:
        """Connects to the database."""
        self._connection.open()
        self._is_connected = True

    def begin(self) -> None:
        """Starts a transaction on the underlying database connection."""
        if not self._is_connected:
            self.connect()
        self._connection.begin()
        self._in_transaction = True

    def close(self) -> None:
        """Closes the underlying database connection."""
        self._connection.close()
        self._is_connected = False
        self._in_transaction = False

    def save(self) -> None:
        """Saves changes to the database."""
        self._connection.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        """Rolls back the database to state as of last save or commit."""
        self._connection.rollback()
        self._in_transaction = False

    def query(self, ocl: OCL) -> Union[Entity, None]:
        """Executes a query on the database."""
        self._open_session()
        cursor = self._connection.cursor
        result = cursor.execute(ocl)
        self._close_session()
        return result

    def insert(self, ocl: OCL) -> int:
        """Inserts an object into object storage."""
        self.query(ocl)

    def select(self, ocl: OCL) -> tuple:
        """Performs a select query returning a single instance or row."""
        return self.query(ocl)

    def select_all(self, ocl: OCL) -> list:
        """Performs a select query returning multiple instances or rows."""
        return self.query(ocl)

    def update(self, ocl: OCL) -> None:
        """Performs an update on existing data in the database."""
        self.query(ocl)
        return 1

    def delete(self, ocl: OCL) -> None:
        """Deletes existing data."""
        self.query(ocl)

    def create(self, ocl: OCL) -> None:
        """Executes create OCL statement for databases and tables."""
        self._connection = ObjectDBConnection(location=ocl.location,
                                              autocommit=ocl.autocommit,
                                              autoclose=ocl.autoclose)
        msg = f"Created an object store at {ocl.location}: autocommit={ocl.autocommit}; autoclose={ocl.autoclose}."
        self._logger.info(msg)

    def drop(self, ocl: OCL) -> None:
        """Drop a database or table."""
        storage_pattern = ocl.location + "*"
        cache_pattern = os.path.join(os.path.dirname(ocl.location), self._connection.cache_filename) + "*"
        if self._connection.location == ocl.location:
            self.close()
        self._remove(storage_pattern, shelf='storage')
        self._remove(cache_pattern, shelf='cache')
        msg = f"Dropped object store at {ocl.location}."
        self._logger.info(msg)

    def exists(self, ocl: OCL) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""
        if hasattr(ocl, "location"):
            pattern = ocl.location + "*"
            return len(glob(pattern)) > 0
        else:
            return self.query(ocl)

    def _open_session(self) -> None:
        """Opens a database connection if not already open."""
        if not self._is_connected:
            self.connect()

    def _close_session(self) -> None:
        """Saves and closes the connection, if not in transaction."""
        if not self._in_transaction and self._autocommit:
            self.save()
        if not self._in_transaction and self._autoclose:
            self.close()

    def _remove(self, pattern, shelf: str) -> None:
        """Removes files that match the glob pattern."""
        file_list = glob(pattern, recursive=True)
        for filepath in file_list:
            try:
                os.remove(filepath)
                msg = f"Removed {filepath} from {shelf}."
                self._logger.debug(msg)

            except OSError:
                msg = f"Error while deleting file at {filepath}"
                self._logger.error(msg)
                raise OSError(msg)
