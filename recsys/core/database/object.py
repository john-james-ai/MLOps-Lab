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
# Modified   : Tuesday January 3rd 2023 10:25:25 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Object persistence module"""
import os
import shelve
from glob import glob
from mysql.connector import errors

from .base import AbstractConnection, AbstractDatabase
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                             OBJECT DATABASE (PSEUDO) CONNECTION                                  #
# ------------------------------------------------------------------------------------------------ #
class ObjectDBConnection(AbstractConnection):
    """Creates an object datastore at the designated directory location.

    Args:
        location (str): The path and filename for the data store. The base of the path is
            the database name by convention.
    """

    def __init__(self, location: str) -> None:
        super().__init__()
        self._location = location
        self._connection = None
        self._is_connected = False
        self._in_transaction = False

        os.makedirs(os.path.dirname(self._location), exist_ok=True)
        self._connection = shelve.open(self._location, writeback=True)
        self._is_connected = True
        self._logger.debug(f"{self.__class__.__name__} is open.")

    @property
    def database(self) -> str:
        """Returns the path of the underlying database."""
        return self._location

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @property
    def in_transaction(self) -> bool:
        return self._in_transaction

    @property
    def cursor(self) -> shelve:
        return self._connection

    @cursor.setter
    def cursor(self, cursor: shelve) -> None:
        self._connection = cursor

    def begin(self) -> None:
        """Starts a transaction."""
        self._in_transaction = True

    def close(self) -> None:
        """Closes the current connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._is_connected = False
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__} is closed.")
        else:
            msg = f"{self.__class__.__name__} is already closed."
            self._logger.warning(msg)
            raise errors.ProgrammingError(msg)

    def commit(self) -> None:
        """Commits data to the underlying database."""
        self._in_transaction = False
        if self._connection is not None:
            self._connection.sync()
            self._logger.debug(f"{self.__class__.__name__} is committed.")
        else:
            self._logger.error(f"{self.__class__.__name__} is closed. Nothing to commit.")

    def rollback(self) -> None:  # pragma: no cover
        """Rollsback changes made to the database since last commit."""
        raise NotImplementedError()

    def delete(self) -> None:
        go = input("This will permanently delete object storage. Continue? (y/n)")
        if 'y' in go.lower():
            locations = self._location + ".*"
            for f in glob(locations):
                os.remove(f)


# ------------------------------------------------------------------------------------------------ #
#                                     OBJECT DATABASE                                              #
# ------------------------------------------------------------------------------------------------ #
class ObjectDB(AbstractDatabase):
    """Manages object persistence."""
    def __init__(self, connection: AbstractConnection) -> None:
        super().__init__()
        self._connection = connection

    @property
    def connection(self) -> AbstractConnection:
        return self._connection

    # -------------------------------------------------------------------------------------------- #
    def query(self, oid: str) -> Entity:
        """Executes a query on the database."""
        return self._connection.cursor[oid]

    # -------------------------------------------------------------------------------------------- #
    def begin(self) -> None:
        self._connection.begin()

    # -------------------------------------------------------------------------------------------- #
    def create(self, location: str) -> None:
        """Creates an object storage database in the designated location."""
        return ObjectDBConnection(location)

    # -------------------------------------------------------------------------------------------- #
    def insert(self, entity: Entity) -> None:
        """Inserts an entity into object storage if it doesn't already exist."""
        cursor = self._connection.cursor
        if entity.oid in cursor.keys():
            msg = f"Unable to insert entity oid: {entity.oid}. The entity already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)
        else:
            cursor[entity.oid] = entity
            self._connection.cursor = cursor
            self._logger.debug(f"{self.__class__.__name__} inserted {entity.__class__.__name__}.{entity.name}.")
            if not self._connection.in_transaction:
                self._connection.commit()

    # -------------------------------------------------------------------------------------------- #
    def select(self, oid: str) -> Entity:
        """Performs a select query returning a single instance or row."""
        try:
            return self._connection.cursor[oid]
        except KeyError:
            msg = f"No entity with oid = {oid} exists in the database."
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        except AttributeError:  # pragma: no cover
            msg = f"No entity with oid = {oid} exists in the database."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def selectall(self, keys: list = None) -> dict:
        """Performs a select query returning one or multiple instances."""
        result = {}
        cursor = self._connection.cursor
        if keys is not None:
            for key in keys:
                try:
                    result[key] = cursor[key]
                except KeyError:
                    msg = f"No object with oid = {key} was found in object storage."
                    self._logger.error(msg)
        else:
            for oid, entity in cursor.items():
                result[oid] = entity
        return result

    # -------------------------------------------------------------------------------------------- #`
    def update(self, entity: Entity) -> None:
        """Performs an update on existing data in the database."""
        cursor = self._connection.cursor
        try:
            if entity.oid in cursor.keys():
                cursor[entity.oid] = entity
                self._connection.cursor = cursor
                self._logger.debug(f"{self.__class__.__name__} updated {entity.__class__.__name__}.{entity.name}.")
                if not self._connection.in_transaction:
                    self._connection.commit()
            else:
                msg = f"No object oid = {entity.oid} was found in object storage. Try inserting instead."
                self._logger.error(msg)
                raise FileNotFoundError(msg)
        except AttributeError:  # pragma: no cover
            msg = f"No object oid = {entity.oid} was found in object storage. Try inserting instead."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def delete(self, oid: str) -> None:
        """Deletes existing data from the database."""
        cursor = self._connection.cursor
        try:
            del cursor[oid]
            self._connection.cursor = cursor
            self._logger.debug(f"{self.__class__.__name__} deleted entity identified by oid = {oid}.")
            if not self._connection.in_transaction:
                self._connection.commit()
        except KeyError:
            msg = f"No object oid = {oid} was found in object storage."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def drop(self) -> None:
        self._connection.delete()

    # -------------------------------------------------------------------------------------------- #
    def exists(self, oid: str) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""
        try:
            return oid in self._connection.cursor.keys()
        except AttributeError:
            return False

    # -------------------------------------------------------------------------------------------- #
    def save(self) -> None:
        """Saves changes to the database."""
        self._connection.commit()
