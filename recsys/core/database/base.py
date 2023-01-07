#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 3rd 2023 12:33:25 am                                                #
# Modified   : Saturday January 7th 2023 01:01:36 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Base Database and Connection Classes."""
import os
import shelve
from abc import abstractmethod
from typing import Union

from recsys.core.services.base import Service
from recsys.core.dal.sql.base import OCL


# ------------------------------------------------------------------------------------------------ #
#                                          CURSOR                                                  #
# ------------------------------------------------------------------------------------------------ #
class Cursor(Service):
    """Abstract base class for object database cursors.

    Args:
        location (str): The path of the shelve database file.
    """
    def __init__(self, location) -> None:
        self._location = location
        self._cursor = None
        self._open()

    def open(self) -> None:
        os.makedirs(os.path.dirname(self._location), exist_ok=True)
        self._cursor = shelve.open(self._location)

    def close(self) -> None:
        self._cursor.close()

    @abstractmethod
    def _insert(self, oml: OCL) -> None:
        """Inserts an entity into the underlying object data store."""

    @abstractmethod
    def _update(clf, oml: OCL) -> None:
        """Updates an existing entity in the underlying object data store."""

    @abstractmethod
    def _delete(clf, oml: OCL) -> None:
        """Deletes an existing entity from the underlying object data store."""

    @abstractmethod
    def _exists(clf, oml: OCL) -> None:
        """Checks the existence of a object store or object in the store."""


# ------------------------------------------------------------------------------------------------ #
#                                        CONNECTION                                                #
# ------------------------------------------------------------------------------------------------ #
class Connection(Service):
    """Abstract base class for Database connections."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""

    @property
    @abstractmethod
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""

    @property
    @abstractmethod
    def cursor(self) -> bool:
        """Returns the cursor for the underlying connection."""

    @abstractmethod
    def open(self) -> None:
        """Opens a connection to the database."""

    @abstractmethod
    def begin(self) -> None:
        """Start a transaction on the connection."""

    @abstractmethod
    def close(self) -> None:
        """Closes the connection."""

    @abstractmethod
    def commit(self) -> None:
        """Commits the connection"""

    @abstractmethod
    def rollback(self) -> None:
        """Rolls back the database to the last commit."""

class MySQLConnection(Connection):
    """MySQL Database."""

    def __init__(self, connector: pymysql.connect) -> None:
        super().__init__()
        self._connector = connector
        self._is_connected = False
        self._in_transaction = False
        self._connection = None

    @property
    def is_connected(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""
        return self._is_connected

    @property
    def in_transaction(self) -> bool:
        """Returns the True if a transaction has been started."""
        return self._in_transaction

    @property
    def cursor(self) -> pymysql.connections.Connection.cursor:
        """Returns a cursor from the connection."""
        try:
            return self._connection.cursor()
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def begin(self) -> None:
        """Start a transaction on the connection."""
        try:
            self._connection.begin()
            self._in_transaction = True
            self._logger.debug(f"{self.__class__.__name__}  transaction started.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def open(self) -> None:
        """Opens a database connection."""

        dotenv.load_dotenv()
        host = os.getenv("MYSQL_HOST")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        self._database = os.getenv("MYSQL_DATABASE")

        try:
            self._connection = self._connector(host=host, user=user, password=password, database=self._database, autocommit=False)
            self._is_connected = True
            self._logger.debug(f"{self.__class__.__name__} is connected.")
        except mysql.connector.Error as err:  # pragma: no cover
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                msg = "Invalid user name or password"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                msg = "Database does not exist"
                self._logger.error(msg)
                raise mysql.connector.Error(msg)
            else:
                self._logger.error(err)
                raise mysql.connector.Error()

    def close(self) -> None:
        """Closes the connection."""
        try:
            self._connection.close()
            self._is_connected = False
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__}  is closed.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def commit(self) -> None:
        """Commits the connection"""
        try:
            self._connection.commit()
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__}  is committed.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def rollback(self) -> None:
        """Rolls back the database to the last commit."""
        try:
            self._connection.rollback()
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__}  is rolled back.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class AbstractDatabase(Service):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def query(self, *args, **kwargs):
        """Executes a query on the database and returns a cursor object."""

    @abstractmethod
    def begin(self, *args, **kwargs) -> None:
        """Starts a transaction on the underlying database connection."""

    @abstractmethod
    def create(self, *args, **kwargs) -> None:
        """Create a database or table."""

    @abstractmethod
    def insert(self, *args, **kwargs) -> Union[int, None]:
        """Inserts data into a table and returns the last row id."""

    @abstractmethod
    def select(self, *args, **kwargs) -> list:
        """Performs a select query returning a single instance or row."""

    @abstractmethod
    def select_all(self, *args, **kwargs) -> list:
        """Performs a select query returning multiple instances or rows."""

    @abstractmethod
    def update(self, *args, **kwargs) -> None:
        """Performs an update on existing data in the database."""

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """Deletes existing data or database."""

    @abstractmethod
    def drop(self, *args, **kwargs) -> None:
        """Drop a database or table."""

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Returns True if the data specified by the parameters exists. Returns False otherwise."""

    @abstractmethod
    def save(self) -> None:
        """Saves changes to the database."""
