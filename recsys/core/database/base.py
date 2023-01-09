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
# Modified   : Monday January 9th 2023 01:22:41 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Base Database and Connection Classes."""
from abc import abstractmethod
from typing import Union
import mysql.connector
import pymysql

from recsys.core.services.base import Service


# ------------------------------------------------------------------------------------------------ #
#                                        CONNECTION                                                #
# ------------------------------------------------------------------------------------------------ #
class Connection(Service):
    """MySQL Database."""

    def __init__(self, connector: pymysql.connect = None) -> None:
        super().__init__()
        self._connector = connector
        self._is_open = False
        self._in_transaction = False
        self._connection = None

    @property
    def is_open(self) -> bool:
        """Returns the True if the connection is open, False otherwise."""
        return self._is_open

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

    @abstractmethod
    def open(self) -> None:
        """Opens a database connection."""

    def close(self) -> None:
        """Closes the connection."""
        try:
            self._connection.close()
            self._is_open = False
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
    def begin(self, *args, **kwargs) -> None:
        """Starts a transaction on the underlying database connection."""

    @abstractmethod
    def insert(self, *args, **kwargs) -> Union[int, None]:
        """Inserts data into a table and returns the last row id."""

    @abstractmethod
    def select(self, *args, **kwargs) -> list:
        """Performs a select query returning a single instance or row."""

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
