#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/database/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 3rd 2023 12:33:25 am                                                #
# Modified   : Tuesday January 24th 2023 08:13:46 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Base Database and Connection Classes."""
from abc import ABC, abstractmethod
from typing import Union
import mysql.connector
import pymysql
import logging


# ------------------------------------------------------------------------------------------------ #
#                                        CONNECTION                                                #
# ------------------------------------------------------------------------------------------------ #
class Connection(ABC):
    """MySQL Database."""

    def __init__(
        self, connector: pymysql.connect = None, autocommit: bool = False, autoclose: bool = False
    ) -> None:
        self._autocommit = autocommit
        self._autoclose = autoclose
        self._connector = connector
        self._is_open = False
        self._in_transaction = False
        self._connection = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    def database(self) -> str:
        return self._database

    @property
    def autocommit(self) -> str:
        return self._autocommit

    @property
    def autoclose(self) -> str:
        return self._autoclose

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
            self._logger.debug(
                f"{self.__class__.__name__}  transaction started on {self._database}."
            )
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
            self._logger.debug(f"{self.__class__.__name__}  {self._database} is closed.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def commit(self) -> None:
        """Commits the connection"""
        try:
            self._connection.commit()
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__} {self._database} is committed.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()

    def rollback(self) -> None:
        """Rolls back the database to the last commit."""
        try:
            self._connection.rollback()
            self._in_transaction = False
            self._logger.debug(f"{self.__class__.__name__} {self._database} is rolled back.")
        except mysql.connector.Error as err:  # pragma: no cover
            self._logger.error(err)
            raise mysql.connector.Error()


# ------------------------------------------------------------------------------------------------ #
#                                        DATABASE                                                  #
# ------------------------------------------------------------------------------------------------ #
class AbstractDatabase(ABC):
    def __init__(self):
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

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
